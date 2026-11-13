"""Bounded read-only MT5 market observation collector and immutable local store.

This module has no execution gateway, risk-approval or order-construction dependency.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from types import MappingProxyType
from pathlib import Path
import re
import sqlite3


TIMEFRAME_SECONDS={"M1":60,"M2":120,"M3":180,"M5":300,"M15":900,"M30":1800,"H1":3600,"H4":14400}
DEFAULT_MAX_BAR_AGE_SECONDS={"M1":120,"M2":240,"M3":360,"M5":600,"M15":1800,"M30":3600,"H1":7200,"H4":21600}
MAX_BAR_AGE_PERIODS=4.0

from .sqlite_support import sqlite_session


class ObservationError(RuntimeError):
    pass


def _utc(value, label):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json(value):
    def convert(item):
        if isinstance(item, datetime): return item.astimezone(timezone.utc).isoformat()
        raise TypeError(type(item).__name__)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=convert)


@dataclass(frozen=True)
class CaptureSubscription:
    pairs: tuple[str, ...]
    timeframes: tuple[str, ...] = ("M1", "M5", "M15", "M30", "H1", "H4")
    bars_per_capture: int = 100
    max_quote_age_seconds: float = 30.0
    max_bar_age_seconds: Mapping[str,float] | None = None
    max_bar_age_periods: float = MAX_BAR_AGE_PERIODS

    def __post_init__(self):
        if not self.pairs or len(set(self.pairs)) != len(self.pairs) or any(not isinstance(pair,str) or not re.fullmatch(r"[A-Z]{6}",pair) or pair[:3]==pair[3:] for pair in self.pairs): raise ValueError("Subscriptions must contain unique standard Forex pairs")
        if not self.timeframes or not set(self.timeframes) <= {"M1", "M2", "M3", "M5", "M15", "M30", "H1", "H4"}: raise ValueError("Unsupported subscription timeframe")
        if not 1 <= self.bars_per_capture <= 5000 or not 1 <= self.max_quote_age_seconds <= 300: raise ValueError("Capture bounds are invalid")
        if not isinstance(self.max_bar_age_periods,(int,float)) or isinstance(self.max_bar_age_periods,bool) or not math.isfinite(self.max_bar_age_periods) or not 1<=self.max_bar_age_periods<=MAX_BAR_AGE_PERIODS: raise ValueError("Maximum bar-age periods must be between 1 and the hard four-period ceiling")
        limits=dict(DEFAULT_MAX_BAR_AGE_SECONDS)
        if self.max_bar_age_seconds is not None:
            supplied=dict(self.max_bar_age_seconds)
            if set(supplied)-set(DEFAULT_MAX_BAR_AGE_SECONDS) or not {"M1","M5","M15","M30","H1","H4"} <= set(supplied) or any(not isinstance(v,(int,float)) or isinstance(v,bool) or not math.isfinite(v) or v<=0 for v in supplied.values()): raise ValueError("Positive finite bar freshness limits are required for M1, M5, M15, M30, H1 and H4")
            limits.update(supplied)
        if any(limits[tf]>TIMEFRAME_SECONDS[tf]*self.max_bar_age_periods for tf in limits):raise ValueError("Bar freshness limits exceed the configured per-timeframe upper bound")
        object.__setattr__(self,"max_bar_age_seconds",MappingProxyType(limits))


def _session_seconds(value):
    text=str(value).strip()
    if text.isdigit():
        seconds=int(text)
        return seconds if 0<=seconds<86400 else None
    parts=text.split(":")
    if len(parts) not in (2,3) or any(not part.isdigit() for part in parts):return None
    hour,minute=int(parts[0]),int(parts[1]);second=int(parts[2]) if len(parts)==3 else 0
    if hour>23 or minute>59 or second>59:return None
    return hour*3600+minute*60+second


def _market_session_state(sessions, when: datetime, server_timezone, schedule_complete=False):
    """Return open/closed only for a complete, parseable broker weekly schedule."""
    if not schedule_complete or server_timezone is None:return "unknown"
    try:
        local=when.astimezone(server_timezone)
        day=(local.weekday()+1)%7  # MT5: Sunday=0 through Saturday=6.
        seconds=local.hour*3600+local.minute*60+local.second
        parsed=[]
        for row in sessions:
            if len(row)<4:return "unknown"
            session_day=int(row[0]);start=_session_seconds(row[2]);end=_session_seconds(row[3])
            if session_day not in range(7) or start is None or end is None or start==end:return "unknown"
            parsed.append((session_day,start,end))
        if not parsed:return "closed"
        previous=(day-1)%7
        for session_day,start,end in parsed:
            if start<end and session_day==day and start<=seconds<end:return "open"
            if start>end and ((session_day==day and seconds>=start) or (session_day==previous and seconds<end)):return "open"
        return "closed"
    except (TypeError,ValueError,OverflowError):
        return "unknown"


def _assess_internal_gaps(bars, timeframe, sessions, server_timezone, schedule_complete=False):
    """Classify omitted bar starts; closures count as expected only with schedule evidence."""
    seconds=TIMEFRAME_SECONDS[timeframe];gaps=[];unknown=False;missing=False
    for left,right in zip(bars,bars[1:]):
        delta=(right["time_utc"]-left["time_utc"]).total_seconds()
        if delta<=seconds:continue
        steps=delta/seconds;rounded=round(steps)
        if rounded<2 or not math.isclose(steps,rounded,rel_tol=0,abs_tol=1e-9):
            gaps.append({"from_utc":left["time_utc"].isoformat(),"to_utc":right["time_utc"].isoformat(),"gap_seconds":delta,"missing_bar_starts":None,"classification":"unknown_alignment"});unknown=True;continue
        missing_starts=[left["time_utc"]+timedelta(seconds=seconds*step) for step in range(1,rounded)]
        states=[_market_session_state(sessions,stamp,server_timezone,schedule_complete) for stamp in missing_starts]
        if all(state=="closed" for state in states):classification="expected_market_closure"
        elif "open" in states:classification="unexplained_missing_candles";missing=True
        else:classification="unknown_session_state";unknown=True
        gaps.append({"from_utc":left["time_utc"].isoformat(),"to_utc":right["time_utc"].isoformat(),"gap_seconds":delta,"missing_bar_starts":len(missing_starts),"classification":classification})
    completeness="incomplete" if missing else ("unknown" if unknown else "complete")
    return gaps,completeness


class ObservationStore:
    """Append-only observation and event store; duplicate ids are never overwritten."""
    def __init__(self, path: str | Path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True);exists=self.path.exists()
        try:
            with sqlite_session(self.path) as db:
                if exists and db.execute("PRAGMA quick_check").fetchone()[0] != "ok": raise ObservationError("Observation database integrity check failed")
                db.execute("PRAGMA journal_mode=WAL")
                db.executescript("""
                CREATE TABLE IF NOT EXISTS observations(observation_id TEXT PRIMARY KEY,broker TEXT NOT NULL,server TEXT NOT NULL,pair TEXT NOT NULL,symbol TEXT NOT NULL,time_utc TEXT NOT NULL,collected_utc TEXT NOT NULL,payload_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS observation_stream_time ON observations(broker,server,symbol,time_utc);
                CREATE TABLE IF NOT EXISTS capture_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,time_utc TEXT NOT NULL,event TEXT NOT NULL,details_json TEXT NOT NULL,prev_hash TEXT NOT NULL,event_hash TEXT NOT NULL);
                CREATE TRIGGER IF NOT EXISTS observations_immutable_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS observations_immutable_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT,'observations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS capture_events_immutable_update BEFORE UPDATE ON capture_events BEGIN SELECT RAISE(ABORT,'capture events are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS capture_events_immutable_delete BEFORE DELETE ON capture_events BEGIN SELECT RAISE(ABORT,'capture events are immutable'); END;
                """)
        except (sqlite3.DatabaseError,OSError) as exc:
            if isinstance(exc,ObservationError): raise
            raise ObservationError("Observation store unavailable; capture refused") from exc

    def event(self, kind: str, details: dict, *, at: datetime):
        at=_utc(at,"event time"); body=_json(details)
        with sqlite_session(self.path,timeout=2) as db:
            db.execute("BEGIN IMMEDIATE")
            self._append_event(db,kind,body,at)

    @staticmethod
    def _append_event(db,kind,body,at):
        last=db.execute("SELECT event_hash FROM capture_events ORDER BY seq DESC LIMIT 1").fetchone();prev=last[0] if last else "0"*64
        digest=hashlib.sha256((prev+kind+at.isoformat()+body).encode()).hexdigest()
        db.execute("INSERT INTO capture_events(time_utc,event,details_json,prev_hash,event_hash) VALUES(?,?,?,?,?)",(at.isoformat(),kind,body,prev,digest))

    def append(self, observation: dict) -> str:
        payload=_json(observation); digest=hashlib.sha256(payload.encode()).hexdigest()
        identity={k:observation[k] for k in ("broker","server","symbol","time_utc","bid","ask")}
        identity["bars_sha256"]=hashlib.sha256(_json(observation["bars"]).encode()).hexdigest()
        identifier=hashlib.sha256(_json(identity).encode()).hexdigest()
        stamp=_utc(observation["time_utc"],"observation time").isoformat()
        collected=_utc(observation["collected_utc"],"collection time").isoformat()
        with sqlite_session(self.path,timeout=2) as db:
            db.execute("BEGIN IMMEDIATE")
            prior=db.execute("SELECT time_utc FROM observations WHERE broker=? AND server=? AND symbol=? ORDER BY time_utc DESC LIMIT 1",(observation["broker"],observation["server"],observation["symbol"])).fetchone()
            if prior and stamp < prior[0]:
                return "out_of_order"
            try:
                db.execute("INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?)",(identifier,observation["broker"],observation["server"],observation["pair"],observation["symbol"],stamp,collected,payload,digest))
                self._append_event(db,"observation_recorded",_json({"observation_id":identifier,"payload_sha256":digest}),_utc(observation["collected_utc"],"collection time"))
            except sqlite3.IntegrityError:
                return "duplicate"
        return identifier

    def verify(self) -> bool:
        try:
            with sqlite_session(self.path) as db:
                for identifier,broker,server,pair,symbol,stamp,collected,encoded,digest in db.execute("SELECT observation_id,broker,server,pair,symbol,time_utc,collected_utc,payload_json,payload_sha256 FROM observations"):
                    if hashlib.sha256(encoded.encode()).hexdigest()!=digest:return False
                    payload=json.loads(encoded)
                    identity={k:payload[k] for k in ("broker","server","symbol","time_utc","bid","ask")}
                    identity["bars_sha256"]=hashlib.sha256(_json(payload["bars"]).encode()).hexdigest()
                    expected_id=hashlib.sha256(_json(identity).encode()).hexdigest()
                    payload_time=datetime.fromisoformat(payload["time_utc"]);payload_collected=datetime.fromisoformat(payload["collected_utc"])
                    if identifier!=expected_id or (broker,server,pair,symbol,stamp,collected)!=(payload["broker"],payload["server"],payload["pair"],payload["symbol"],_utc(payload_time,"observation time").isoformat(),_utc(payload_collected,"collection time").isoformat()):return False
                record_events=[json.loads(details) for event,details in db.execute("SELECT event,details_json FROM capture_events") if event=="observation_recorded"]
                links={item.get("observation_id"):item.get("payload_sha256") for item in record_events}
                if len(record_events)!=db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] or len(links)!=len(record_events):return False
                for identifier,digest in db.execute("SELECT observation_id,payload_sha256 FROM observations"):
                    if links.get(identifier)!=digest:return False
                previous="0"*64
                for at,event,details,prev,digest in db.execute("SELECT time_utc,event,details_json,prev_hash,event_hash FROM capture_events ORDER BY seq"):
                    expected=hashlib.sha256((previous+event+at+details).encode()).hexdigest()
                    if prev!=previous or digest!=expected:return False
                    previous=digest
            return True
        except sqlite3.DatabaseError as exc: raise ObservationError("Observation store could not be verified") from exc
        except (ValueError,TypeError,KeyError):return False

    def health(self, *, now: datetime|None=None, freshness_seconds: float=120) -> dict:
        now=_utc(now or datetime.now(timezone.utc),"health time")
        if freshness_seconds<=0:raise ValueError("Health freshness bound must be positive")
        valid=self.verify()
        with sqlite_session(self.path) as db:
            count=db.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            last=db.execute("SELECT collected_utc FROM observations ORDER BY collected_utc DESC LIMIT 1").fetchone()
            run=db.execute("SELECT time_utc,details_json FROM capture_events WHERE event='capture_complete' ORDER BY seq DESC LIMIT 1").fetchone()
        run_details=json.loads(run[1]) if run else None
        run_status=run_details.get("status","unknown") if run_details else "unknown"
        if not valid:status="corrupt"
        elif last is None:status="not_started"
        else:
            age=(now-datetime.fromisoformat(last[0]).astimezone(timezone.utc)).total_seconds()
            status="current" if 0<=age<=freshness_seconds else "stale_or_future"
            if status=="current" and run_status!="healthy":status="partial"
        return {"status":status,"integrity_verified":valid,"observation_count":count,
                "last_observation_utc":last[0] if last else None,
                "last_capture_run":run_details,"last_capture_status":run_status}

    def latest_shadow_input(self, pair: str, broker: str, server: str, *, now: datetime|None=None, max_age_seconds: float=30) -> dict:
        """Return typed completed bars and a content id only from a fresh stored broker observation."""
        now=_utc(now or datetime.now(timezone.utc),"shadow input time")
        if not isinstance(max_age_seconds,(int,float)) or isinstance(max_age_seconds,bool) or not math.isfinite(max_age_seconds) or not 0<max_age_seconds<=300:raise ValueError("Shadow quote age bound must be positive and at most 300 seconds")
        with sqlite_session(self.path) as db:
            row=db.execute("SELECT observation_id,payload_json,payload_sha256 FROM observations WHERE pair=? AND broker=? AND server=? ORDER BY collected_utc DESC LIMIT 1",(pair,broker,server)).fetchone()
        if not row:raise ObservationError("No broker observation is available")
        observation_id,encoded,digest=row
        if hashlib.sha256(encoded.encode()).hexdigest()!=digest:raise ObservationError("Broker observation hash mismatch")
        value=json.loads(encoded)
        if not isinstance(value,dict) or value.get("pair")!=pair or value.get("broker")!=broker or value.get("server")!=server or value.get("symbol") is None:
            raise ObservationError("Stored observation identity is inconsistent")
        collected=datetime.fromisoformat(value["collected_utc"]);quote_time=datetime.fromisoformat(value["quote_time_utc"])
        if collected.tzinfo is None or quote_time.tzinfo is None:raise ObservationError("Stored observation has a naive timestamp")
        required=value.get("required_timeframes");bars=value.get("bars");freshness=value.get("timeframe_freshness");expected_count=value.get("bars_per_capture")
        supported={"M1","M2","M3","M5","M15","M30","H1","H4"}
        if not isinstance(required,list) or not required or len(required)!=len(set(required)) or not set(required)<=supported:
            raise ObservationError("Stored observation has missing or invalid required timeframe declaration")
        required_keys=set(required)
        if not isinstance(bars,dict) or not isinstance(freshness,dict) or set(bars)!=required_keys or set(freshness)!=required_keys:
            raise ObservationError("Stored bar and freshness timeframe keys are missing or mismatched")
        if not isinstance(expected_count,int) or isinstance(expected_count,bool) or not 1<=expected_count<=5000:
            raise ObservationError("Stored observation has invalid expected bar count")
        age=(now-quote_time.astimezone(timezone.utc)).total_seconds()
        if age<0 or age>max_age_seconds:raise ObservationError("Stored broker quote is stale or future-dated")
        bid=value.get("bid");ask=value.get("ask");spread=value.get("spread");spread_points=value.get("spread_points")
        if any(not isinstance(x,(int,float)) or isinstance(x,bool) or not math.isfinite(float(x)) for x in (bid,ask,spread,spread_points)) or bid<=0 or ask<=bid or spread<=0 or spread_points<=0 or not math.isclose(float(spread),float(ask)-float(bid),rel_tol=0,abs_tol=1e-12):
            raise ObservationError("Stored broker quote structure is invalid")
        from .models import Candle
        candles={}
        for timeframe in required:
            rows=bars[timeframe];assessment=freshness[timeframe]
            if not isinstance(rows,list) or len(rows)!=expected_count or not isinstance(assessment,dict):raise ObservationError(f"Stored {timeframe} series is incomplete")
            if assessment.get("status")!="fresh" or assessment.get("freshness")!="fresh" or assessment.get("usable") is not True or assessment.get("completeness")!="complete":raise ObservationError(f"Stored {timeframe} freshness assessment is not usable")
            if assessment.get("expected_count")!=expected_count or assessment.get("actual_count")!=expected_count:raise ObservationError(f"Stored {timeframe} count evidence is inconsistent")
            gaps=assessment.get("gaps")
            if not isinstance(gaps,list) or any(not isinstance(gap,dict) or gap.get("classification")!="expected_market_closure" for gap in gaps):raise ObservationError(f"Stored {timeframe} gap assessment is incomplete or unknown")
            if gaps and assessment.get("session_schedule_complete") is not True:raise ObservationError(f"Stored {timeframe} closure evidence lacks a complete broker schedule")
            diagnostics=assessment.get("adapter_diagnostics")
            if diagnostics is not None and (not isinstance(diagnostics,dict) or diagnostics.get("discarded_count",0)!=0):raise ObservationError(f"Stored {timeframe} adapter diagnostics report discarded bars")
            limit=assessment.get("max_age_seconds")
            if not isinstance(limit,(int,float)) or isinstance(limit,bool) or not math.isfinite(limit) or not 0<limit<=TIMEFRAME_SECONDS[timeframe]*MAX_BAR_AGE_PERIODS:raise ObservationError(f"Stored {timeframe} age limit is invalid")
            converted=[];previous=None
            for bar in rows:
                try:
                    stamp=_utc(datetime.fromisoformat(bar["time_utc"]),"stored bar time")
                    prices=[bar[k] for k in ("open","high","low","close")]
                    if any(not isinstance(x,(int,float)) or isinstance(x,bool) or not math.isfinite(float(x)) or x<=0 for x in prices):raise ValueError("invalid OHLC")
                    if float(bar["high"])<max(float(bar["open"]),float(bar["close"])) or float(bar["low"])>min(float(bar["open"]),float(bar["close"])) or float(bar["high"])<float(bar["low"]):raise ValueError("invalid OHLC geometry")
                    if not isinstance(bar["tick_volume"],(int,float)) or isinstance(bar["tick_volume"],bool) or not math.isfinite(float(bar["tick_volume"])) or bar["tick_volume"]<0:raise ValueError("invalid volume")
                    if not isinstance(bar["spread_points"],(int,float)) or isinstance(bar["spread_points"],bool) or not math.isfinite(float(bar["spread_points"])) or bar["spread_points"]<0:raise ValueError("invalid spread")
                except (KeyError,TypeError,ValueError) as exc:raise ObservationError(f"Stored {timeframe} bar structure is invalid") from exc
                if previous is not None and stamp<=previous:raise ObservationError(f"Stored {timeframe} bars are not strictly chronological")
                if stamp>=now or stamp>=quote_time.astimezone(timezone.utc):raise ObservationError(f"Stored {timeframe} bar is future-dated relative to validation or quote time")
                previous=stamp
                converted.append(Candle(stamp,float(bar["open"]),float(bar["high"]),float(bar["low"]),float(bar["close"]),float(bar["tick_volume"]),True,float(bar["spread_points"])))
            newest=converted[-1].time;close_time=newest+timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
            if assessment.get("newest_bar_open_utc")!=newest.isoformat() or assessment.get("newest_bar_close_utc")!=close_time.isoformat():raise ObservationError(f"Stored {timeframe} latest-bar evidence is inconsistent")
            age_at_quote=(quote_time.astimezone(timezone.utc)-close_time).total_seconds();age_now=(now-close_time).total_seconds()
            if age_at_quote<0 or age_now<0 or age_at_quote>limit or age_now>limit:raise ObservationError(f"Stored {timeframe} bars are stale or incomplete at shadow-input time")
            candles[timeframe]=converted
        return {"observation_id":observation_id,"pair":pair,"candles_by_timeframe":candles,
                "spread_points":float(value["spread_points"]),"quote_time_utc":quote_time.astimezone(timezone.utc),
                "collected_utc":collected.astimezone(timezone.utc),"snapshot":{"observation_sha256":digest,
                "broker":value["broker"],"server":value["server"],"symbol":value["symbol"],"account_currency":value["account_currency"],
                "bid":value["bid"],"ask":value["ask"],"spread_source":value["spread_source"],"ohlc_source":value["ohlc_source"]}}


class ReadOnlyMT5Collector:
    """One bounded read-only capture cycle. Reconnection attempts are explicit and limited."""
    def __init__(self, adapter, store: ObservationStore, *, expected_broker: str, expected_server: str,
                 subscription: CaptureSubscription, expected_account_type: str="demo", reconnect_attempts: int = 1,
                 clock: Callable[[], datetime] | None = None):
        if not expected_broker or not expected_server: raise ValueError("Expected broker and server are required")
        if expected_account_type!="demo": raise ValueError("Read-only capture is restricted to the configured demo account")
        if not 0 <= reconnect_attempts <= 2: raise ValueError("Reconnect attempts must be bounded to 0..2")
        if clock is not None and not callable(clock): raise ValueError("UTC clock must be callable")
        self.adapter=adapter;self.store=store;self.expected_broker=expected_broker;self.expected_server=expected_server;self.expected_account_type=expected_account_type
        self.subscription=subscription;self.reconnect_attempts=reconnect_attempts
        self.clock=clock or (lambda: datetime.now(timezone.utc));self._clock_injected=clock is not None

    def _connected(self):
        try:return bool(self.adapter.is_connected())
        except Exception:return False

    def collect_once(self, *, now: datetime | None = None) -> dict:
        explicit_now=now is not None
        now=_utc(now if explicit_now else self.clock(),"capture time")
        def validation_time():
            return _utc(self.clock(),"validation time") if self._clock_injected or not explicit_now else now
        try:
            if not self.store.verify():return {"status":"store_corrupt","observations":0,"rejected":1,"reasons":{"store_corrupt":1}}
        except ObservationError:
            return {"status":"store_unavailable","observations":0,"rejected":1,"reasons":{"store_unavailable":1}}
        attempts=0
        while not self._connected():
            if attempts>self.reconnect_attempts:
                self.store.event("disconnected",{"attempts":attempts},at=now)
                return {"status":"disconnected","observations":0,"rejected":1,"reasons":{"disconnected":1}}
            try:
                self.adapter.connect();attempts+=1
            except Exception as exc:
                attempts+=1
                if attempts>self.reconnect_attempts:
                    self.store.event("connection_failed",{"attempts":attempts,"error_type":type(exc).__name__},at=now)
                    return {"status":"connection_failed","observations":0,"rejected":1,"reasons":{"connection_failed":1}}
        try:
            account=self.adapter.account_status()
            if account.broker!=self.expected_broker or account.server!=self.expected_server or account.account_type!=self.expected_account_type or not account.terminal_connected:
                self.store.event("identity_rejected",{"broker_match":account.broker==self.expected_broker,"server_match":account.server==self.expected_server,"account_type_match":account.account_type==self.expected_account_type,"connected":account.terminal_connected},at=now)
                return {"status":"identity_rejected","observations":0,"rejected":1,"reasons":{"identity_rejected":1}}
            specs=self.adapter.symbols(self.subscription.pairs)
        except Exception as exc:
            self.store.event("broker_read_failed",{"error_type":type(exc).__name__},at=now)
            return {"status":"broker_read_failed","observations":0,"rejected":1,"reasons":{"broker_read_failed":1}}
        accepted=0;reasons={};gaps=[];timeframe_freshness={};quality_issues={}
        def reject(reason):reasons[reason]=reasons.get(reason,0)+1
        def note_quality(reason):quality_issues[reason]=quality_issues.get(reason,0)+1
        def mark_unassessed(pair,spec,status,reason,at):
            session_state=_market_session_state(getattr(spec,"sessions",()) if spec else (),at,getattr(self.adapter,"server_timezone",None),getattr(spec,"session_schedule_complete",False) if spec else False)
            result_status="market_closed" if status in ("no_quote","stale_quote") and session_state=="closed" else ("freshness_unknown" if status in ("no_quote","stale_quote") else status)
            timeframe_freshness[pair]={tf:{"status":result_status,"usable":False,"bars_observed":False,"reason":reason,"session_state":session_state,"max_age_seconds":self.subscription.max_bar_age_seconds.get(tf)} for tf in self.subscription.timeframes}
            note_quality(result_status)
            self.store.event("timeframe_freshness",{"pair":pair,"symbol":spec.broker_symbol if spec else None,"timeframes":timeframe_freshness[pair]},at=now)
        for pair in self.subscription.pairs:
            spec=specs.get(pair)
            if spec is None or not spec.supported:
                reject("unavailable_symbol");self.store.event("unavailable_symbol",{"pair":pair},at=now);mark_unassessed(pair,spec,"unavailable","symbol_unavailable",now);continue
            try: quote=self.adapter.price(spec.broker_symbol)
            except Exception as exc:
                reject("quote_read_failed");self.store.event("quote_read_failed",{"pair":pair,"error_type":type(exc).__name__},at=now);mark_unassessed(pair,spec,"unknown","quote_read_failed",validation_time());continue
            if quote is None:
                reject("stale_or_invalid_quote");self.store.event("stale_or_invalid_quote",{"pair":pair},at=now);mark_unassessed(pair,spec,"no_quote","quote_unavailable",validation_time());continue
            try:quote_time=_utc(quote.time_utc,"quote time")
            except ValueError:
                reject("invalid_quote_timestamp");self.store.event("invalid_quote_timestamp",{"pair":pair},at=now);mark_unassessed(pair,spec,"unknown","invalid_quote_timestamp",validation_time());continue
            quote_validation_time=validation_time()
            age=(quote_validation_time-quote_time).total_seconds()
            if age<0:
                reject("future_quote");self.store.event("future_quote",{"pair":pair},at=now);mark_unassessed(pair,spec,"unknown","future_quote",quote_validation_time);continue
            if not quote.fresh or age>self.subscription.max_quote_age_seconds:
                reject("stale_or_invalid_quote");self.store.event("stale_or_invalid_quote",{"pair":pair},at=now);mark_unassessed(pair,spec,"stale_quote","stale_quote",quote_validation_time);continue
            if not math.isfinite(quote.bid) or not math.isfinite(quote.ask) or quote.bid<=0 or quote.ask<=quote.bid:
                reject("stale_or_invalid_quote");self.store.event("stale_or_invalid_quote",{"pair":pair},at=now);mark_unassessed(pair,spec,"invalid","invalid_quote_prices",quote_validation_time);continue
            if spec.point<=0 or not math.isfinite(spec.point):
                reject("invalid_symbol_specification");self.store.event("invalid_symbol_specification",{"pair":pair},at=now);mark_unassessed(pair,spec,"invalid","invalid_symbol_specification",quote_validation_time);continue
            bars_by_tf={};pair_freshness={};bar_error=False
            for timeframe in self.subscription.timeframes:
                try: bars=self.adapter.completed_bars(spec.broker_symbol,timeframe,self.subscription.bars_per_capture)
                except Exception as exc:
                    reject("bar_read_failed");bar_error=True;pair_freshness[timeframe]={"status":"read_error","usable":False,"reason":"bar_read_failed","expected_count":self.subscription.bars_per_capture,"actual_count":0,"completeness":"unknown"};self.store.event("bar_read_failed",{"pair":pair,"timeframe":timeframe,"error_type":type(exc).__name__},at=now);continue
                bar_validation_time=validation_time()
                normalized=[];series_error=None
                for bar in bars:
                    try:
                        stamp=_utc(bar["time_utc"],"bar time")
                        prices=[bar[k] for k in ("open","high","low","close")]
                        if any(not isinstance(x,(int,float)) or isinstance(x,bool) or not math.isfinite(float(x)) or x<=0 for x in prices):raise ValueError("bad OHLC")
                        if float(bar["high"])<max(float(bar["open"]),float(bar["close"])) or float(bar["low"])>min(float(bar["open"]),float(bar["close"])) or float(bar["high"])<float(bar["low"]):raise ValueError("bad OHLC geometry")
                        spread_points=bar["spread_points"];volume=bar["tick_volume"]
                        if not isinstance(spread_points,int) or spread_points<0 or not isinstance(volume,int) or volume<0:raise ValueError("bad bar metadata")
                    except (KeyError,TypeError,ValueError):
                        reject("invalid_bar");bar_error=True;series_error={"status":"invalid","usable":False,"reason":"invalid_bar"};break
                    if stamp>=bar_validation_time:
                        reject("future_bar");bar_error=True;series_error={"status":"future","usable":False,"reason":"future_bar","rejected_bar_time_utc":stamp.isoformat()};break
                    normalized.append({**bar,"time_utc":stamp})
                adapter_diagnostics=getattr(self.adapter,"last_bar_diagnostics",None)
                if isinstance(adapter_diagnostics,dict) and adapter_diagnostics.get("symbol")==spec.broker_symbol and adapter_diagnostics.get("timeframe")==timeframe.upper():
                    adapter_diagnostics={k:v for k,v in adapter_diagnostics.items() if k not in ("symbol","timeframe")}
                else:adapter_diagnostics=None
                if series_error is not None:
                    series_error.update({"expected_count":self.subscription.bars_per_capture,"actual_count":len(normalized),"completeness":"invalid","adapter_diagnostics":adapter_diagnostics})
                    pair_freshness[timeframe]=series_error
                    continue
                if any(normalized[i]["time_utc"]>=normalized[i+1]["time_utc"] for i in range(len(normalized)-1)):
                    reject("bar_order_invalid");bar_error=True;pair_freshness[timeframe]={"status":"invalid","usable":False,"reason":"bar_order_invalid","expected_count":self.subscription.bars_per_capture,"actual_count":len(normalized),"completeness":"invalid","adapter_diagnostics":adapter_diagnostics};continue
                bars_by_tf[timeframe]=normalized
                seconds=TIMEFRAME_SECONDS[timeframe]
                limit=self.subscription.max_bar_age_seconds[timeframe]
                gap_diagnostics,gap_completeness=_assess_internal_gaps(normalized,timeframe,getattr(spec,"sessions",()),getattr(self.adapter,"server_timezone",None),getattr(spec,"session_schedule_complete",False))
                actual_count=len(normalized);expected_count=self.subscription.bars_per_capture
                completeness="incomplete" if actual_count!=expected_count else gap_completeness
                if actual_count!=expected_count:note_quality("bar_count_incomplete")
                if gap_completeness!="complete":note_quality("gap_"+gap_completeness)
                if not normalized:
                    pair_freshness[timeframe]={"status":"incomplete","usable":False,"reason":"no_completed_bars","freshness":"unknown","expected_count":expected_count,"actual_count":0,"completeness":completeness,"gaps":gap_diagnostics,"max_age_seconds":limit,"adapter_diagnostics":adapter_diagnostics}
                    note_quality("missing_timeframe")
                    continue
                newest=normalized[-1]["time_utc"];close_time=newest+timedelta(seconds=seconds)
                age_now=(bar_validation_time-close_time).total_seconds();age_quote=(quote_time-close_time).total_seconds()
                if age_now<0:
                    freshness_state,reason="incomplete","bar_close_after_validation_time"
                elif age_quote<0:
                    freshness_state,reason="incomplete","bar_close_after_quote_time"
                elif age_now>limit or age_quote>limit:
                    freshness_state,reason="stale","latest_completed_bar_exceeds_age_limit"
                else:
                    freshness_state,reason="fresh",None
                state=(freshness_state if freshness_state!="fresh" else ("fresh" if completeness=="complete" else completeness))
                usable=freshness_state=="fresh" and completeness=="complete"
                pair_freshness[timeframe]={"status":state,"freshness":freshness_state,"usable":usable,"reason":reason if reason else (None if usable else "series_completeness_not_established"),"expected_count":expected_count,"actual_count":actual_count,"completeness":completeness,"gaps":gap_diagnostics,"session_schedule_complete":bool(getattr(spec,"session_schedule_complete",False)),"newest_bar_open_utc":newest.isoformat(),"newest_bar_close_utc":close_time.isoformat(),"age_at_validation_seconds":age_now,"age_at_quote_seconds":age_quote,"max_age_seconds":limit,"session_state":"active_quote","adapter_diagnostics":adapter_diagnostics}
                if freshness_state!="fresh":note_quality(freshness_state+"_timeframe")
                for gap in gap_diagnostics:
                    gaps.append({"pair":pair,"timeframe":timeframe,**gap})
            timeframe_freshness[pair]=pair_freshness
            self.store.event("timeframe_freshness",{"pair":pair,"symbol":spec.broker_symbol,"timeframes":pair_freshness},at=now)
            if bar_error:continue
            spread=(quote.ask-quote.bid)/spec.point if spec.point>0 else None
            observation={"schema_version":1,"broker":account.broker,"server":account.server,"account_currency":account.currency,
                "pair":pair,"symbol":spec.broker_symbol,"quote_time_utc":quote_time,"time_utc":quote_time,
                "collected_utc":now,"bid":quote.bid,"ask":quote.ask,"spread":quote.ask-quote.bid,
                "spread_points":spread,"spread_source":"broker_tick_bid_ask","ohlc_source":"broker_copy_rates_completed_bars",
                "required_timeframes":list(self.subscription.timeframes),"bars_per_capture":self.subscription.bars_per_capture,
                "ohlc_price_basis":"broker_chart_mode; not reconstructed","bars":bars_by_tf,"timeframe_freshness":pair_freshness,
                "commission":"unavailable_not_inferred","swap":"unavailable_not_inferred","slippage":"unavailable_not_inferred"}
            result=self.store.append(observation)
            if result=="duplicate":reject("duplicate_observation");self.store.event("duplicate_observation",{"pair":pair,"symbol":spec.broker_symbol,"quote_time_utc":quote_time.isoformat()},at=now)
            elif result=="out_of_order":reject("out_of_order_observation");self.store.event("out_of_order_observation",{"pair":pair,"symbol":spec.broker_symbol,"quote_time_utc":quote_time.isoformat()},at=now)
            else:accepted+=1
        for gap in gaps:self.store.event("possible_missing_candle",gap,at=now)
        fully_healthy=accepted==len(self.subscription.pairs) and all(timeframe_freshness.get(pair,{}).get(tf,{}).get("status")=="fresh" for pair in self.subscription.pairs for tf in self.subscription.timeframes)
        status="healthy" if fully_healthy else ("partial" if accepted or timeframe_freshness else "no_valid_observations")
        self.store.event("capture_complete",{"accepted":accepted,"rejected":sum(reasons.values()),"gaps":len(gaps),"status":status,"quality_issues":quality_issues,"timeframe_freshness":timeframe_freshness},at=now)
        return {"status":status,"observations":accepted,"rejected":sum(reasons.values()),"reasons":reasons,"quality_issues":quality_issues,"timeframe_freshness":timeframe_freshness,"possible_missing_candles":gaps,"broker":account.broker,"server":account.server,"collection_time_utc":now.isoformat(),"store_verified":self.store.verify()}
