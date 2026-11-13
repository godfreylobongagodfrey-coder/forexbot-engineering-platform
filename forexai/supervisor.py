"""Small persistent scheduler with bounded attempts and explicit recovery states."""
from __future__ import annotations

from datetime import datetime,timezone,timedelta
from pathlib import Path
import sqlite3
from .sqlite_support import sqlite_session


class SupervisorError(RuntimeError): pass

def _time(value):
    value=value or datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None: raise ValueError("Supervisor timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)

class TaskSupervisor:
    """Runs registered offline/read-only jobs once when due; never restarts forever."""
    def __init__(self,path:str|Path,*,max_tasks_per_cycle:int=4,max_backoff_seconds:int=3600):
        if not 1<=max_tasks_per_cycle<=16 or max_backoff_seconds<1: raise ValueError("Supervisor resource limits are invalid")
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.max_tasks_per_cycle=max_tasks_per_cycle;self.max_backoff_seconds=max_backoff_seconds
        exists=self.path.exists()
        try:
            with sqlite_session(self.path) as db:
                if exists and db.execute("PRAGMA quick_check").fetchone()[0]!="ok":raise SupervisorError("Task state integrity check failed")
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("""CREATE TABLE IF NOT EXISTS tasks(name TEXT PRIMARY KEY,interval_seconds INTEGER NOT NULL,max_attempts INTEGER NOT NULL,base_backoff_seconds INTEGER NOT NULL,next_run_utc TEXT NOT NULL,status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,last_started_utc TEXT,last_success_utc TEXT,last_error TEXT)""")
                db.execute("""CREATE TABLE IF NOT EXISTS task_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,at_utc TEXT NOT NULL,name TEXT NOT NULL,event TEXT NOT NULL,details TEXT NOT NULL)""")
        except (sqlite3.DatabaseError,OSError) as exc:
            if isinstance(exc,SupervisorError):raise
            raise SupervisorError("Persistent task state unavailable; supervisor fails closed") from exc

    def register(self,name:str,handler_interval_seconds:int,*,max_attempts:int=3,base_backoff_seconds:int=5,now:datetime|None=None):
        now=_time(now)
        if not name or not 1<=handler_interval_seconds<=31_536_000 or not 1<=max_attempts<=10 or not 1<=base_backoff_seconds<=self.max_backoff_seconds:raise ValueError("Task schedule is outside bounded limits")
        try:
            with sqlite_session(self.path,timeout=2) as db:
                row=db.execute("SELECT status FROM tasks WHERE name=?",(name,)).fetchone()
                if row and row[0]=="running":raise SupervisorError("Cannot reconfigure a running task")
                db.execute("INSERT INTO tasks(name,interval_seconds,max_attempts,base_backoff_seconds,next_run_utc,status,attempts) VALUES(?,?,?,?,?,'scheduled',0) ON CONFLICT(name) DO UPDATE SET interval_seconds=excluded.interval_seconds,max_attempts=excluded.max_attempts,base_backoff_seconds=excluded.base_backoff_seconds",(name,handler_interval_seconds,max_attempts,base_backoff_seconds,now.isoformat()))
                db.execute("INSERT INTO task_events(at_utc,name,event,details) VALUES(?,?,?,?)",(now.isoformat(),name,"registered",""))
        except sqlite3.DatabaseError as exc:raise SupervisorError("Task registration failed") from exc

    def reset_failed(self,name:str,*,operator_confirmed:bool,now:datetime|None=None):
        if operator_confirmed is not True:raise PermissionError("Resetting a failed task requires explicit operator confirmation")
        now=_time(now)
        with sqlite_session(self.path,timeout=2) as db:
            row=db.execute("SELECT status FROM tasks WHERE name=?",(name,)).fetchone()
            if not row or row[0]!="failed":raise SupervisorError("Only a failed task can be manually reset")
            db.execute("UPDATE tasks SET status='scheduled',attempts=0,next_run_utc=?,last_error=NULL WHERE name=?",(now.isoformat(),name))
            db.execute("INSERT INTO task_events(at_utc,name,event,details) VALUES(?,?,?,?)",(now.isoformat(),name,"operator_reset",""))

    def recover_after_restart(self,*,now:datetime|None=None)->int:
        now=_time(now);changed=0
        with sqlite_session(self.path,timeout=2) as db:
            db.execute("BEGIN IMMEDIATE")
            rows=db.execute("SELECT name,attempts,max_attempts,base_backoff_seconds FROM tasks WHERE status='running'").fetchall()
            for name,attempts,max_attempts,base in rows:
                if attempts>=max_attempts:
                    status="failed";next_run=now
                else:
                    status="retry_wait";delay=min(self.max_backoff_seconds,base*(2**max(0,attempts-1)));next_run=now+timedelta(seconds=delay)
                db.execute("UPDATE tasks SET status=?,next_run_utc=?,last_error='interrupted_by_restart' WHERE name=?",(status,next_run.isoformat(),name))
                db.execute("INSERT INTO task_events(at_utc,name,event,details) VALUES(?,?,?,?)",(now.isoformat(),name,"restart_recovery",status));changed+=1
        return changed

    def run_due(self,handlers:dict,*,now:datetime|None=None)->list[dict]:
        now=_time(now);results=[]
        with sqlite_session(self.path,timeout=2) as db:
            names=[r[0] for r in db.execute("SELECT name FROM tasks WHERE status IN ('scheduled','retry_wait') AND next_run_utc<=? ORDER BY next_run_utc,name LIMIT ?",(now.isoformat(),self.max_tasks_per_cycle))]
        for name in names:
            with sqlite_session(self.path,timeout=2) as db:
                db.execute("BEGIN IMMEDIATE")
                row=db.execute("SELECT interval_seconds,max_attempts,base_backoff_seconds,attempts FROM tasks WHERE name=? AND status IN ('scheduled','retry_wait') AND next_run_utc<=?",(name,now.isoformat())).fetchone()
                if not row:continue
                interval,max_attempts,base,attempts=row;attempts+=1
                db.execute("UPDATE tasks SET status='running',attempts=?,last_started_utc=? WHERE name=?",(attempts,now.isoformat(),name))
                db.execute("INSERT INTO task_events(at_utc,name,event,details) VALUES(?,?,?,?)",(now.isoformat(),name,"started",str(attempts)))
            handler=handlers.get(name)
            try:
                if handler is None:raise LookupError("No handler registered for due task")
                value=handler()
                status="succeeded";error=""
            except Exception as exc:
                # Never persist exception messages; provider/adapter exceptions may contain secrets.
                error=f"{type(exc).__name__}: task handler failed"
                if attempts>=max_attempts:status="failed";next_run=now
                else:status="retry_wait";next_run=now+timedelta(seconds=min(self.max_backoff_seconds,base*(2**(attempts-1))))
                value=None
            else:
                next_run=now+timedelta(seconds=interval)
            with sqlite_session(self.path,timeout=2) as db:
                db.execute("BEGIN IMMEDIATE")
                db.execute("UPDATE tasks SET status=?,next_run_utc=?,attempts=?,last_error=?,last_success_utc=CASE WHEN ?='succeeded' THEN ? ELSE last_success_utc END WHERE name=?",(status,next_run.isoformat(),0 if status=='succeeded' else attempts,error,status,now.isoformat(),name))
                db.execute("INSERT INTO task_events(at_utc,name,event,details) VALUES(?,?,?,?)",(now.isoformat(),name,status,error))
            results.append({"task":name,"status":status,"attempt":attempts,"error":error,"result":value})
        return results

    def status(self)->list[dict]:
        with sqlite_session(self.path,row_factory=sqlite3.Row) as db:
            return [dict(row) for row in db.execute("SELECT * FROM tasks ORDER BY name")]
