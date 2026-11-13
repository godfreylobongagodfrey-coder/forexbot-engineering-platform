"""Append-only local JSONL decision journal; never accepts credential fields."""
import json
from datetime import datetime, timezone
from pathlib import Path

_FORBIDDEN={"api_key","token","password","authorization","secret"}
def _contains_secret_key(value) -> bool:
    if isinstance(value,dict): return any(k.lower() in _FORBIDDEN or _contains_secret_key(v) for k,v in value.items())
    if isinstance(value,(list,tuple)): return any(_contains_secret_key(v) for v in value)
    return False
def record(path: str | Path, event: dict) -> None:
    if not isinstance(event,dict) or _contains_secret_key(event): raise ValueError("Audit event contains forbidden fields")
    row={"timestamp_utc":datetime.now(timezone.utc).isoformat(),**event}
    target=Path(path); target.parent.mkdir(parents=True,exist_ok=True)
    with target.open("a",encoding="utf-8") as f: f.write(json.dumps(row,allow_nan=False,separators=(",",":"))+"\n")
