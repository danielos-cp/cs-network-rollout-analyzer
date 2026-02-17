# analyzer.py
from __future__ import annotations
import json, os, re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

def try_json_loads(s: Any) -> Optional[Dict[str, Any]]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    if isinstance(s, dict):
        return s
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    if not s:
        return None
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        try:
            s2 = s.encode("utf-8").decode("unicode_escape")
            obj = json.loads(s2)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

def norm_status(x: Any) -> str:
    if not isinstance(x, str):
        return "unknown"
    xl = x.lower()
    if "success" in xl:
        return "success"
    if "failed" in xl or "fail" in xl:
        return "failed"
    if "init" in xl:
        return "init"
    if "running" in xl:
        return "running"
    return "other"

def extract_error_fields(s: Any) -> Dict[str, Any]:
    lb = try_json_loads(s)
    if not isinstance(lb, dict):
        return {"beacon_status": None, "error_message": None, "beacon_progress": None, "beacon_action": None}
    return {
        "beacon_status": lb.get("networkBeaconStatus"),
        "error_message": lb.get("errorMessage"),
        "beacon_progress": lb.get("networkBeaconProgress"),
        "beacon_action": lb.get("action"),
    }

IP_RE      = re.compile(r"\b(?:(?:\d{1,3}\.){3}\d{1,3})\b")
HEX_ID_RE  = re.compile(r"\b[0-9a-fA-F]{6,}\b")
NUM_ID_RE  = re.compile(r"\b\d{6,}\b")
ISO_DT_RE  = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z")
WS_RE      = re.compile(r"\s+")
BRACKET_RE = re.compile(r"\[[^\]]+\]")
CODE1      = re.compile(r"ErrorCode:\s*(\d+)")
CODE2      = re.compile(r"^(\d{3})\s*-\s*")

EMAIL_RE = re.compile(r"(?i)(?<![\w\.-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?![\w\.-])")

def redact_emails_in_string(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    return EMAIL_RE.sub("<redacted_email>", s)

def sanitize_drop_email_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["updatedByEmail"]:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df

def sanitize_redact_emails(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(redact_emails_in_string)
    return df

def fix_spaced_letters(msg: Optional[str]) -> Optional[str]:
    if not isinstance(msg, str):
        return msg
    tokens = msg.split()
    if not tokens:
        return msg
    single_ratio = sum(1 for t in tokens if len(t) == 1) / len(tokens)
    if single_ratio > 0.5:
        return "".join(tokens)
    return msg

def canonical_error_full(row: pd.Series) -> Optional[str]:
    msg = row.get("error_message")
    if not isinstance(msg, str) or not msg.strip():
        msg = row.get("beacon_status")
    return fix_spaced_letters(msg) if (isinstance(msg, str) and msg.strip()) else None

def normalize_error_text(msg: Optional[str]) -> Optional[str]:
    if not isinstance(msg, str):
        return None
    m = msg.replace("\n", " ")
    m = BRACKET_RE.sub("[...]", m)
    m = IP_RE.sub("<IP>", m)
    m = ISO_DT_RE.sub("<ISO-DATETIME>", m)
    m = NUM_ID_RE.sub("<NUM>", m)
    m = HEX_ID_RE.sub("<HEX>", m)
    m = WS_RE.sub(" ", m).strip()
    return m

def extract_code(msg: Optional[str]) -> Optional[str]:
    if not isinstance(msg, str):
        return None
    m = CODE1.search(msg)
    if m:
        return m.group(1)
    m = CODE2.search(msg)
    if m:
        return m.group(1)
    return None

def combine_dataframes(dfs: List[pd.DataFrame], names: List[str]) -> pd.DataFrame:
    frames = []
    for df, name in zip(dfs, names):
        df = df.copy()
        df["source_file"] = os.path.basename(name) if name else "uploaded.csv"
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=0, ignore_index=True)

def analyze_dataframes(
    dataframes: List[pd.DataFrame],
    file_names: List[str],
    top_n: int = 10,
    dedupe_rows: bool = False
) -> Tuple[Dict[str, Any], Dict[str, pd.DataFrame]]:
    df = combine_dataframes(dataframes, file_names)

    for c in ["datetime", "action", "status", "networkName", "networkId", "lastBeacon"]:
        if c not in df.columns:
            df[c] = None

    if dedupe_rows:
        df = df.drop_duplicates().reset_index(drop=True)

    df["status_norm"] = df["status"].apply(norm_status)

    lb_df = df["lastBeacon"].apply(extract_error_fields).apply(pd.Series)
    df = pd.concat([df, lb_df], axis=1)

    df = sanitize_drop_email_columns(df)
    df = sanitize_redact_emails(df)

    op_success = int((df["status_norm"] == "success").sum())
    op_failed  = int((df["status_norm"] == "failed").sum())

    net_grp         = df.groupby("networkId", dropna=False)
    net_has_success = net_grp["status_norm"].apply(lambda s: (s == "success").any())
    net_has_failed  = net_grp["status_norm"].apply(lambda s: (s == "failed").any())

    broken_mask        = (net_has_failed) & (~net_has_success)
    broken_network_ids = list(net_has_success.index[broken_mask])

    failed_ops = df[df["status_norm"] == "failed"].copy()
    failed_ops["raw_error_full"] = failed_ops.apply(canonical_error_full, axis=1)
    failed_ops["error_norm"]     = failed_ops["raw_error_full"].apply(normalize_error_text)
    failed_ops["error_code"]     = failed_ops["raw_error_full"].apply(extract_code)

    def pick_example_message(g: pd.DataFrame) -> str:
        g2 = g.sort_values("datetime", ascending=False) if "datetime" in g.columns else g
        for m in g2["raw_error_full"]:
            if isinstance(m, str) and m.strip():
                return m
        return ""

    error_freq = (
        failed_ops
        .groupby(["error_code", "error_norm"], dropna=False)
        .agg(
            count=("error_norm", "size"),
            example_message=("raw_error_full", lambda s: pick_example_message(failed_ops.loc[s.index])),
        )
        .reset_index()
        .sort_values(["count", "error_code", "error_norm"], ascending=[False, True, True])
    )

    fail_by_action = (
        failed_ops.groupby("action").size().reset_index(name="failed_ops")
        .sort_values("failed_ops", ascending=False)
    )

    broken_base = df[df["networkId"].isin(broken_network_ids)].copy()
    if "datetime" in broken_base.columns:
        broken_base = broken_base.sort_values("datetime", ascending=True)
    broken_df = (
        broken_base
        .drop_duplicates(subset=["networkId"], keep="last")
        .loc[:, ["datetime","networkName","networkId","action","status","error_message","beacon_status","beacon_progress","source_file"]]
        .sort_values("datetime", ascending=False)
        .reset_index(drop=True)
    )

    artifacts = {
        "failed_operations": sanitize_drop_email_columns(sanitize_redact_emails(failed_ops)),
        "error_summary": sanitize_redact_emails(error_freq),
        "failures_by_action": fail_by_action,
        "broken_networks": sanitize_drop_email_columns(sanitize_redact_emails(broken_df)),
        "combined": sanitize_drop_email_columns(sanitize_redact_emails(df)),
    }

    summary = {
        "inputs": {
            "files": [os.path.basename(n) for n in file_names],
            "rows_total": int(len(df)),
            "dedupe_rows": bool(dedupe_rows),
        },
        "operations": {
            "total": int(len(df)),
            "success": op_success,
            "failed": op_failed,
            "other": int(len(df) - op_success - op_failed),
        },
        "networks": {
            "unique": int(df["networkId"].nunique()),
            "with_success": int(net_has_success.sum()),
            "with_failed": int(net_has_failed.sum()),
            "broken_networks_count": int(len(broken_network_ids)),
        },
        "top_errors": artifacts["error_summary"].head(top_n).to_dict(orient="records"),
    }

    return summary, artifacts