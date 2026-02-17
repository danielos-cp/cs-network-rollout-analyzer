# app.py
import os
os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"

import pandas as pd
import streamlit as st
from analyzer import analyze_dataframes

st.set_page_config(page_title="Network Rollout Analyzer", page_icon="📡", layout="wide")
st.title("📡 Network Rollout Analyzer")
st.write(
    "Upload one or more **CSV** files exported from Jarvis. "
    "The app analyzes them **as one dataset** and shows status, broken networks, and recurring errors."
)

with st.sidebar:
    st.header("Options")
    top_n = st.number_input("Top-N errors", min_value=3, max_value=50, value=10, step=1)
    st.caption("De-duplication is always enabled.")

uploaded_files = st.file_uploader(
    "Upload CSV log file(s)",
    type=["csv"],
    accept_multiple_files=True,
    help="You can upload several files at once."
)

if not uploaded_files:
    st.info("⬆️ Upload at least one CSV to begin.")
    st.stop()

dataframes, names = [], []
for f in uploaded_files:
    try:
        df = pd.read_csv(f)
    except Exception:
        f.seek(0)
        df = pd.read_csv(f, encoding="utf-8-sig")
    dataframes.append(df)
    names.append(f.name)

with st.spinner("Analyzing…"):
    summary, artifacts = analyze_dataframes(
        dataframes=dataframes,
        file_names=names,
        top_n=int(top_n),
        dedupe_rows=True,
    )
st.success("Analysis complete.")

# Summary
st.subheader("Summary")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total operations", f"{summary['operations']['total']:,}")
col2.metric("Success", f"{summary['operations']['success']:,}")
col3.metric("Failed", f"{summary['operations']['failed']:,}")
col4.metric("Other", f"{summary['operations']['other']:,}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Unique networks", f"{summary['networks']['unique']:,}")
c2.metric("Networks w/ success", f"{summary['networks']['with_success']:,}")
c3.metric("Networks w/ failures", f"{summary['networks']['with_failed']:,}")
c4.metric(
    "Broken networks",
    f"{summary['networks']['broken_networks_count']:,}",
    help="Has failures and no successes"
)

with st.expander("Inputs"):
    st.write("Files:", ", ".join(summary["inputs"]["files"]))
    st.write("Rows total:", f"{summary['inputs']['rows_total']:,}")
    st.write("De-duplicate identical rows:", True)

st.markdown("---")

# Tenant fields
TENANT_FIRST = ["customerId", "companySize"]
TENANT_LABELS = {
    "customerId": "Tenant ID",
    "companySize": "Company Size"
}

BROKEN_SHOW_ORDER = TENANT_FIRST + [
    "datetime", "networkName", "networkId", "action", "status",
    "error_message", "beacon_status", "beacon_progress", "source_file"
]

FAILED_SHOW_ORDER = TENANT_FIRST + [
    "datetime", "source_file", "networkName", "networkId", "action", "status",
    "raw_error_full", "beacon_progress", "beacon_status"
]

def _present_columns(df: pd.DataFrame, desired: list[str]) -> list[str]:
    return [c for c in desired if c in df.columns]

def _rename_for_display(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=TENANT_LABELS)

# Broken Networks
st.subheader("Broken Networks")
broken_df = artifacts["broken_networks"]
if broken_df.empty:
    st.success("✅ No broken networks found.")
else:
    show_cols = _present_columns(broken_df, BROKEN_SHOW_ORDER)
    display_df = _rename_for_display(broken_df[show_cols])
    st.dataframe(display_df, use_container_width=True)
    st.download_button(
        "⬇️ Download broken_networks.csv",
        data=broken_df.to_csv(index=False).encode("utf-8"),
        file_name="broken_networks.csv"
    )

st.markdown("---")

# Top errors
st.subheader("Top Recurring Errors")
top_errors = pd.DataFrame(summary["top_errors"])
if not top_errors.empty:
    cols = ["error_code", "count", "example_message"]
    present = [c for c in cols if c in top_errors.columns]
    st.dataframe(top_errors[present], use_container_width=True)
    st.download_button(
        "⬇️ Download error_summary.csv",
        data=artifacts["error_summary"].to_csv(index=False).encode("utf-8"),
        file_name="error_summary.csv"
    )
else:
    st.info("No failed operations detected.")

st.markdown("---")

# Failures by action
st.subheader("Failures by Action")
fail_by_action = artifacts["failures_by_action"]
if not fail_by_action.empty:
    st.dataframe(fail_by_action, use_container_width=True)
    st.bar_chart(fail_by_action.set_index("action")["failed_ops"])
    st.download_button(
        "⬇️ Download failures_by_action.csv",
        data=fail_by_action.to_csv(index=False).encode("utf-8"),
        file_name="failures_by_action.csv"
    )
else:
    st.info("No failed operations by action.")

st.markdown("---")

# Failed operations
st.subheader("Failed Operations")
failed_ops = artifacts["failed_operations"]
if not failed_ops.empty:
    show_cols = _present_columns(failed_ops, FAILED_SHOW_ORDER)
    display_df = _rename_for_display(failed_ops[show_cols])
    st.dataframe(display_df, use_container_width=True, height=350)
    st.download_button(
        "⬇️ Download failed_operations.csv",
        data=failed_ops.to_csv(index=False).encode("utf-8"),
        file_name="failed_operations.csv"
    )
else:
    st.info("No failed operations found.")

st.markdown("---")

# Combined dataset
with st.expander("Combined dataset"):
    combined = artifacts["combined"]
    st.caption("Full enriched dataset (emails redacted, email columns removed).")
    combined_cols = TENANT_FIRST + [c for c in combined.columns if c not in TENANT_FIRST]
    preview = _rename_for_display(combined[combined_cols]).head(100)
    st.dataframe(preview, use_container_width=True)
    st.download_button(
        "⬇️ Download combined.csv",
        data=combined.to_csv(index=False).encode("utf-8"),
        file_name="combined.csv"
    )
