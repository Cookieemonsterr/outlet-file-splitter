import streamlit as st
import pandas as pd
import zipfile
import io
import re

st.set_page_config(page_title="Outlet Splitter", layout="centered")
st.title("🧩 Outlet Splitter & CSV Converter")
st.caption("Upload any file → get Google-Sheets-ready CSVs + split by outlet")

# ---------- Helpers ----------

def clean_columns(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def is_numeric_header(col):
    return bool(re.fullmatch(r"\d{5,}", str(col).strip()))

def detect_outlet_row_column(df):
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in ["outlet", "store", "branch", "site"]):
            if df[c].nunique(dropna=True) < len(df) * 0.3:
                return c
    return None

def detect_outlet_columns(df):
    return [c for c in df.columns if is_numeric_header(c)]

def to_csv_bytes(df):
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

# ---------- Upload ----------

uploaded = st.file_uploader(
    "Upload file",
    type=["csv", "tsv", "txt", "xlsx", "xls", "json"]
)

if uploaded:
    # ---------- Read file ----------
    if uploaded.name.endswith(("xlsx", "xls")):
        df = pd.read_excel(uploaded, dtype=object)
    elif uploaded.name.endswith("json"):
        df = pd.read_json(uploaded)
    else:
        sep = "\t" if uploaded.name.endswith("tsv") else ","
        df = pd.read_csv(uploaded, sep=sep, dtype=object)

    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df = clean_columns(df)

    st.success(f"Loaded {len(df)} rows × {len(df.columns)} columns")

    # ---------- Detect structure ----------
    outlet_row_col = detect_outlet_row_column(df)
    outlet_cols = detect_outlet_columns(df)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:

        # ---------- CASE 1: Outlet as columns ----------
        if outlet_cols:
            base_cols = [c for c in df.columns if c not in outlet_cols]

            # Combined
            z.writestr("combined.csv", to_csv_bytes(df))

            # Long format
            long_df = df.melt(
                id_vars=base_cols,
                value_vars=outlet_cols,
                var_name="outlet_id",
                value_name="outlet_value"
            )
            z.writestr("long_format.csv", to_csv_bytes(long_df))

            # Per outlet
            for oc in outlet_cols:
                out_df = df[base_cols + [oc]].copy()
                out_df.insert(0, "outlet_id", oc)
                out_df = out_df.rename(columns={oc: "outlet_value"})
                z.writestr(f"outlet_{oc}.csv", to_csv_bytes(out_df))

            st.info("Detected outlets as COLUMNS")

        # ---------- CASE 2: Outlet as rows ----------
        elif outlet_row_col:
            z.writestr("combined.csv", to_csv_bytes(df))

            for outlet, grp in df.groupby(outlet_row_col):
                name = str(outlet).replace("/", "-")
                z.writestr(f"outlet_{name}.csv", to_csv_bytes(grp))

            st.info(f"Detected outlet column: `{outlet_row_col}`")

        else:
            z.writestr("combined.csv", to_csv_bytes(df))
            st.warning("No outlet detected — exported combined CSV only")

    st.download_button(
        "⬇️ Download results (ZIP)",
        zip_buffer.getvalue(),
        file_name="outlet_outputs.zip",
        mime="application/zip"
    )
