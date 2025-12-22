import streamlit as st
import pandas as pd
import zipfile
import io
import re

st.set_page_config(page_title="Outlet Splitter", layout="centered")
st.title("🧩 Outlet Splitter & CSV Converter")
st.caption("Upload file(s) → get Google-Sheets-ready CSVs + split by outlet (rows / columns / sheets)")

SUPPORTED_TYPES = ["csv", "tsv", "txt", "xlsx", "xls", "json"]

# ---------------- Helpers ----------------

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]
    # strip strings
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def safe_name(s: str) -> str:
    s = str(s)
    s = s.replace("/", "-").replace("\\", "-")
    s = re.sub(r'[<>:"|?*]', "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120] if s else "UNKNOWN"

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def is_numeric_header(col) -> bool:
    return bool(re.fullmatch(r"\d{5,}", str(col).strip()))

def detect_outlet_columns(df: pd.DataFrame):
    return [c for c in df.columns if is_numeric_header(c)]

def detect_outlet_row_column(df: pd.DataFrame):
    """
    Finds a column that likely represents outlet/store/branch/site.
    Prefers columns that repeat (unique ratio not too high).
    """
    for c in df.columns:
        lc = str(c).lower()
        if any(k in lc for k in ["outlet", "store", "branch", "site", "location"]):
            nun = df[c].nunique(dropna=True)
            if len(df) == 0:
                continue
            ratio = nun / max(1, len(df))
            # outlet identifier should repeat across rows
            if nun >= 2 and ratio < 0.4:
                return c
    return None

# ✅ NEW: robust reader for CSV/TSV/TXT (fixes UnicodeDecodeError)
def read_text_table_with_fallback(uploaded, sep: str) -> tuple[pd.DataFrame, str]:
    """
    Tries multiple encodings so the app doesn't crash on non-UTF8 CSV exports.
    Returns: (df, encoding_used)
    """
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None

    for enc in encodings_to_try:
        try:
            uploaded.seek(0)  # IMPORTANT: reset pointer before each attempt
            df = pd.read_csv(uploaded, sep=sep, dtype=object, encoding=enc)
            return df, enc
        except Exception as e:
            last_err = e

    raise last_err

def read_any_file(uploaded):
    name = uploaded.name.lower()

    if name.endswith(("xlsx", "xls")):
        # Return dict of sheets
        uploaded.seek(0)
        sheets = pd.read_excel(uploaded, sheet_name=None, dtype=object)
        cleaned = {}
        for sh, df in sheets.items():
            if df is None or getattr(df, "empty", True):
                continue
            df = clean_df(df)
            if not df.empty:
                cleaned[str(sh).strip()] = df
        return {"type": "excel", "sheets": cleaned}

    if name.endswith("json"):
        uploaded.seek(0)
        df = pd.read_json(uploaded)
        df = clean_df(df)
        return {"type": "table", "df": df}

    # csv/tsv/txt ✅ now uses fallback encodings
    sep = "\t" if name.endswith("tsv") else ","
    df, used_encoding = read_text_table_with_fallback(uploaded, sep=sep)
    df = clean_df(df)
    return {"type": "table", "df": df, "encoding": used_encoding}

# ---------------- UI ----------------

uploaded_files = st.file_uploader(
    "Upload file(s)",
    type=SUPPORTED_TYPES,
    accept_multiple_files=True
)

if uploaded_files:
    big_zip = io.BytesIO()

    with zipfile.ZipFile(big_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for uploaded in uploaded_files:
            folder = safe_name(uploaded.name)
            result = read_any_file(uploaded)

            # Optional: record encoding used (only for csv/tsv/txt)
            if result.get("encoding"):
                z.writestr(f"{folder}/INFO_encoding.txt", f"Read using encoding: {result['encoding']}")

            # ===========================
            # CASE A: Excel with MULTIPLE SHEETS (each sheet = outlet)
            # ===========================
            if result["type"] == "excel":
                sheets = result["sheets"]

                if len(sheets) == 0:
                    z.writestr(f"{folder}/ERROR.txt", "No readable data found in this Excel file.")
                    continue

                if len(sheets) > 1:
                    # combined = stack sheets with outlet_id = sheet name
                    combined_frames = []
                    for sh, df_sh in sheets.items():
                        out = df_sh.copy()
                        out.insert(0, "outlet_id", sh)
                        out.insert(1, "_sheet", sh)
                        combined_frames.append(out)

                        z.writestr(
                            f"{folder}/outlet_{safe_name(sh)}.csv",
                            to_csv_bytes(out)
                        )

                    combined_df = pd.concat(combined_frames, ignore_index=True)
                    z.writestr(f"{folder}/combined.csv", to_csv_bytes(combined_df))

                    # For sheet-based outlets, "long_format" is basically combined with outlet_id
                    z.writestr(f"{folder}/long_format.csv", to_csv_bytes(combined_df))

                    z.writestr(
                        f"{folder}/INFO.txt",
                        "Detected multiple sheets → treated each sheet as an outlet."
                    )
                    continue  # IMPORTANT: do not run row/column outlet detection

                # If ONLY ONE sheet → fall back to normal detection
                df = list(sheets.values())[0]

            else:
                df = result["df"]

            if df is None or df.empty:
                z.writestr(f"{folder}/ERROR.txt", "No readable rows found.")
                continue

            # ===========================
            # CASE B: Outlet as COLUMNS (numeric outlet ids as headers)
            # ===========================
            outlet_cols = detect_outlet_columns(df)
            if outlet_cols:
                base_cols = [c for c in df.columns if c not in outlet_cols]

                # combined
                z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))

                # long
                long_df = df.melt(
                    id_vars=base_cols,
                    value_vars=outlet_cols,
                    var_name="outlet_id",
                    value_name="outlet_value"
                )
                z.writestr(f"{folder}/long_format.csv", to_csv_bytes(long_df))

                # per outlet
                for oc in outlet_cols:
                    out_df = df[base_cols + [oc]].copy()
                    out_df.insert(0, "outlet_id", oc)
                    out_df = out_df.rename(columns={oc: "outlet_value"})
                    z.writestr(f"{folder}/outlet_{safe_name(oc)}.csv", to_csv_bytes(out_df))

                z.writestr(
                    f"{folder}/INFO.txt",
                    "Detected outlets as COLUMNS (numeric outlet ids in headers)."
                )
                continue

            # ===========================
            # CASE C: Outlet as ROWS (a column like store/outlet/site/branch)
            # ===========================
            outlet_row_col = detect_outlet_row_column(df)
            if outlet_row_col:
                z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))

                # per outlet
                for outlet, grp in df.groupby(outlet_row_col, dropna=False):
                    grp = grp.copy()
                    grp.insert(0, "outlet_id", outlet)
                    z.writestr(f"{folder}/outlet_{safe_name(outlet)}.csv", to_csv_bytes(grp))

                # long_format = combined with outlet_id column (same idea)
                long_df = df.copy()
                long_df.insert(0, "outlet_id", long_df[outlet_row_col])
                z.writestr(f"{folder}/long_format.csv", to_csv_bytes(long_df))

                z.writestr(
                    f"{folder}/INFO.txt",
                    f"Detected outlets as ROWS using column: {outlet_row_col}"
                )
                continue

            # ===========================
            # CASE D: No outlet detected
            # ===========================
            z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))
            z.writestr(
                f"{folder}/INFO.txt",
                "No outlet detected → exported combined.csv only."
            )

    st.success("Processed files successfully ✅")
    st.download_button(
        "⬇️ Download results (ZIP)",
        big_zip.getvalue(),
        file_name="outlet_outputs.zip",
        mime="application/zip"
    )
