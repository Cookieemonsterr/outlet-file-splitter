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
    priority_patterns = [
        r"\boutlet\s*id\b",
        r"\bstore\s*id\b",
        r"\bbranch\s*id\b",
        r"\boutlet\b",
        r"\bbranch\b",
        r"\bstore\b",
        r"\bsite\s*no\b",
        r"\bsite\b",
        r"\blocation\b",
    ]
    for pat in priority_patterns:
        for c in df.columns:
            if re.search(pat, str(c).lower()):
                series = df[c].dropna()
                if not series.empty and series.nunique() / len(series) < 0.7:
                    return c
    return None

def detect_outlet_row_column_smart(df: pd.DataFrame):
    col = detect_outlet_row_column(df)
    if col:
        return col

    bad_keywords = ["upc","barcode","sku","item","price","qty","stock","name"]
    best = None

    for c in df.columns[:20]:
        if any(k in str(c).lower() for k in bad_keywords):
            continue
        series = df[c].dropna()
        if series.empty:
            continue
        ratio = series.nunique() / len(series)
        if ratio < 0.7:
            score = (1 - ratio)
            if best is None or score > best[0]:
                best = (score, c)

    return best[1] if best else None

def read_text_table_with_fallback(uploaded, sep: str):
    for enc in ["utf-8","utf-8-sig","cp1252","latin1","utf-16"]:
        try:
            uploaded.seek(0)
            df = pd.read_csv(uploaded, sep=sep, dtype=object, encoding=enc)
            return clean_df(df), enc
        except:
            pass
    raise ValueError("Could not decode file")

def looks_like_utf16_text(sample: bytes) -> bool:
    return b"\x00" in sample or sample.startswith((b"\xff\xfe", b"\xfe\xff"))

def read_any_file(uploaded):
    name = uploaded.name.lower()

    if name.endswith(("xlsx","xls")):
        data = uploaded.getvalue()
        excel_bytes = io.BytesIO(data)

        for engine in ["openpyxl","xlrd"]:
            try:
                excel_bytes.seek(0)
                xls = pd.ExcelFile(excel_bytes, engine=engine)
                sheets = {}
                for sh in xls.sheet_names:
                    excel_bytes.seek(0)
                    df = pd.read_excel(excel_bytes, sheet_name=sh, dtype=object, engine=engine)
                    df = clean_df(df)
                    if not df.empty:
                        sheets[sh] = df
                return {"type":"excel","sheets":sheets}
            except:
                pass

        # fake XLS → text
        sample = data[:2000]
        if looks_like_utf16_text(sample):
            for sep in ["\t",","]:
                for enc in ["utf-16","utf-16-le","utf-16-be"]:
                    try:
                        df = pd.read_csv(io.BytesIO(data), sep=sep, dtype=object, encoding=enc)
                        return {"type":"table","df":clean_df(df)}
                    except:
                        pass

        raise ValueError("Unreadable Excel")

    if name.endswith("json"):
        uploaded.seek(0)
        return {"type":"table","df":clean_df(pd.read_json(uploaded))}

    sep = "\t" if name.endswith("tsv") else ","
    df, _ = read_text_table_with_fallback(uploaded, sep)
    return {"type":"table","df":df}

# ---------------- UI ----------------

uploaded_files = st.file_uploader(
    "Upload file(s)",
    type=SUPPORTED_TYPES,
    accept_multiple_files=True
)

st.divider()

mode = st.radio(
    "Output mode",
    ["Auto (split when detected)", "Convert only (no splitting)"],
    index=0
)

keep_outlet_only_in_filename = st.checkbox(
    "When splitting: keep outlet ID only in the file name (do NOT add outlet_id column inside CSV)",
    value=True
)

if uploaded_files:
    big_zip = io.BytesIO()

    with zipfile.ZipFile(big_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for uploaded in uploaded_files:
            folder = safe_name(uploaded.name)

            try:
                result = read_any_file(uploaded)
            except Exception as e:
                z.writestr(f"{folder}/ERROR.txt", str(e))
                continue

            if result["type"] == "excel":
                sheets = result["sheets"]

                if mode == "Convert only (no splitting)":
                    df = list(sheets.values())[0]
                    z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))
                    continue

                if len(sheets) > 1:
                    for sh, df in sheets.items():
                        z.writestr(f"{folder}/outlet_{safe_name(sh)}.csv", to_csv_bytes(df))
                    continue

                df = list(sheets.values())[0]
            else:
                df = result["df"]

            if mode == "Convert only (no splitting)":
                z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))
                continue

            outlet_cols = detect_outlet_columns(df)
            if outlet_cols:
                base = [c for c in df.columns if c not in outlet_cols]
                for oc in outlet_cols:
                    out = df[base + [oc]].rename(columns={oc:"outlet_value"})
                    z.writestr(f"{folder}/outlet_{safe_name(oc)}.csv", to_csv_bytes(out))
                continue

            outlet_row_col = detect_outlet_row_column_smart(df)
            if outlet_row_col:
                for outlet, grp in df.groupby(outlet_row_col):
                    z.writestr(f"{folder}/outlet_{safe_name(outlet)}.csv", to_csv_bytes(grp))
                continue

            z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))

    st.success("Processed files successfully ✅")
    st.download_button(
        "⬇️ Download results (ZIP)",
        big_zip.getvalue(),
        file_name="outlet_outputs.zip",
        mime="application/zip"
    )
