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

# ✅ FIXED: prioritize Outlet ID over Site no + still supports site/store/branch/outlet
def detect_outlet_row_column(df: pd.DataFrame):
    """
    Detect outlet column using priority patterns (Outlet ID first).
    """
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

    cols = list(df.columns)
    for pat in priority_patterns:
        for c in cols:
            lc = str(c).lower().strip()
            if re.search(pat, lc):
                series = df[c].dropna()
                if series.empty:
                    continue
                nun = series.nunique()
                ratio = nun / max(1, len(series))
                # outlet identifier should repeat across rows
                if nun >= 2 and ratio < 0.7:
                    return c
    return None

# ✅ NEW: smart fallback if headers are messy / shifted
def detect_outlet_row_column_smart(df: pd.DataFrame):
    """
    Smart detection even if headers are weird or outlet column shifts A/B/C.
    Priority:
      1) header-based detection
      2) behavior-based scan (repeating ID-like values)
    """
    col = detect_outlet_row_column(df)
    if col:
        return col

    if df is None or df.empty:
        return None

    bad_keywords = ["upc", "barcode", "gtin", "sku", "item", "product", "price", "qty", "quantity", "stock", "name", "description", "plu"]
    best = None  # (score, colname)

    # scan first 20 columns (outlet identifiers are usually early)
    for c in df.columns[:20]:
        lc = str(c).lower().strip()

        if any(k in lc for k in bad_keywords):
            continue

        series = df[c].dropna()
        if series.empty:
            continue

        nun = series.nunique()
        total = len(series)
        ratio = nun / max(1, total)

        if nun < 2:
            continue
        if ratio > 0.7:  # too unique, likely item/sku/name
            continue

        sample = series.astype(str).head(40)

        # % that look like numeric IDs (3+ digits)
        looks_id = sample.apply(lambda x: bool(re.fullmatch(r"\d{3,}", x.strip()))).mean()

        # score: prefer repeating + ID-looking
        score = (1 - ratio) * 3 + looks_id * 5

        if best is None or score > best[0]:
            best = (score, c)

    return best[1] if best else None

# ✅ NEW: if both Site no + Outlet ID exist, combine them to avoid collisions
def apply_combined_outlet_key_if_possible(df: pd.DataFrame):
    """
    If both 'Site no' and 'Outlet ID' exist, create _outlet_key = 'Site no - Outlet ID'
    and return (df, '_outlet_key'). Otherwise return (df, None).
    """
    def norm(s): return str(s).lower().strip()

    site_col = None
    outletid_col = None

    for c in df.columns:
        lc = norm(c)
        if site_col is None and re.search(r"\bsite\s*no\b", lc):
            site_col = c
        if outletid_col is None and re.search(r"\boutlet\s*id\b", lc):
            outletid_col = c

    if site_col and outletid_col:
        df2 = df.copy()
        df2["_outlet_key"] = df2[site_col].astype(str).fillna("UNKNOWN") + " - " + df2[outletid_col].astype(str).fillna("UNKNOWN")
        return df2, "_outlet_key"

    return df, None

# ✅ robust reader for CSV/TSV/TXT (fixes UnicodeDecodeError)
def read_text_table_with_fallback(uploaded, sep: str) -> tuple[pd.DataFrame, str]:
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None

    for enc in encodings_to_try:
        try:
            uploaded.seek(0)
            df = pd.read_csv(uploaded, sep=sep, dtype=object, encoding=enc)
            return df, enc
        except Exception as e:
            last_err = e

    raise last_err

def read_any_file(uploaded):
    name = uploaded.name.lower()

    if name.endswith(("xlsx", "xls")):
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

            # record encoding used (only for csv/tsv/txt)
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
                    z.writestr(f"{folder}/long_format.csv", to_csv_bytes(combined_df))

                    z.writestr(
                        f"{folder}/INFO.txt",
                        "Detected multiple sheets → treated each sheet as an outlet."
                    )
                    continue

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

                z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))

                long_df = df.melt(
                    id_vars=base_cols,
                    value_vars=outlet_cols,
                    var_name="outlet_id",
                    value_name="outlet_value"
                )
                z.writestr(f"{folder}/long_format.csv", to_csv_bytes(long_df))

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
            # CASE C: Outlet as ROWS (smart detection)
            # ===========================
            # Prefer a combined key if both exist (Site no + Outlet ID)
            df2, combined_key = apply_combined_outlet_key_if_possible(df)
            if combined_key:
                outlet_row_col = combined_key
                df = df2
                z.writestr(f"{folder}/INFO_outlet_key.txt", "Using combined outlet key: Site no - Outlet ID")
            else:
                outlet_row_col = detect_outlet_row_column_smart(df)

            if outlet_row_col:
                z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))
                z.writestr(f"{folder}/INFO_outlet_column.txt", f"Outlet column detected: {outlet_row_col}")

                for outlet, grp in df.groupby(outlet_row_col, dropna=False):
                    grp = grp.copy()
                    grp.insert(0, "outlet_id", outlet)
                    z.writestr(f"{folder}/outlet_{safe_name(outlet)}.csv", to_csv_bytes(grp))

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
