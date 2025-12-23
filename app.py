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

    # Stronger column cleaning (handles weird invisible chars common in exports)
    df.columns = [
        re.sub(r"[\x00-\x1f\u2000-\u200f\u2028\u2029]+", "", str(c)).strip()
        for c in df.columns
    ]

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
    # UTF-8 with BOM so Google Sheets opens cleanly (Arabic too)
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
                if nun >= 2 and ratio < 0.7:
                    return c
    return None

def detect_outlet_row_column_smart(df: pd.DataFrame):
    col = detect_outlet_row_column(df)
    if col:
        return col

    if df is None or df.empty:
        return None

    bad_keywords = ["upc", "barcode", "gtin", "sku", "item", "product", "price", "qty",
                    "quantity", "stock", "name", "description", "plu"]
    best = None

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
        if ratio > 0.7:
            continue

        sample = series.astype(str).head(40)
        looks_id = sample.apply(lambda x: bool(re.fullmatch(r"\d{3,}", x.strip()))).mean()
        score = (1 - ratio) * 3 + looks_id * 5

        if best is None or score > best[0]:
            best = (score, c)

    return best[1] if best else None

def apply_combined_outlet_key_if_possible(df: pd.DataFrame):
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
        df2["_outlet_key"] = (
            df2[site_col].astype(str).fillna("UNKNOWN") + " - " +
            df2[outletid_col].astype(str).fillna("UNKNOWN")
        )
        return df2, "_outlet_key"

    return df, None

def read_text_table_with_fallback(file_like, sep: str) -> tuple[pd.DataFrame, str]:
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None

    for enc in encodings_to_try:
        try:
            file_like.seek(0)
            df = pd.read_csv(file_like, sep=sep, dtype=object, encoding=enc)
            return df, enc
        except Exception as e:
            last_err = e

    raise last_err

# ---------------- Excel header-row auto detection ----------------

HEADER_TOKENS = [
    "upc", "barcode", "gtin", "sku",
    "category", "sub_category", "subcategory",
    "item", "product", "name", "description",
    "price", "rsp", "stock", "qty", "quantity",
    "outlet", "store", "branch", "site", "plu"
]

def detect_header_row_from_preview(preview_df: pd.DataFrame, max_rows: int = 30) -> int:
    best_row = 0
    best_hits = -1

    rows_to_check = min(max_rows, len(preview_df))
    for r in range(rows_to_check):
        row = preview_df.iloc[r].tolist()
        cells = [str(x).strip().lower() for x in row if pd.notna(x)]
        hits = sum(1 for c in cells if any(t in c for t in HEADER_TOKENS))
        if hits > best_hits:
            best_hits = hits
            best_row = r

    return best_row

def read_excel_sheet_smart_from_bytes(excel_bytes: io.BytesIO, sheet_name: str, engine: str):
    excel_bytes.seek(0)
    preview = pd.read_excel(excel_bytes, sheet_name=sheet_name, header=None, nrows=40, dtype=object, engine=engine)
    header_row = detect_header_row_from_preview(preview, max_rows=30)

    excel_bytes.seek(0)
    df = pd.read_excel(excel_bytes, sheet_name=sheet_name, header=header_row, dtype=object, engine=engine)
    df = clean_df(df)
    return df, header_row

# ✅ detect if file is likely UTF-16 text pretending to be XLS
def looks_like_utf16_text(sample: bytes) -> bool:
    if sample.startswith(b"\xff\xfe") or sample.startswith(b"\xfe\xff"):
        return True
    if b"\x00" in sample:
        return True
    return False

# ✅ detect if file is HTML/XML pretending to be XLS
def looks_like_html_or_xml(sample: bytes) -> bool:
    s = sample.lstrip()
    return s.startswith(b"<") and (b"<table" in s.lower() or b"<?xml" in s.lower() or b"<html" in s.lower())

def try_read_html_tables(excel_bytes: io.BytesIO):
    decodings = ["utf-8", "utf-8-sig", "cp1252", "latin1", "utf-16", "utf-16-le", "utf-16-be"]
    last_err = None
    for enc in decodings:
        try:
            excel_bytes.seek(0)
            text = excel_bytes.read().decode(enc, errors="ignore")
            tables = pd.read_html(text)
            if tables:
                df = clean_df(tables[0].astype(object))
                return df, enc
        except Exception as e:
            last_err = e
    if last_err:
        raise last_err
    raise ValueError("No HTML tables detected")

def try_read_fake_table(excel_bytes: io.BytesIO):
    """
    Robust parser for files pretending to be .XLS but actually text exports.
    Handles:
    - UTF-16/UTF-16LE/UTF-16BE with/without BOM
    - broken/odd-length UTF-16 (common!)
    - TSV/CSV/; /| delimiters
    """
    seps = ["\t", ",", ";", "|"]
    encs = ["utf-16", "utf-16-le", "utf-16-be", "utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None

    # 1) Try normal pandas read_csv on bytes first (fast path)
    for sep in seps:
        for enc in encs:
            try:
                excel_bytes.seek(0)
                df = pd.read_csv(excel_bytes, sep=sep, dtype=object, encoding=enc)
                df = clean_df(df)
                if df is not None and not df.empty and len(df.columns) > 1:
                    return df, sep, enc
            except Exception as e:
                last_err = e

    # 2) ✅ BOM-less / broken UTF-16 fallback (your exact file type)
    try:
        excel_bytes.seek(0)
        raw = excel_bytes.read()

        for enc in ["utf-16-le", "utf-16-be", "utf-16"]:
            text = raw.decode(enc, errors="ignore")
            if ("\t" in text) or ("," in text) or ("\n" in text):
                for sep in seps:
                    try:
                        df = pd.read_csv(io.StringIO(text), sep=sep, dtype=object)
                        df = clean_df(df)
                        if df is not None and not df.empty and len(df.columns) > 1:
                            return df, sep, f"{enc} (decoded ignore)"
                    except Exception:
                        pass

    except Exception as e:
        last_err = e

    if last_err:
        raise last_err
    raise ValueError("Could not parse as a delimited table")

def read_any_file(uploaded):
    name = uploaded.name.lower()

    # ---------------- Excel branch (xlsx/xls) ----------------
    if name.endswith(("xlsx", "xls")):
        excel_bytes = io.BytesIO(uploaded.getvalue())

        # 1) Try real Excel engines first
        engines_to_try = ["openpyxl", "xlrd"]
        last_err = None

        for engine in engines_to_try:
            try:
                excel_bytes.seek(0)
                xls = pd.ExcelFile(excel_bytes, engine=engine)

                cleaned = {}
                header_rows = {}

                for sh in xls.sheet_names:
                    df_sh, header_row = read_excel_sheet_smart_from_bytes(excel_bytes, sh, engine=engine)
                    if df_sh is None or df_sh.empty:
                        continue
                    cleaned[str(sh).strip()] = df_sh
                    header_rows[str(sh).strip()] = header_row

                return {"type": "excel", "sheets": cleaned, "header_rows": header_rows, "excel_engine": engine}

            except Exception as e:
                last_err = e

        # 2) Fallbacks for "Excel-looking" exports
        try:
            excel_bytes.seek(0)
            sample = excel_bytes.read(4000)

            # HTML/XML saved as .xls
            if looks_like_html_or_xml(sample):
                excel_bytes.seek(0)
                df, used_enc = try_read_html_tables(excel_bytes)
                return {"type": "table", "df": df, "encoding": used_enc, "note": "XLS was actually HTML/XML"}

            # UTF-16/other delimited text pretending to be XLS (your case)
            if looks_like_utf16_text(sample):
                excel_bytes.seek(0)
                df, used_sep, used_enc = try_read_fake_table(excel_bytes)
                return {"type": "table", "df": df, "encoding": used_enc, "note": f"XLS was actually text (sep={repr(used_sep)})"}

            # still try delimited text even if not obviously UTF-16
            excel_bytes.seek(0)
            df, used_sep, used_enc = try_read_fake_table(excel_bytes)
            return {"type": "table", "df": df, "encoding": used_enc, "note": f"XLS was actually text (sep={repr(used_sep)})"}

        except Exception:
            pass

        # If everything failed, raise the original Excel error
        raise last_err

    # ---------------- JSON branch ----------------
    if name.endswith("json"):
        uploaded.seek(0)
        df = pd.read_json(uploaded)
        df = clean_df(df)
        return {"type": "table", "df": df}

    # ---------------- CSV/TSV/TXT branch ----------------
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

# ✅ NEW: modes/options
mode = st.radio(
    "What do you want to do?",
    ["Auto split + convert (existing behavior)", "Convert only (no splitting)"],
    index=0
)

keep_outlet_id_only_in_filename = st.checkbox(
    "When splitting, keep outlet id ONLY in the filename (don't add outlet_id column inside CSV)",
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
                z.writestr(f"{folder}/ERROR.txt", f"Failed to read file: {uploaded.name}\n\n{repr(e)}")
                continue

            # record encoding used (csv/tsv/txt OR fake-xls-text)
            if result.get("encoding"):
                z.writestr(f"{folder}/INFO_encoding.txt", f"Read using encoding: {result['encoding']}")

            if result.get("note"):
                z.writestr(f"{folder}/INFO_note.txt", result["note"])

            # record excel header row detection + engine
            if result.get("type") == "excel":
                if result.get("excel_engine"):
                    z.writestr(f"{folder}/INFO_excel_engine.txt", f"Excel engine used: {result['excel_engine']}")
                if result.get("header_rows"):
                    rows_info = "\n".join([f"{sh}: header_row={hr}" for sh, hr in result["header_rows"].items()])
                    z.writestr(f"{folder}/INFO_excel_header_rows.txt", rows_info)

            # ===========================
            # MODE: Convert ONLY (no splitting)
            # ===========================
            if mode == "Convert only (no splitting)":
                if result["type"] == "excel":
                    sheets = result["sheets"]
                    if not sheets:
                        z.writestr(f"{folder}/ERROR.txt", "No readable data found in this Excel file.")
                        continue

                    # Export every sheet to CSV (no outlet_id insert)
                    for sh, df_sh in sheets.items():
                        z.writestr(f"{folder}/{safe_name(sh)}.csv", to_csv_bytes(df_sh))

                    # Also export a combined.csv if multiple sheets (stacked with helper col)
                    if len(sheets) > 1:
                        combined_frames = []
                        for sh, df_sh in sheets.items():
                            out = df_sh.copy()
                            out.insert(0, "_sheet", sh)
                            combined_frames.append(out)
                        combined_df = pd.concat(combined_frames, ignore_index=True)
                        z.writestr(f"{folder}/combined.csv", to_csv_bytes(combined_df))
                else:
                    df = result["df"]
                    if df is None or df.empty:
                        z.writestr(f"{folder}/ERROR.txt", "No readable rows found.")
                        continue
                    z.writestr(f"{folder}/combined.csv", to_csv_bytes(df))

                z.writestr(f"{folder}/INFO.txt", "Convert-only mode → no splitting performed.")
                continue

            # ===========================
            # AUTO SPLIT (existing behavior + outlet_id-in-filename option)
            # ===========================

            # CASE A: Excel with MULTIPLE SHEETS (each sheet = outlet)
            if result["type"] == "excel":
                sheets = result["sheets"]

                if len(sheets) == 0:
                    z.writestr(f"{folder}/ERROR.txt", "No readable data found in this Excel file.")
                    continue

                if len(sheets) > 1:
                    combined_frames = []
                    for sh, df_sh in sheets.items():
                        out = df_sh.copy()

                        # Only add outlet_id column if user wants it inside
                        if not keep_outlet_id_only_in_filename:
                            out.insert(0, "outlet_id", sh)

                        # Keep a helper _sheet column for traceability
                        out.insert(0 if keep_outlet_id_only_in_filename else 1, "_sheet", sh)

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

            # CASE B: Outlet as COLUMNS (numeric outlet ids as headers)
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
                    out_df = out_df.rename(columns={oc: "outlet_value"})

                    if not keep_outlet_id_only_in_filename:
                        out_df.insert(0, "outlet_id", oc)

                    z.writestr(f"{folder}/outlet_{safe_name(oc)}.csv", to_csv_bytes(out_df))

                z.writestr(
                    f"{folder}/INFO.txt",
                    "Detected outlets as COLUMNS (numeric outlet ids in headers)."
                )
                continue

            # CASE C: Outlet as ROWS (smart detection)
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

                    if not keep_outlet_id_only_in_filename:
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

            # CASE D: No outlet detected
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
