"""
LLM-assisted data comparison: compares expected vs actual DataFrames
using fuzzy column mapping and value-set validation.
Ported from notebook's compare_tables_based_on_query_with_llm.
"""
import json
import time
import pandas as pd
import numpy as np
from openai import OpenAI

import config

_client = OpenAI(api_key=config.OPENAI_API_KEY)


def compare_tables_based_on_query_with_llm(
    expected_df: pd.DataFrame,
    actual_df: pd.DataFrame,
    user_query: str,
    row_index: int = -1,
) -> dict:
    label = f"Query Row {row_index + 2}" if row_index >= 0 else "Query"

    # --- Step 1: LLM identifies important columns + fuzzy mapping ---
    system_message = "You are a data alignment assistant. Return only valid JSON."
    column_prompt = f"""
User query: "{user_query}"

Expected dataframe columns: {list(expected_df.columns)}
Actual dataframe columns: {list(actual_df.columns)}

Tasks:
1. Identify the **important columns** from expected_df needed to answer the query.
2. Build a fuzzy mapping of actual_df column names → expected_df column names:
   - Handle case-insensitive matches, underscores vs spaces, abbreviations, synonyms, plural/singular.
   - If a required expected column is missing in actual_df, map it to "MISSING".
   - Ignore irrelevant extra actual_df columns.

Return JSON in this format only:
{{
  "important_columns": ["Column1", "Column2"],
  "mapping": {{"actual_col_name": "expected_col_name"}}
}}
"""
    start = time.time()
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": column_prompt},
            ],
            response_format={"type": "json_object"},
        )
        col_info = json.loads(resp.choices[0].message.content)
        important_columns = col_info.get("important_columns", [])
        mapping = col_info.get("mapping", {})
        print(f"{label}: Column mapping took {time.time() - start:.2f}s → {len(important_columns)} cols, {len(mapping)} mapped")

        if not important_columns:
            return _fail("COLUMN_MAPPING_ERROR", "LLM did not select any important columns")
    except Exception as e:
        return _fail("LLM_COLUMN_MAPPING_ERROR", f"Error during column mapping: {e}")

    # --- Step 2: Apply mapping ---
    mapped_cols = {a: e for a, e in mapping.items() if e != "MISSING"}
    actual_aligned = actual_df.rename(columns=mapped_cols)
    for col in important_columns:
        if col not in actual_aligned.columns:
            actual_aligned[col] = None

    expected_filtered = expected_df[important_columns].copy() if not expected_df.empty else pd.DataFrame(columns=important_columns)
    actual_filtered = actual_aligned[important_columns].copy() if not actual_aligned.empty else pd.DataFrame(columns=important_columns)

    # --- Step 3: Normalize values ---
    def normalize(val):
        if pd.isna(val):
            return None
        if isinstance(val, str):
            return val.strip().lower()
        if isinstance(val, (float, int, np.number)):
            return round(float(val), 2)
        return val

    for col in important_columns:
        expected_filtered[col] = expected_filtered[col].map(normalize)
        actual_filtered[col] = actual_filtered[col].map(normalize)

    expected_values = {col: expected_filtered[col].dropna().values.tolist() for col in important_columns}
    actual_values = {col: actual_filtered[col].dropna().values.tolist() for col in important_columns}

    # --- Step 4: Empty data checks ---
    if expected_filtered.empty and actual_filtered.empty:
        return _pass("Both datasets are empty for the important columns.", important_columns, mapping, {}, {}, {})
    if expected_filtered.empty and not actual_filtered.empty:
        return _fail("NO_DATA_MISMATCH", "Expected empty, actual has data.", important_columns, mapping, {}, actual_values, {})
    if not expected_filtered.empty and actual_filtered.empty:
        return _fail("NO_DATA_MISMATCH", "Actual empty, expected has data.", important_columns, mapping, expected_values, {}, {})

    # --- Step 5: Value-set comparison ---
    value_mismatches = {}
    for col in important_columns:
        expected_set = set(expected_filtered[col].dropna())
        actual_set = set(actual_filtered[col].dropna())
        missing = list(expected_set - actual_set)
        extra = list(actual_set - expected_set)
        if missing or extra:
            value_mismatches[col] = {"missing_in_actual": missing, "extra_in_actual": extra}

    if value_mismatches:
        return _fail("LOGIC_ERROR", "Discrepancies found.", important_columns, mapping, expected_values, actual_values, value_mismatches)

    return _pass("Datasets match logically.", important_columns, mapping, expected_values, actual_values, {})


def _pass(desc, cols=None, mapping=None, exp=None, act=None, mis=None):
    return {"status": "PASS", "failure_category": "NONE", "description": desc,
            "important_columns": cols or [], "column_mapping": mapping or {},
            "expected_values": exp or {}, "actual_values": act or {}, "value_mismatches": mis or {}}


def _fail(cat, desc, cols=None, mapping=None, exp=None, act=None, mis=None):
    return {"status": "FAIL", "failure_category": cat, "description": desc,
            "important_columns": cols or [], "column_mapping": mapping or {},
            "expected_values": exp or {}, "actual_values": act or {}, "value_mismatches": mis or {}}
