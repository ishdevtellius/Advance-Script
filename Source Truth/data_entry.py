"""
Data-Population flow (two-phase).
Phase 1: Run all parent (non-follow-up) queries → get golden SQL from Kaiya.
Phase 2: Run all follow-up queries in their parent's conversation_id.

Ported from notebook cells: "Data-Population Entry Point" and
"Data-Population Entry Point Followup".
"""
import time
import concurrent.futures

import pandas as pd
from gspread import Worksheet

import config
from kaiya_client import get_kaiya_response
from evaluator import compare_tables_based_on_query_with_llm
from llm_summary import generate_sql_comparison_summary
from sheet_utils import (
    ensure_columns,
    update_worksheet_row,
    get_parent_idx,
)

EXTRA_COLS = ["Trace ID", "Tellius Link", "Received SQL", "Received Chart Title"]

# conversation_id map shared across phases: df_index → conversation_id
_conversation_map: dict[int, str] = {}


def run(df: pd.DataFrame, worksheet: Worksheet, retry: int = 1, followup_only: bool = False):
    """
    Main entry point.
    - followup_only=False  → runs Phase 1 (parents) then Phase 2 (follow-ups).
    - followup_only=True   → runs Phase 2 only (follow-ups).
    """
    ensure_columns(df, worksheet, EXTRA_COLS)

    parent_rows = []
    followup_rows = []

    for idx, row in df.iterrows():
        if get_parent_idx(row) is not None:
            followup_rows.append((idx, row))
        else:
            parent_rows.append((idx, row))

    print(f"\n{'='*60}")
    print(f"  DATA-POPULATION: {len(parent_rows)} parents, {len(followup_rows)} follow-ups")
    print(f"  Retry attempts: {retry}  |  Workers: {config.MAX_WORKERS}")
    print(f"{'='*60}\n")

    if not followup_only:
        _run_phase(parent_rows, df, worksheet, retry, "Phase 1 (Parents)")

    _run_phase(followup_rows, df, worksheet, retry, "Phase 2 (Follow-ups)")


def _run_phase(rows, df, worksheet, retry, phase_name):
    if not rows:
        print(f"[{phase_name}] No rows to process.\n")
        return

    print(f"\n--- {phase_name}: {len(rows)} rows ---")

    if config.SEQUENTIAL_EXECUTION:
        for idx, row in rows:
            result = _process_single(idx, row, df, worksheet, retry, phase_name)
            if result:
                update_worksheet_row(result[1], result[0], df, worksheet)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as pool:
            futures = {
                pool.submit(_process_single, idx, row, df, worksheet, retry, phase_name): idx
                for idx, row in rows
            }
            for future in concurrent.futures.as_completed(futures):
                fidx = futures[future]
                try:
                    result = future.result()
                    if result:
                        update_worksheet_row(result[1], result[0], df, worksheet)
                except Exception as e:
                    print(f"[{phase_name}] Row {fidx + 2} exception: {e}")
                    update_worksheet_row(
                        {"Status": "FAIL", "Failure Description": str(e), "Failure Category": "GENERIC_ERROR"},
                        fidx, df, worksheet,
                    )


def _process_single(idx, row, df, worksheet, retry, phase):
    t0 = time.time()
    api_times, llm_times = [], []

    if row.get("Ignore") == "Yes":
        print(f"[{phase}] Skipping Row {idx + 2} (ignored)")
        return None
    if row.get("Correct SQL"):
        print(f"[{phase}] Skipping Row {idx + 2} (already has Correct SQL)")
        return None

    kaiya_query = row.get("Kaiya Query")
    bv_id = config.resolve_bv_id(row.get("Business View", ""))

    updates = {
        "Failure Description": "", "Failure Category": "",
        "Correct SQL": "", "Status": "", "Trace ID": "", "Response Time (in s)": "",
    }

    if not kaiya_query or not bv_id:
        updates["Failure Description"] = "Missing Kaiya Query or Business View"
        updates["Failure Category"] = "INPUT_ERROR"
        return idx, updates

    # Resolve parent conversation_id for follow-ups
    parent_idx = get_parent_idx(row)
    conv_id = _conversation_map.get(parent_idx) if parent_idx is not None else None
    if conv_id is None and parent_idx is not None:
        sheet_trace = str(df.iloc[parent_idx].get("Trace ID", "") or "").strip()
        if sheet_trace:
            conv_id = sheet_trace

    if conv_id:
        print(f"[{phase}] Row {idx + 2}: FOLLOW-UP of Row {parent_idx + 2} (conv: {conv_id[:8]}...) — {kaiya_query}")
    elif parent_idx is not None:
        print(f"[{phase}] Row {idx + 2}: WARNING follow-up but no parent conv_id — new conversation — {kaiya_query}")
    else:
        print(f"[{phase}] Row {idx + 2}: NEW CONVERSATION — {kaiya_query}")

    try:
        response_data, response_sql = [], []
        for attempt in range(retry):
            t1 = time.time()
            result, error = get_kaiya_response(bv_id, kaiya_query, conversation_id=conv_id)
            api_times.append(time.time() - t1)

            sql = result.get("sql")
            chart_title = result.get("chart_title")
            returned_conv = result.get("conversation_id")

            updates["Trace ID"] = returned_conv
            updates["Received Chart Title"] = chart_title
            updates["Received SQL"] = sql if attempt == 0 else f'{updates["Received SQL"]}\n\n{sql}'

            _conversation_map[idx] = returned_conv

            if error or result.get("data") is None or sql is None:
                print(f"[{phase}] Row {idx + 2}: Kaiya error — {error}")
                updates["Failure Description"] = error
                updates["Failure Category"] = "KAIYA_API_ERROR"
                updates["Status"] = "FAIL"
                break

            response_data.append(result.get("data"))
            response_sql.append(sql)

            if attempt > 0:
                t2 = time.time()
                cmp = compare_tables_based_on_query_with_llm(response_data[attempt - 1], response_data[attempt], kaiya_query)
                llm_times.append(time.time() - t2)
                if cmp["status"] != "PASS":
                    t3 = time.time()
                    summary = generate_sql_comparison_summary(kaiya_query, response_sql[attempt - 1], response_sql[attempt], cmp.get("value_mismatches"))
                    llm_times.append(time.time() - t3)
                    updates["Status"] = "FAIL"
                    updates["Failure Category"] = cmp.get("failure_category")
                    updates["Failure Description"] = str(summary)
                    break
        else:
            updates["Correct SQL"] = response_sql[-1]
            updates["Status"] = "PASS"
            print(f"[{phase}] Row {idx + 2}: Golden SQL found")

    except Exception as e:
        updates["Failure Description"] = str(e)
        updates["Failure Category"] = "GENERIC_ERROR"

    total = time.time() - t0
    updates["Response Time (in s)"] = f"Total: {total:.2f}s | API: {sum(api_times):.2f}s | LLM: {sum(llm_times):.2f}s"
    print(f"[{phase}] Row {idx + 2}: Done in {total:.2f}s")
    return idx, updates
