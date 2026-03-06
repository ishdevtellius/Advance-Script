"""
Query Evaluation flow (two-phase).
Phase 1: Run all parent queries → compare Kaiya SQL result vs golden SQL result.
Phase 2: Run all follow-up queries in parent's conversation_id → same comparison.

Ported from notebook cells: "Query Evaluation Entry Point" and
"Query Evaluation Entry Point Followup".
"""
import json
import time
import concurrent.futures

import pandas as pd
from gspread import Worksheet

import config
from kaiya_client import get_kaiya_response, get_sql_query_response
from evaluator import compare_tables_based_on_query_with_llm
from llm_summary import generate_sql_comparison_summary
from sheet_utils import (
    ensure_columns,
    update_worksheet_row,
    get_parent_idx,
)

EXTRA_COLS = [
    "Received SQL", "Received Chart Title", "Status", "Failure Description",
    "Failure Category", "Trace ID", "Response Time (in s)",
    "Has Reflection", "Reflection feedback", "Reflection Action",
    "time_to_final_s", "time_to_intermediate_s", "Path/Mode",
]

_conversation_map: dict[int, str] = {}


def run(df: pd.DataFrame, worksheet: Worksheet, retry: int = 1, followup_only: bool = False):
    ensure_columns(df, worksheet, EXTRA_COLS)

    parent_rows = []
    followup_rows = []

    for idx, row in df.iterrows():
        if get_parent_idx(row) is not None:
            followup_rows.append((idx, row))
        else:
            parent_rows.append((idx, row))

    print(f"\n{'='*60}")
    print(f"  QUERY EVALUATION: {len(parent_rows)} parents, {len(followup_rows)} follow-ups")
    print(f"  Retry: {retry}  |  Workers: {config.MAX_WORKERS}")
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
    kaiya_api_time = 0
    sql_api_time = 0
    llm_col_time = 0
    llm_sum_time = 0

    correct_sql = row.get("Correct SQL")
    kaiya_query = row.get("Kaiya Query")
    bv_id = config.resolve_bv_id(row.get("Business View", ""))

    if not correct_sql or str(row.get("Ignore", "")).strip().lower() == "yes":
        reason = "ignored" if str(row.get("Ignore", "")).strip().lower() == "yes" else "no Correct SQL"
        print(f"[{phase}] Skipping Row {idx + 2} ({reason})")
        return None

    updates = {
        "Failure Description": "", "Failure Category": "",
        "Received Chart Title": "", "Received SQL": "",
        "Status": "", "Trace ID": "", "Response Time (in s)": "",
        "Has Reflection": "", "Reflection feedback": "", "Reflection Action": "",
        "time_to_final_s": "", "time_to_intermediate_s": "", "Path/Mode": "",
    }

    if not kaiya_query or not bv_id:
        updates["Failure Description"] = "Missing Kaiya Query or Business View"
        updates["Failure Category"] = "INPUT_ERROR"
        updates["Status"] = "FAIL"
        return idx, updates

    # Resolve parent conversation_id for follow-ups
    parent_idx = get_parent_idx(row)
    conv_id = _conversation_map.get(parent_idx) if parent_idx is not None else None
    if conv_id is None and parent_idx is not None:
        sheet_trace = str(df.iloc[parent_idx].get("Trace ID", "") or "").strip()
        if sheet_trace:
            conv_id = sheet_trace

    try:
        # 1. Execute golden SQL
        print(f"[{phase}] Row {idx + 2}: Executing golden SQL via API")
        t1 = time.time()
        df_sql, sql_error = get_sql_query_response(bv_id, correct_sql)
        sql_api_time = time.time() - t1

        if sql_error:
            updates["Status"] = "FAIL"
            updates["Failure Category"] = "SQL_API_ERROR"
            updates["Failure Description"] = sql_error
        else:
            # 2. Call Kaiya (with retries)
            all_sqls, all_responses = [], []
            evaluation_passed = False

            for attempt in range(retry):
                print(f"[{phase}] Row {idx + 2}: Kaiya attempt {attempt + 1}/{retry}")
                t2 = time.time()
                kaiya_resp, kaiya_err = get_kaiya_response(bv_id, kaiya_query, conversation_id=conv_id)
                kaiya_api_time += time.time() - t2

                returned_conv = kaiya_resp.get("conversation_id")
                _conversation_map[idx] = returned_conv

                if kaiya_err:
                    print(f"[{phase}] Row {idx + 2}: Kaiya error attempt {attempt + 1}: {kaiya_err}")
                    if attempt == retry - 1:
                        updates["Status"] = "FAIL"
                        updates["Failure Category"] = "KAIYA_API_ERROR"
                        updates["Failure Description"] = kaiya_err
                    continue

                kaiya_df = kaiya_resp.get("data")
                kaiya_sql = kaiya_resp.get("sql", "")

                all_sqls.append(kaiya_sql)
                all_responses.append(kaiya_df)

                updates["Trace ID"] = returned_conv
                updates["Received SQL"] = kaiya_sql
                updates["Received Chart Title"] = kaiya_resp.get("chart_title", "")

                # Populate tracing fields
                hr = kaiya_resp.get("has_reflection")
                if hr is not None:
                    updates["Has Reflection"] = str(hr)
                rf = kaiya_resp.get("reflection_feedback", "")
                if rf:
                    updates["Reflection feedback"] = rf
                ra = kaiya_resp.get("reflection_action", "")
                if ra:
                    updates["Reflection Action"] = ra
                ttf = kaiya_resp.get("time_to_final_s", "")
                if ttf:
                    updates["time_to_final_s"] = ttf
                tti = kaiya_resp.get("time_to_intermediate_s", "")
                if tti:
                    updates["time_to_intermediate_s"] = tti
                pm = kaiya_resp.get("path_mode", "")
                if pm:
                    updates["Path/Mode"] = pm

                # 3. Compare data
                t3 = time.time()
                comparison = compare_tables_based_on_query_with_llm(df_sql, kaiya_df, kaiya_query, idx)
                llm_col_time += time.time() - t3

                if comparison["status"] == "PASS":
                    updates["Status"] = "PASS"
                    updates["Failure Category"] = "NONE"
                    evaluation_passed = True
                    print(f"[{phase}] Row {idx + 2}: PASSED on attempt {attempt + 1}")
                    break
                else:
                    if attempt == retry - 1:
                        t4 = time.time()
                        summary = generate_sql_comparison_summary(
                            kaiya_query, correct_sql, kaiya_sql,
                            comparison.get("value_mismatches"), idx,
                        )
                        llm_sum_time = time.time() - t4
                        updates["Status"] = summary.get("status", "FAIL")
                        updates["Failure Category"] = summary.get("failure_category", "GENERIC_ERROR")
                        updates["Failure Description"] = json.dumps(summary)

            if len(all_sqls) > 1 and not evaluation_passed:
                updates["Received SQL"] = "\n\n--- ATTEMPT SEPARATOR ---\n\n".join(all_sqls)

    except Exception as e:
        updates["Status"] = "FAIL"
        updates["Failure Description"] = str(e)
        updates["Failure Category"] = "GENERIC_ERROR"

    total = time.time() - t0
    updates["Response Time (in s)"] = (
        f"Total: {total:.2f}s | SQL API: {sql_api_time:.2f}s | "
        f"Kaiya API: {kaiya_api_time:.2f}s | LLM Col: {llm_col_time:.2f}s | LLM Sum: {llm_sum_time:.2f}s"
    )
    print(f"[{phase}] Row {idx + 2}: Done in {total:.2f}s → {updates['Status']}")
    return idx, updates
