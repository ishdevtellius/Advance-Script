"""
Kaiya API client — call_api, get_kaiya_response, get_sql_query_response.
Ported from the Jupyter notebook's utility functions.
"""
import uuid
import requests
import pandas as pd
from typing import Optional, Tuple, Dict, Any

import config


def call_api(url: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict], Optional[str]]:
    headers = {
        "Authorization": config.AUTH_TOKEN,
        "Content-Type": "application/json",
        "USERID": config.USER_ID,
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=config.TIMEOUT)
        resp.raise_for_status()
        return resp.json(), None
    except Exception as e:
        return None, str(e)


def get_sql_query_response(bv_id: str, sql_query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Execute a SQL query against a Business View and return the result as a DataFrame."""
    try:
        sql_url = f"{config.BASE_URL}/api/tql/query/execute"
        payload = {"query": sql_query, "bvId": bv_id}
        data, error = call_api(sql_url, payload)
        if error:
            return None, error
        col_names = [col["name"] for col in data["columns"]]
        df = pd.DataFrame(data["rows"], columns=col_names)
        return df, None
    except Exception as e:
        return None, str(e)


def get_kaiya_response(
    bv_id: str,
    query: str,
    conversation_id: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Send a natural-language query to Kaiya and parse the full response.
    Uses @sql prefix to force the Text2SQL path.
    """
    kaiya_url = (
        f"{config.BASE_URL}/api/kaiya/conversation/process"
        "?async=false&srl_post_processing_clarification=false"
    )

    if conversation_id is None:
        conversation_id = str(uuid.uuid4())
    message_id = str(uuid.uuid4())

    result: Dict[str, Any] = {
        "data": None,
        "sql": None,
        "chart_title": "",
        "conversation_id": conversation_id,
        "has_reflection": None,
        "reflection_feedback": "",
        "reflection_action": "",
        "time_to_final_s": "",
        "time_to_intermediate_s": "",
        "path_mode": "",
    }

    try:
        payload = {
            "conversationId": conversation_id,
            "messageId": message_id,
            "businessViewId": bv_id,
            "query": f"@sql {query}",
            "userId": config.USER_ID,
        }
        data, error = call_api(kaiya_url, payload)
        if error or data is None:
            return result, error

        if data.get("status") != "success":
            return result, data.get("internalErrorMessage", data.get("msg", "Unknown error"))

        if not data.get("searchResponses"):
            return result, "No Data found in Kaiya Response"

        search_response = data["searchResponses"][0]
        precomputed = search_response.get("preComputedData", {})
        generated_sql = search_response.get("metadata", {}).get("generatedSql")
        chart_title = search_response.get("metadata", {}).get("chartTitle", "")

        result["sql"] = generated_sql
        result["chart_title"] = chart_title
        result["conversation_id"] = conversation_id

        # --- Tracing ---
        tracing = data.get("tracing", []) or search_response.get("tracing", [])

        reflection_retry_list = []
        reflection_action_list = []
        reflection_issues_list = []

        for entry in tracing:
            trace_type = entry.get("trace_type", "")
            node_output = entry.get("node_output", {})

            if trace_type == "text2sql_reflection":
                retry_value = node_output.get("retry")
                if retry_value is not None:
                    reflection_retry_list.append(str(retry_value))
                for issue in node_output.get("issues", []):
                    if isinstance(issue, dict):
                        fb = issue.get("feedback", "")
                        cat = issue.get("category", "")
                        reflection_issues_list.append(f"{cat}: {fb}" if cat else fb)
                    elif isinstance(issue, str):
                        reflection_issues_list.append(issue)
                action = node_output.get("action", "")
                if action:
                    reflection_action_list.append(action)

            elif trace_type == "execution_duration_log":
                ttf = node_output.get("time_to_final_s", "")
                if ttf:
                    result["time_to_final_s"] = str(ttf)
                tti = node_output.get("time_to_intermediate_s", "")
                if tti:
                    result["time_to_intermediate_s"] = str(tti)

            elif trace_type == "flow_selector_for_sql_generation":
                mode = node_output.get("mode", "")
                if mode:
                    result["path_mode"] = mode

        if reflection_retry_list:
            result["has_reflection"] = "\n".join(reflection_retry_list)
        if reflection_issues_list:
            result["reflection_feedback"] = "\n".join(reflection_issues_list)
        if reflection_action_list:
            result["reflection_action"] = "\n".join(reflection_action_list)

        if precomputed:
            col_names = [col["name"] for col in precomputed["columns"]]
            result["data"] = pd.DataFrame(precomputed["rows"], columns=col_names)
            return result, None
        elif generated_sql:
            result["data"] = pd.DataFrame()
            return result, None
        else:
            return result, "No Data or SQL found in Kaiya Response"

    except Exception as e:
        return result, str(e)
