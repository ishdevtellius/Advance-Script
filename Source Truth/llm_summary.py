"""
LLM-based SQL comparison summary.
Ported from notebook's generate_sql_comparison_summary.
"""
import json
import time
from typing import Optional
from openai import OpenAI

import config

_client = OpenAI(api_key=config.OPENAI_API_KEY)

SYSTEM_MESSAGE = """
You are an expert SQL evaluator. You will be given:
1. An analytical question in plain English
2. A benchmark SQL query (expected correct SQL)
3. A received SQL query (generated SQL)
4. A comparison JSON report with row counts, column counts, schemas, and mismatches

Your task:
- Analyze the two SQLs in context of the analytical question and the comparison JSON.
- Decide if the **received SQL** correctly answers the analytical question.

**Rules:**
1. Consider PASS if core values are present/correct, even with minor column name, order, rounding, or pivot differences.
2. If the benchmark is wrong but received is correct, set FAIL (benchmark wrong).
3. If the question is ambiguous and both are valid, set PASS with category AMBIGUOUS.
4. If received is incorrect, explain briefly why.

**Failure Categories (when status = FAIL):**
- LOGIC_ERROR, MISSING_DATA, SCHEMA_MISMATCH, AMBIGUOUS, GENERIC_ERROR, NO_DATA_MISMATCH

**Output (strict JSON):**
{
  "status": "PASS" or "FAIL",
  "failure_category": "LOGIC_ERROR | MISSING_DATA | SCHEMA_MISMATCH | AMBIGUOUS | GENERIC_ERROR | NONE | NO_DATA_MISMATCH",
  "description": "1-2 line explanation"
}
"""


def generate_sql_comparison_summary(
    analytical_query: str,
    expected_sql: str,
    actual_sql: str,
    comparison_result: dict,
    row_index: int = -1,
) -> Optional[dict]:
    label = f"Query Row {row_index + 2}" if row_index >= 0 else "Query"

    prompt = f"""
Analytical Question:
{analytical_query}

Benchmark SQL:
{expected_sql}

Received SQL:
{actual_sql}

Comparison JSON (summary of data differences):
{json.dumps(comparison_result, indent=2)}
"""
    start = time.time()
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": SYSTEM_MESSAGE},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        summary = json.loads(resp.choices[0].message.content)
        print(f"{label}: SQL summary LLM took {time.time() - start:.2f}s → {summary.get('status')}")
        return summary
    except Exception as e:
        print(f"{label}: SQL summary LLM error after {time.time() - start:.2f}s: {e}")
        return {"status": "ERROR", "failure_category": "GENERIC_ERROR", "description": str(e)}
