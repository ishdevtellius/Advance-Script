"""
Google Sheets helpers: auth, read, update rows, hyperlink, parent-index parsing.
Ported from notebook's gspread utility cells.
"""
import os
from pathlib import Path
from typing import Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

import config

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gspread_client() -> gspread.Client:
    """Authenticate using a service-account JSON file (path from .env)."""
    creds_path = Path(config.GOOGLE_CREDENTIALS_PATH)
    if not creds_path.exists():
        creds_path = Path(__file__).resolve().parent / config.GOOGLE_CREDENTIALS_PATH
    creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
    return gspread.authorize(creds)


def open_sheet() -> tuple[gspread.Worksheet, pd.DataFrame]:
    """Open the configured spreadsheet/worksheet and return (worksheet, DataFrame)."""
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(config.SHEET_NAME)
    headers = worksheet.row_values(1)
    data = worksheet.get_all_records(expected_headers=headers)
    df = pd.DataFrame(data)
    return worksheet, df


def ensure_columns(df: pd.DataFrame, worksheet: gspread.Worksheet, extra_cols: list[str]):
    """Make sure extra columns exist in the DataFrame and update the sheet header row."""
    changed = False
    for col in extra_cols:
        if col not in df.columns:
            df[col] = ""
            changed = True
    if changed:
        worksheet.update("A1", [df.columns.tolist()])


def update_worksheet_row(row_updates: dict, row_index: int, df: pd.DataFrame, worksheet: gspread.Worksheet):
    """Write a dict of column→value updates to a specific row in the sheet."""
    for col in row_updates:
        if col not in df.columns:
            df[col] = ""

    for col, val in row_updates.items():
        df.at[row_index, col] = val

    row_data = df.iloc[row_index].values.tolist()
    row_range = f"A{row_index + 2}:{chr(65 + len(df.columns) - 1)}{row_index + 2}"
    worksheet.update(values=[row_data], range_name=row_range)

    _populate_tellius_hyperlink(df, worksheet, row_index)
    print(f"Updated Row {row_index + 2} in sheet")


def _populate_tellius_hyperlink(df: pd.DataFrame, worksheet: gspread.Worksheet, row_index: int):
    trace_id = df.iloc[row_index].get("Trace ID", "")
    if not trace_id:
        return
    last_col = df.shape[1]
    formula = f'=HYPERLINK("{config.BASE_URL}/kaiya/chat/conversation/{trace_id}", "Open in Tellius")'
    cell = gspread.utils.rowcol_to_a1(row_index + 2, last_col)
    worksheet.update(range_name=cell, values=[[formula]], raw=False)


def get_parent_idx(row) -> Optional[int]:
    """Convert 'Follow-up Of' sheet row number to DataFrame index."""
    followup_of = row.get("Follow-up Of", "")
    if followup_of and str(followup_of).strip():
        try:
            return int(float(str(followup_of).strip())) - 2
        except (ValueError, TypeError):
            print(f"WARNING: Invalid 'Follow-up Of' value: {followup_of}")
    return None
