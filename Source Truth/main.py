#!/usr/bin/env python3
"""
Kaiya Text2SQL Evaluation — Source Truth CLI

Usage:
    python main.py data_entry                  # Phase 1 + Phase 2 (parents → follow-ups)
    python main.py data_entry --followup       # Phase 2 only (follow-ups only)
    python main.py query_evaluation            # Phase 1 + Phase 2 (parents → follow-ups)
    python main.py query_evaluation --followup # Phase 2 only (follow-ups only)

Options:
    --retry N   Number of retry attempts per query (default: 1)
"""
import argparse
import sys
import time

import sheet_utils
import data_entry
import query_evaluation


def main():
    parser = argparse.ArgumentParser(
        description="Kaiya Text2SQL Evaluation — Source Truth CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py data_entry                   Populate golden SQL for all queries
  python main.py data_entry --followup        Populate golden SQL for follow-ups only
  python main.py query_evaluation             Evaluate all queries against golden SQL
  python main.py query_evaluation --followup  Evaluate follow-ups only
        """,
    )

    parser.add_argument(
        "mode",
        choices=["data_entry", "query_evaluation"],
        help="Execution mode: 'data_entry' populates golden SQL, 'query_evaluation' evaluates results",
    )
    parser.add_argument(
        "--followup",
        action="store_true",
        default=False,
        help="Run only the follow-up phase (Phase 2). Parents must already be complete.",
    )
    parser.add_argument(
        "--retry",
        type=int,
        default=1,
        help="Number of retry attempts per query (default: 1)",
    )

    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"  Kaiya Source Truth CLI")
    print(f"  Mode         : {args.mode}")
    print(f"  Follow-up    : {'only' if args.followup else 'included (after parents)'}")
    print(f"  Retry        : {args.retry}")
    print(f"{'#'*60}\n")

    print("Loading Google Sheet...")
    t0 = time.time()
    worksheet, df = sheet_utils.open_sheet()
    print(f"Loaded {len(df)} rows in {time.time() - t0:.2f}s\n")

    if args.mode == "data_entry":
        data_entry.run(df, worksheet, retry=args.retry, followup_only=args.followup)
    elif args.mode == "query_evaluation":
        query_evaluation.run(df, worksheet, retry=args.retry, followup_only=args.followup)

    print(f"\n{'#'*60}")
    print(f"  DONE — Total elapsed: {time.time() - t0:.2f}s")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    main()
