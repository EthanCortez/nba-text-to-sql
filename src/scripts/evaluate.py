"""
evaluate.py

Utility functions for SQL execution, correctness checking,
edit distance, error categorization, and distribution plotting.
Used by pipeline.py during evaluation.

Author: Sabrina Park
"""

import re
from typing import Any, List, Tuple, Optional


# -------------------------------------------------
# 1. Run SQL safely (Postgres / psycopg2)
# -------------------------------------------------
def execute_sql(conn, sql: str):
    """
    Runs SQL and returns:
        (success: bool, rows: list | None, error_message: str)

    - Works for SELECT (fetches rows)
    - Works for non-SELECT (returns [])
    - Rolls back on failure, commits on success
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)

            # If this statement returns rows (e.g., SELECT), cur.description is not None
            if cur.description is not None:
                rows = cur.fetchall()
            else:
                rows = []

        conn.commit()
        return True, rows, ""

    except Exception as e:
        # Important: keep connection usable after a failed query
        try:
            conn.rollback()
        except Exception:
            pass
        return False, None, str(e)


# -------------------------------------------------
# 1.5 Normalize rows (optional but recommended)
# -------------------------------------------------
def normalize_rows(rows: List[Tuple[Any, ...]]) -> List[Tuple[str, ...]]:
    """
    Normalize result sets so execution accuracy is stable:
      - convert values to strings
      - strip whitespace
      - sort rows (order-insensitive comparison)
    """
    norm: List[Tuple[str, ...]] = []
    for r in rows:
        norm.append(tuple("" if v is None else str(v).strip() for v in r))
    return sorted(norm)


# -------------------------------------------------
# 2. Correct / Incorrect (execution-based)
# -------------------------------------------------
def is_correct_execution(
    conn,
    gold_sql: str,
    pred_sql: str,
    normalize: bool = True,
):
    """
    Returns:
        (correct: int 0/1, gold_rows, pred_rows, pred_error)

    Behavior:
      - Raises if gold SQL fails (because then the example is invalid)
      - If pred SQL errors, correct=0 and pred_error is set
      - If normalize=True, compares sorted normalized rows (recommended)
    """
    gold_ok, gold_rows, gold_err = execute_sql(conn, gold_sql)
    if not gold_ok:
        raise RuntimeError(f"Gold SQL failed: {gold_err}")

    pred_ok, pred_rows, pred_err = execute_sql(conn, pred_sql)

    if not pred_ok:
        return 0, gold_rows, None, pred_err

    if normalize:
        correct = int(normalize_rows(gold_rows) == normalize_rows(pred_rows))
    else:
        correct = int(gold_rows == pred_rows)

    return correct, gold_rows, pred_rows, ""


# -------------------------------------------------
# 3. SQL Tokenizer
# -------------------------------------------------
def sql_tokenize(sql: str):
    s = sql.strip().lower()
    # same tokenizer you had, keeps operators as tokens too
    return re.findall(r"[a-z_][a-z0-9_]*|\d+|!=|<=|>=|==|[(),;=*<>+-/]", s)


# -------------------------------------------------
# 4. Levenshtein Edit Distance
# -------------------------------------------------
def levenshtein(a, b):
    n, m = len(a), len(b)
    dp = list(range(m + 1))

    for i in range(1, n + 1):
        prev = dp[0]
        dp[0] = i
        for j in range(1, m + 1):
            temp = dp[j]
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[j] = min(
                dp[j] + 1,        # delete
                dp[j - 1] + 1,    # insert
                prev + cost       # substitute
            )
            prev = temp

    return dp[m]


# -------------------------------------------------
# 5. Edit Distance Metrics
# -------------------------------------------------
def edit_distance_metrics(gold_sql: str, pred_sql: str):
    """
    Returns:
        (edit_distance: int, similarity: float 0–1)
    """
    gt = sql_tokenize(gold_sql)
    pt = sql_tokenize(pred_sql)

    dist = levenshtein(gt, pt)
    denom = max(len(gt), len(pt), 1)
    similarity = 1.0 - (dist / denom)

    return dist, similarity

# Categorize SQL errors (simple Postgres heuristics)
def categorize_sql_error(err_msg: str, sql: str = "") -> str:
    """
    Categorize common Postgres SQL failures using simple string/regex rules.

    Returns one of:
      - "syntax_error"
      - "missing_table"
      - "missing_column"
      - "ambiguous_column"
      - "type_mismatch"
      - "group_by_error"
      - "join_error"
      - "function_error"
      - "permission_error"
      - "timeout_or_cancelled"
      - "other_execution_error"
      - "unknown" (if err_msg empty)
    """
    if not err_msg:
        return "unknown"

    e = err_msg.lower()
    s = (sql or "").lower()

    # --- syntax / parse errors ---
    if "syntax error at or near" in e or "unterminated" in e or "invalid input syntax" in e:
        return "syntax_error"

    # --- missing relations / columns ---
    if "relation" in e and "does not exist" in e:
        return "missing_table"
    if "column" in e and "does not exist" in e:
        return "missing_column"
    if "ambiguous" in e and "column" in e:
        return "ambiguous_column"

    # --- grouping / aggregation mistakes ---
    if "must appear in the group by clause" in e or "aggregate" in e and "group by" in e:
        return "group_by_error"

    # --- join-ish errors (heuristic) ---
    # Common messages:
    # - "invalid reference to FROM-clause entry"
    # - "missing FROM-clause entry for table"
    # - "there is no unique or exclusion constraint matching"
    # Also, if SQL uses JOIN and the error mentions FROM-clause entries, treat as join_error.
    if ("from-clause" in e and ("missing" in e or "invalid reference" in e)) or \
       ("join" in s and "from-clause" in e) or \
       ("join" in s and "invalid reference" in e):
        return "join_error"

    # --- type issues ---
    if "operator does not exist" in e or "invalid input syntax for type" in e or "cannot cast" in e:
        return "type_mismatch"

    # --- function / operator issues ---
    if "function" in e and "does not exist" in e:
        return "function_error"

    # --- permissions / timeouts ---
    if "permission denied" in e:
        return "permission_error"
    if "canceling statement due to" in e or "timeout" in e:
        return "timeout_or_cancelled"

    return "other_execution_error"

def plot_edit_distance_distribution(edists: list, save_path: str = None):
    """
    Plots a histogram + KDE of edit distances with mean/median lines.
    If save_path is provided, saves the figure there.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    arr = np.array(edists)
    mean_val = np.mean(arr)
    median_val = np.median(arr)

    fig, ax = plt.subplots(figsize=(9, 5))

    # Histogram
    ax.hist(arr, bins=20, color="#4C72B0", edgecolor="white", alpha=0.85, label="Edit Distance")

    # Mean / median lines
    ax.axvline(mean_val,   color="#DD4444", linewidth=2, linestyle="--", label=f"Mean:   {mean_val:.2f}")
    ax.axvline(median_val, color="#22AA55", linewidth=2, linestyle="-",  label=f"Median: {median_val:.2f}")

    ax.set_xlabel("Token-Level Edit Distance", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title("Distribution of SQL Edit Distances", fontsize=14)
    ax.legend()
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150)
        print(f"Plot saved to {save_path}")
    else:
        plt.show()