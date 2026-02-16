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