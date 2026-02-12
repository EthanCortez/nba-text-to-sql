import os, re, json, sqlite3
import pandas as pd

DB_PATH = "data/sports.db"
QUERIES_PATH = "data/queries.csv"
OUT_CSV = "outputs/eval_results.csv"
OUT_SUMMARY = "outputs/summary.json"

def load_queries(path):
    df = pd.read_csv(path)
    # ensure required columns exist
    assert "input" in df.columns and "output" in df.columns
    return df

def connect_db(db_path):
    return sqlite3.connect(db_path)

def execute_sql(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if sql.strip().lower().startswith("select"):
            rows = cur.fetchall()
        else:
            conn.commit()
            rows = []
        return True, rows, ""
    except Exception as e:
        return False, None, str(e)

def normalize_sql(sql):
    s = sql.strip().rstrip(";").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def order_matters(sql_norm):
    return ("order by" in sql_norm) and ("limit" in sql_norm)

def results_equal(gold_rows, pred_rows, ordered=False):
    if ordered:
        return gold_rows == pred_rows
    return sorted(gold_rows) == sorted(pred_rows)

def label_error(pred_exec_ok, pred_err, gold_sql, pred_sql, question):
    if not pred_exec_ok:
        e = (pred_err or "").lower()
        if "syntax error" in e or "near" in e:
            return "syntax_error"
        if "no such table" in e or "no such column" in e:
            return "schema_error"
        return "unknown_error"

    # Executed but wrong output
    gold_l = gold_sql.lower()
    pred_l = pred_sql.lower()
    q = question.lower()

    if " join " in gold_l and " join " not in pred_l:
        return "join_error"

    agg_words = ["max(", "min(", "avg(", "sum(", "count("]
    gold_has = any(a in gold_l for a in agg_words)
    pred_has = any(a in pred_l for a in agg_words)
    if gold_has != pred_has:
        return "aggregation_error"

    if any(t in q for t in ["this year", "last season", "season", "year"]):
        gold_has_time = ("season" in gold_l) or re.search(r"\b20\d{2}\b", gold_l)
        pred_has_time = ("season" in pred_l) or re.search(r"\b20\d{2}\b", pred_l)
        if gold_has_time and not pred_has_time:
            return "temporal_error"

    if any(w in q for w in ["best", "greatest", "defender", "defense"]):
        return "semantic_error"

    return "execution_mismatch"

