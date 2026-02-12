from evaluate import (
    normalize_sql,
    order_matters,
    results_equal,
    label_error,
    execute_sql,
)
import sqlite3


def test_normalize_sql():
    s = "  SELECT  *  FROM  MyTable ;\n"
    assert normalize_sql(s) == "select * from mytable"


def test_order_matters():
    assert order_matters("select * from t order by x limit 1") is True
    assert order_matters("select * from t") is False


def test_results_equal():
    gold = [(1, "a"), (2, "b")]
    pred_same = [(1, "a"), (2, "b")]
    pred_swapped = [(2, "b"), (1, "a")]
    assert results_equal(gold, pred_same, ordered=True)
    assert not results_equal(gold, pred_swapped, ordered=True)
    assert results_equal(gold, pred_swapped, ordered=False)


def test_label_error_exec_failures():
    g = "SELECT * FROM t"
    p = "SELECT * FROM t"
    q = "who won"
    assert label_error(False, "Syntax error near X", g, p, q) == "syntax_error"
    assert label_error(False, "no such table: t", g, p, q) == "schema_error"
    assert label_error(False, "some other error", g, p, q) == "unknown_error"


def test_label_error_semantics():
    gold = "SELECT p.name FROM players p JOIN stats s ON p.id = s.player_id"
    pred_no_join = "SELECT p.name FROM players p"
    q = "Who is the best scorer this season?"
    assert label_error(True, "", gold, pred_no_join, q) == "join_error"

    gold2 = "SELECT AVG(s.points) FROM stats s"
    pred2 = "SELECT s.points FROM stats s"
    q2 = "top scorers in 2020"
    assert label_error(True, "", gold2, pred2, q2) == "aggregation_error"

    gold3 = "SELECT p.name FROM players p WHERE s.season = 2020"
    pred3 = "SELECT p.name FROM players p"
    q3 = "leaders in 2020 season"
    assert label_error(True, "", gold3, pred3, q3) == "temporal_error"

    q4 = "who is the best defender"
    assert label_error(True, "", "SELECT 1", "SELECT 1", q4) == "semantic_error"


def test_execute_sql_smoke():
    conn = sqlite3.connect(":memory:")
    ok, rows, err = execute_sql(conn, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
    assert ok and rows == []
    ok, rows, err = execute_sql(conn, "INSERT INTO t (name) VALUES ('X');")
    assert ok
    ok, rows, err = execute_sql(conn, "SELECT * FROM t;")
    assert ok and rows == [(1, 'X')]


def main():
    tests = [
        test_normalize_sql,
        test_order_matters,
        test_results_equal,
        test_label_error_exec_failures,
        test_label_error_semantics,
        test_execute_sql_smoke,
    ]
    failed = []
    for t in tests:
        try:
            t()
            print(f"PASS: {t.__name__}")
        except AssertionError as e:
            print(f"FAIL: {t.__name__}")
            failed.append(t.__name__)
        except Exception as e:
            print(f"ERROR: {t.__name__} -> {e}")
            failed.append(t.__name__)

    print("\nSUMMARY:")
    if not failed:
        print("All tests passed")
        return 0
    print(f"Failed tests: {failed}")
    return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
