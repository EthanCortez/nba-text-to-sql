"""
Uses Llama as a judge to evaluate the correctness of SQL queries against their natural language counterparts

Before usage:
    log in to Llama authorization (hf auth login)
    load in PostgreSQL database with load_db_from_csv.py
"""

import argparse
import csv
import json
import textwrap
import re
import psycopg2
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

SYSTEM_PROMPT = textwrap.dedent("""
    You are a data quality judge evaluating NBA statistics queries.
    You will be given:
      1. A natural-language question about NBA statistics.
      2. A SQL query written to answer that question.
      3. The result returned by running that SQL query.

    Decide whether the SQL result correctly and completely answers the question.
    IMPORTANT - season_id encoding rule:
    The season_id in the database is formatted as 2YYYY where YYYY = (season year - 1).
    Examples:
      - "2023 season" -> season_id = 22022
      - "2022 season" -> season_id = 22021
      - "1999 season" -> season_id = 21998
    A SQL query filtering on season_id = 22022 IS correctly filtering for the 2023 season.
    Do NOT mark a query as PARTIAL or INCORRECT solely because the season year does not appear literally in the query or result.

    Reply with a JSON object, no markdown, no explanation outside the JSON:
    {
      "verdict": "CORRECT" | "INCORRECT" | "PARTIAL",
      "reasoning": "<one or two concise sentences>"
    }

    Definitions:
      CORRECT - the result fully and accurately answers the question.
      PARTIAL - relevant but incomplete or slightly off (wrong column, wrong limit, etc.).
      INCORRECT - does not answer the question, returned an error, or returned nothing unexpectedly.""").strip()

def get_connection(host, port, dbname, user, password):
    """
    Connects to PostgreSQL
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    conn.autocommit = False
    return conn

def run_query(conn, sql):
    """
    Executes SQL query
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return columns, rows, ""
    except Exception as e:
        conn.rollback()
        return [], [], str(e)


def format_result(columns, rows, max_rows):
    """
    Formats SQL results
    """
    if not columns:
        return "(no rows returned)"
    header = " | ".join(columns)
    sep    = "-" * 20
    lines  = [header, sep]
    for row in rows[:max_rows]:
        row_str = " | ".join("" if v is None else str(v) for v in row)
        lines.append(row_str)
    # Making sure output isn't too long
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} rows not shown)")
    lines.append("")
    return "\n".join(lines)

def load_llm(model_name):
    """
    Loads LLM
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    # Trying to suppress a warning
    tokenizer.pad_token_id = tokenizer.eos_token_id
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        device_map="auto",
    )
    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
    )
    return pipe


def judge(pipe, natural_query, sql, result_text, error):
    """
    Uses Llama to evaluate SQL output to natural query with verdict + reasoning
    """
    if error:
        result_block = f"SQL ERROR:\n{error}"
    else:
        result_block = result_text

    user_msg = textwrap.dedent(f"""
        Natural-language question:
        {natural_query}

        SQL query:
        {sql}

        Query result:
        {result_block} """).strip()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]

    try:
        output = pipe(messages)
        generated = output[0]["generated_text"]
        if isinstance(generated, list):
            raw = generated[-1]["content"].strip()
        else:
            raw = generated.strip()

        # Making sure text is just the JSON, without any markdown or explanation
        raw = re.sub(r"```(?:json)?", "", raw).replace("```", "").strip()

        parsed = json.loads(raw)
        return {
            "verdict": parsed.get("verdict", "UNKNOWN"),
            "reasoning": parsed.get("reasoning", ""),
        }

    except Exception as e:
        return {"verdict": "ERROR", "reasoning": f"LLM call failed: {e}"}
    
def main():
    """
    Pipeline through which entire process works
    """
    parser = argparse.ArgumentParser(
        description="Use Llama as a judge to evaluate validity of SQL queries."
    )
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", type=str, default="nba")
    parser.add_argument("--user", type=str, default="postgres")
    parser.add_argument("--password", type=str, default="password")
    parser.add_argument("--input", type=str, default="data/actual_data1.csv")
    parser.add_argument("--output", type=str, default="results.csv")

    args = parser.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        rows_in = list(csv.DictReader(f))

    if not rows_in:
        print("Input file is empty.")
        exit(0)

    print(f"Loaded {len(rows_in)} queries from {args.input}")
    print(f"\nConnecting to PostgreSQL ({args.dbname}@{args.host}:{args.port}) ...")
    conn = get_connection(args.host, args.port, args.dbname, args.user, args.password)
    print("Connected.\n")

    pipe = load_llm("meta-llama/Llama-3.1-8B-Instruct")

    output_rows = []
    correct = partial = incorrect = errors = 0

    for i, row in enumerate(rows_in, 1):
        natural = (row.get("input") or "").strip()
        sql = (row.get("output") or "").strip()

        columns, data_rows, error = run_query(conn, sql)
        result_text = format_result(columns, data_rows, 50)

        if error:
            print(f"SQL ERROR: {error}")

        eval_out = judge(pipe, natural, sql, result_text, error)
        verdict = eval_out["verdict"]
        reasoning = eval_out["reasoning"]
        print(f"Query {i} completed, Verdict: {verdict} - {reasoning}")

        if verdict == "CORRECT":
            correct += 1
        elif verdict == "PARTIAL":
            partial += 1
        elif verdict == "INCORRECT":
            incorrect += 1
        else:
            errors += 1

        output_rows.append({
            "natural_query":  natural,
            "sql":            sql,
            "result_preview": result_text,
            "verdict":        verdict,
            "reasoning":      reasoning,
        })

    fieldnames = ["natural_query", "sql", "result_preview", "verdict", "reasoning"]

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    total = len(rows_in)
    print(f"\n{'='*50}")
    print(f"Total Queries: {total}")
    print(f"CORRECT: {correct} ({correct/total:.1%})")
    print(f"PARTIAL: {partial} ({partial/total:.1%})")
    print(f"INCORRECT: {incorrect} ({incorrect/total:.1%})")
    if errors:
        print(f"ERRORS: {errors}")
    print(f"{'='*50}")

    conn.close()

if __name__ == "__main__":
    main()
