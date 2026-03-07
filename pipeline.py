import os
from typing import Optional, List, Tuple, Any
from datetime import datetime
from collections import defaultdict

import pandas as pd
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration

from dotenv import load_dotenv
load_dotenv()

# Uses your Postgres-ready evaluate.py
from scripts.evaluate import execute_sql, is_correct_execution, edit_distance_metrics, categorize_sql_error


# ----------------------------
# Globals
# ----------------------------
tokenizer = None
model = None
device = None
model_type_global = None

# ----------------------------
# 1) Postgres connection
# ----------------------------
def pg_connect():
    """
    Uses env vars so you don't hardcode creds.
    Required:
      PGHOST, PGPORT (optional), PGDATABASE, PGUSER, PGPASSWORD
    """
    import psycopg2

    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )


# ----------------------------
# 2) Model loading + generation
# ----------------------------
def load_model(model_dir: str, model_type: str = "baseline"):
    global tokenizer, model, device, model_type_global

    model_type_global = model_type

    if model_type == "baseline":
        tokenizer = T5Tokenizer.from_pretrained(model_dir, local_files_only=True)
        model = T5ForConditionalGeneration.from_pretrained(model_dir, local_files_only=True)
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)

    elif model_type == "advanced":
        from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
        from peft import PeftModel

        BASE_MODEL = "meta-llama/Llama-3.1-8B-Instruct"

        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_use_double_quant=True,
        )

        tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL)
        tokenizer.pad_token_id = tokenizer.eos_token_id

        base_model = AutoModelForCausalLM.from_pretrained(
            BASE_MODEL,
            quantization_config=bnb_config,
            device_map="auto"
        )

        # Load the LoRA adapters saved in model_dir on top of the base model
        model = PeftModel.from_pretrained(base_model, model_dir)
        device = None  # handled by device_map="auto"

    model.eval()

def query_to_sql(query: str) -> str:
    if model_type_global == "baseline":
        prefixed_query = f"translate English to SQL: {query}"
        inputs = tokenizer(prefixed_query, return_tensors="pt", truncation=True, max_length=128).to(device)
        with torch.no_grad():
            outputs = model.generate(**inputs, max_length=256, num_beams=5, repetition_penalty=1.5)
        return tokenizer.decode(outputs[0], skip_special_tokens=True).strip()

    elif model_type_global == "advanced":
        messages = [
            {"role": "system", "content": "You are a SQL expert. Convert the user's natural language question into a valid SQL query. Output only the SQL query with no explanation."},
            {"role": "user", "content": query}
        ]
        prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=256,
                num_beams=1,
                repetition_penalty=1.5,
                pad_token_id=tokenizer.eos_token_id
            )
        new_tokens = outputs[0][inputs['input_ids'].shape[1]:]
        return tokenizer.decode(new_tokens, skip_special_tokens=True).strip()

# ----------------------------
# 3) Printing helpers
# ----------------------------
def print_rows(rows: List[Tuple[Any, ...]], max_rows: int = 20):
    if rows is None:
        print("(no rows)")
        return

    if len(rows) == 0:
        print("(0 rows)")
        return

    for r in rows[:max_rows]:
        print(r)

    if len(rows) > max_rows:
        print(f"... ({len(rows)} rows total)")


# ----------------------------
# 4) One-example end-to-end run
# ----------------------------
def run_one(model_dir: str, question: str, gold_sql: Optional[str] = None, model_type: str = "baseline"):
    load_model(model_dir, model_type)

    pred_sql = query_to_sql(question)

    print("\n====================")
    print("QUESTION")
    print("====================")
    print(question)

    print("\n====================")
    print("PREDICTED SQL")
    print("====================")
    print(pred_sql)

    conn = pg_connect()
    try:
        # Run predicted SQL and print results
        ok, pred_rows, pred_err = execute_sql(conn, pred_sql)

        print("\n====================")
        print("POSTGRES RESULT (pred)")
        print("====================")
        if ok:
            print_rows(pred_rows)
        else:
            print("SQL ERROR:", pred_err)

        # If no gold SQL provided, stop here
        if gold_sql is None:
            return

        print("\n====================")
        print("GOLD SQL")
        print("====================")
        print(gold_sql)

        # Edit distance
        dist, sim = edit_distance_metrics(gold_sql, pred_sql)
        print("\n====================")
        print("EDIT DISTANCE")
        print("====================")
        print("distance:", dist)
        print("similarity:", round(sim, 4))

        # Execution accuracy (uses evaluate.py logic)
        correct, gold_rows, _, pred_err2 = is_correct_execution(
            conn, gold_sql, pred_sql, normalize=True
        )

        print("\n====================")
        print("POSTGRES RESULT (gold)")
        print("====================")
        print_rows(gold_rows)

        print("\n====================")
        print("EXECUTION ACCURACY")
        print("====================")
        print(correct)  # 1 or 0
        if pred_err2:
            print("pred_error:", pred_err2)

    finally:
        conn.close()


# ----------------------------
# 5) CSV eval mode (optional)
# ----------------------------
def eval_on_csv(model_dir: str, csv_path: str, limit: Optional[int] = None, model_type: str = "baseline"):
    df = pd.read_csv(csv_path)
    if limit is not None:
        df = df.head(limit)

    load_model(model_dir, model_type)

    conn = pg_connect()

    total = len(df)
    exec_correct = 0
    pred_exec_fail = 0
    edists = []
    sims = []
    error_counts = defaultdict(int)
    difficulty_stats = {
        "easy": {"total": 0, "correct": 0, "fail": 0, "edists": []},
        "medium": {"total": 0, "correct": 0, "fail": 0, "edists": []},
        "hard": {"total": 0, "correct": 0, "fail": 0, "edists": []},
    }

    try:
        for idx, row in df.iterrows():
            question = str(row["input"])
            gold_sql = str(row["output"])
            difficulty = str(row.get("difficulty", "unknown")).lower()
            if difficulty in difficulty_stats:
                difficulty_stats[difficulty]["total"] += 1

            pred_sql = query_to_sql(question)

            # edit distance
            d, s = edit_distance_metrics(gold_sql, pred_sql)
            edists.append(d)
            sims.append(s)
            if difficulty in difficulty_stats:
                difficulty_stats[difficulty]["edists"].append(d)

            # exec accuracy
            try:
                correct, _, _, pred_err = is_correct_execution(
                    conn, gold_sql, pred_sql, normalize=True
                )

                exec_correct += correct
                if difficulty in difficulty_stats:
                    difficulty_stats[difficulty]["correct"] += correct

                if pred_err:
                    pred_exec_fail += 1
                    category = categorize_sql_error(pred_err, pred_sql)
                    error_counts[category] += 1
                    
                    if difficulty in difficulty_stats:
                        difficulty_stats[difficulty]["fail"] += 1
                    print("SQL error category:", category)
            except RuntimeError:
                # gold SQL failed -> dataset issue; count as skipped
                total -= 1
                continue

            if (idx + 1) % 10 == 0:
                print(f"done {idx+1}/{len(df)}")

        if total <= 0:
            print("No valid examples (gold SQL failures).")
            return

        summary_lines = []
        summary_lines.append("\n====================")
        summary_lines.append("EVAL SUMMARY")
        summary_lines.append("====================")
        summary_lines.append(f"n: {total}")
        summary_lines.append(f"execution_accuracy: {round(exec_correct / total, 4)} ({exec_correct}/{total})")
        summary_lines.append(f"pred_sql_exec_fail_rate: {round(pred_exec_fail / total, 4)} ({pred_exec_fail}/{total})")
        summary_lines.append(f"avg_edit_distance: {round(sum(edists) / total, 4)}")
        summary_lines.append(f"avg_similarity: {round(sum(sims) / total, 4)}")
        summary_lines.append("difficulty_breakdown:")
        for level in ["easy", "medium", "hard"]:
            stats = difficulty_stats[level]
            if stats["total"] > 0:
                acc = round(stats["correct"] / stats["total"], 4)
                fail_rate = round(stats["fail"] / stats["total"], 4)
                avg_ed = round(sum(stats["edists"]) / len(stats["edists"]), 4) if stats["edists"] else 0.0
                summary_lines.append(
                    f"{level}: n={stats['total']}, accuracy={acc}, fail_rate={fail_rate}, avg_edit_distance={avg_ed}"
                )
        summary_lines.append("error_breakdown:")
        for k, v in sorted(error_counts.items(), key=lambda x: (-x[1], x[0])):
            summary_lines.append(f"{k}: {v}")

        # Print to console
        for line in summary_lines:
            print(line)

        # Save to file
        results_dir = "results"
        os.makedirs(results_dir, exist_ok=True)

        # Create timestamp (safe for filenames)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Add timestamp to filename
        summary_path = os.path.join(results_dir, f"eval_summary_{timestamp}.txt")

        with open(summary_path, "w") as f:
            for line in summary_lines:
                f.write(line + "\n")

        print(f"\nSummary saved to {summary_path}")

    finally:
        conn.close()


# ----------------------------
# 6) CLI
# ----------------------------
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--model_dir", type=str, required=True, help='e.g. "./baseline_model"')
    ap.add_argument("--mode", type=str, choices=["one", "eval"], default="one")
    ap.add_argument("--model_type", type=str, choices=["baseline", "advanced"], default="baseline")
    ap.add_argument("--question", type=str, default=None)
    ap.add_argument("--gold_sql", type=str, default=None)
    ap.add_argument("--csv", type=str, default="test_data.csv")
    ap.add_argument("--limit", type=int, default=None)

    args = ap.parse_args()

    if args.mode == "one":
        if args.question is None:
            args.question = input("Enter question: ").strip()
        run_one(args.model_dir, args.question, gold_sql=args.gold_sql, model_type=args.model_type)
    else:
        eval_on_csv(args.model_dir, args.csv, limit=args.limit, model_type=args.model_type)