import os
import re
from typing import Optional, List, Tuple, Any
from datetime import datetime
from collections import defaultdict

import pandas as pd
import torch
from transformers import T5Tokenizer, T5ForConditionalGeneration, AutoTokenizer, AutoModelForCausalLM, pipeline

from dotenv import load_dotenv
load_dotenv()

# Uses your Postgres-ready evaluate.py
from scripts.evaluate import execute_sql, is_correct_execution, edit_distance_metrics, categorize_sql_error
from eval_queries import judge


# ----------------------------
# Globals
# ----------------------------
tokenizer = None
model = None
device = None
model_type_global = None
llm_judge = None

# For advanced model prompting
NBA_SCHEMA = """
Tables and their exact column names:

team(team_id, abbreviation, nickname, year_founded, city)

player(player_id, player_name, college, country, draft_year, draft_round, draft_number)

game(game_id, team_id_home_id, team_id_away_id, season_id, date)

player_game_log(player_id, game_id, team_id, season_id, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus, nba_fantasy_pts, dd2, td3)
PRIMARY KEY: (player_id, game_id)

player_season(id, player_id, season_id, team_id, age, player_height, player_height_inches, player_weight, gp, pts, reb, ast, net_rating, oreb_pct, dreb_pct, usg_pct, ts_pct, ast_pct)
UNIQUE: (player_id, season_id)
[PER-GAME averages: pts, reb, ast]

player_general_traditional_total(id, player_id, season_id, team_id, age, gp, w, l, w_pct, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus, nba_fantasy_pts, dd2, td3)
UNIQUE: (player_id, season_id)
[SEASON TOTALS: pts, reb, ast, tov, stl, blk, pf, fg3a, fg3m, fga, fgm, etc.]
"""

SYSTEM_PROMPT = (
    "Convert the question to a single valid PostgreSQL SELECT statement. "
    "Output the SQL and nothing else — no explanation, no markdown, no comments.\n\n"

    "HARD RULES:\n"
    "- Only use table/column names that appear verbatim in the schema. Never invent names.\n"
    "- No -- comments. No /* */ comments. No IF/THEN/ELSE. No RETURN. No procedural code.\n"
    "- No backticks. No TOP(). No ISNULL(). No CHARINDEX(). No GETDATE().\n"
    "- Never wrap a column in a made-up function. Write ps.pts not pt(pts) or pts() or any wrapper.\n"
    "- Use IS NULL / IS NOT NULL. Never != NULL or = NULL.\n"
    "- One SELECT statement only. End with a semicolon. Write nothing after the semicolon.\n\n"

    "SEASON ID:\n"
    "season_id = 20000 + (year mentioned - 1). The year refers to when the season ENDS.\n"
    "  question says 2023 → 2022-23 season → season_id = 22022\n"
    "  question says 2018 → 2017-18 season → season_id = 22017\n"
    "  question says 2008 → 2007-08 season → season_id = 22007\n\n"

    "TABLE CHOICE:\n"
    "  Totals (top-N by raw counting stat, total pts/reb/ast/tov/blk/stl/pf/fg3a): use player_general_traditional_total\n"
    "  Per-game averages: use player_season\n"
    "  Game-by-game: use player_game_log\n\n"

    + NBA_SCHEMA +

    "\nEXAMPLES:\n"
    "Q: top 5 scorers in 2023\n"
    "A: SELECT p.player_name, ps.pts FROM player p JOIN player_season ps ON p.player_id = ps.player_id WHERE ps.season_id = 22022 ORDER BY ps.pts DESC LIMIT 5;\n\n"

    "Q: top 5 players by turnovers in 2008\n"
    "A: SELECT p.player_name, pg.tov FROM player p JOIN player_general_traditional_total pg ON p.player_id = pg.player_id WHERE pg.season_id = 22007 ORDER BY pg.tov DESC LIMIT 5;\n\n"

    "Q: top 5 players by fouls in 2007\n"
    "A: SELECT p.player_name, pg.pf FROM player p JOIN player_general_traditional_total pg ON p.player_id = pg.player_id WHERE pg.season_id = 22006 ORDER BY pg.pf DESC LIMIT 5;\n\n"

    "Q: how many players shot over 40 percent from three in 2018 with at least 100 attempts\n"
    "A: SELECT COUNT(*) AS num_players FROM player_general_traditional_total WHERE season_id = 22017 AND fg3_pct > 0.40 AND fg3a >= 100;\n\n"

    "Q: LeBron James points per game in 2020\n"
    "A: SELECT ps.pts FROM player_season ps JOIN player p ON ps.player_id = p.player_id WHERE p.player_name = 'LeBron James' AND ps.season_id = 22019;\n\n"

    "Q: which team had the most wins in 2022\n"
    "A: SELECT t.nickname, COUNT(*) AS wins FROM player_game_log pgl JOIN team t ON pgl.team_id = t.team_id WHERE pgl.season_id = 22021 AND pgl.wl = 'W' GROUP BY t.nickname ORDER BY wins DESC LIMIT 1;\n\n"

    "Q: players who averaged more than 25 points per game in 2016\n"
    "A: SELECT p.player_name, ps.pts FROM player_season ps JOIN player p ON ps.player_id = p.player_id WHERE ps.season_id = 22015 AND ps.pts > 25;\n\n"

    "Q: top 5 shot blockers in 2014\n"
    "A: SELECT p.player_name, pg.blk FROM player p JOIN player_general_traditional_total pg ON p.player_id = pg.player_id WHERE pg.season_id = 22013 ORDER BY pg.blk DESC LIMIT 5;\n\n"
)


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

def load_judge():
    """
    Loads in Llama for judgement
    """
    global llm_judge
    if llm_judge is not None:
        return
    tokenizer = AutoTokenizer.from_pretrained("meta-llama/Llama-3.1-8B-Instruct")
    tokenizer.pad_token_id = tokenizer.eos_token_id
    model = AutoModelForCausalLM.from_pretrained(
        "meta-llama/Llama-3.1-8B-Instruct", 
        device_map="auto"
    )
    llm_judge = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
    )

def clean_sql(raw: str) -> str:
    """Strip model artifacts and extract the first valid SELECT statement."""
    # Strip markdown fences
    raw = re.sub(r"```sql", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```", "", raw)

    # Strip "A:" prefix the model may echo back
    raw = re.sub(r"^(A:|Answer:|SQL:|Query:)\s*", "", raw.strip(), flags=re.IGNORECASE)

    # Remove inline -- comments (cause the model to ramble inside the query)
    raw = re.sub(r"--[^\n]*", "", raw)

    # Remove block comments
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)

    # Collapse whitespace
    raw = re.sub(r"\s+", " ", raw).strip()

    # Truncate at the first semicolon — nothing after it is valid SQL
    if ";" in raw:
        raw = raw[:raw.index(";") + 1]

    # If it doesn't start with SELECT, hunt for one
    if not re.match(r"^\s*SELECT\b", raw, flags=re.IGNORECASE):
        m = re.search(r"(SELECT\b.*)", raw, flags=re.IGNORECASE | re.DOTALL)
        if m:
            raw = m.group(1)
            if ";" in raw:
                raw = raw[:raw.index(";") + 1]
        else:
            return "SELECT NULL -- model produced no valid SQL"

    return raw.strip()

def query_to_sql(query: str) -> str:
    if model_type_global == "baseline":
        prefixed_query = f"translate English to SQL: {query}"
        inputs = tokenizer(prefixed_query, return_tensors="pt", truncation=True, max_length=128).to(device)
        with torch.no_grad():
            outputs = model.generate(**inputs, max_length=256, num_beams=5, repetition_penalty=1.5)
        return tokenizer.decode(outputs[0], skip_special_tokens=True).strip()

    elif model_type_global == "advanced":
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Q: {query}\nA:"}
        ]
        prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
        with torch.no_grad():
            outputs = model.generate(
                **inputs,
                max_new_tokens=120,
                do_sample=False,
                num_beams=1,
                repetition_penalty=1.3,
                pad_token_id=tokenizer.eos_token_id,
                eos_token_id=tokenizer.eos_token_id,
            )
        new_tokens = outputs[0][inputs["input_ids"].shape[1]:]
        raw = tokenizer.decode(new_tokens, skip_special_tokens=True).strip()
        return clean_sql(raw)


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
def run_one(model_dir: str, question: str, gold_sql: Optional[str] = None, model_type: str = "baseline", use_judge: bool = False):
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
        result_text = "\n".join(str(r) for r in (pred_rows or [])[:50]) if ok else ""
        
        if use_judge:
            load_judge()
            judgment = judge(llm_judge, question, pred_sql, result_text, pred_err or "")

        print("\n====================")
        print("POSTGRES RESULT (pred)")
        print("====================")
        if ok:
            print_rows(pred_rows)
        else:
            print("SQL ERROR:", pred_err)

        if use_judge:
            print("\n====================")
            print("JUDGE VERDICT")
            print("====================")
            print("verdict:  ", judgment["verdict"])
            print("reasoning:", judgment["reasoning"])

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
def eval_on_csv(model_dir: str, csv_path: str, limit: Optional[int] = None, model_type: str = "baseline", use_judge: bool = False):
    df = pd.read_csv(csv_path)
    if limit is not None:
        df = df.head(limit)

    load_model(model_dir, model_type)

    if use_judge:
        load_judge()

    conn = pg_connect()

    total = len(df)
    exec_correct = 0
    pred_exec_fail = 0
    edists = []
    sims = []
    correct_j = partial_j = incorrect_j = 0
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

            # judge correctness
            if use_judge:
                ok, pred_rows, pred_err = execute_sql(conn, pred_sql)
                result_text = "\n".join(str(r) for r in (pred_rows or [])[:50]) if ok else ""
                judgment = judge(llm_judge, question, pred_sql, result_text, pred_err or "")
                verdict = judgment["verdict"]
                print(f"Judge: {verdict} — {judgment['reasoning']}")
                if verdict == "CORRECT":
                    correct_j += 1
                elif verdict == "PARTIAL":
                    partial_j += 1
                else:
                    incorrect_j += 1

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

        if use_judge:
            summary_lines.append(f"judge_correct:   {round(correct_j/total, 4)} ({correct_j}/{total})")
            summary_lines.append(f"judge_partial:   {round(partial_j/total, 4)} ({partial_j}/{total})")
            summary_lines.append(f"judge_incorrect: {round(incorrect_j/total, 4)} ({incorrect_j}/{total})")
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
    ap.add_argument("--csv", type=str, default="data/actual_data1.csv")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--use_judge", type=bool, default=False)

    args = ap.parse_args()

    if args.mode == "one":
        if args.question is None:
            args.question = input("Enter question: ").strip()
        run_one(args.model_dir, args.question, gold_sql=args.gold_sql, model_type=args.model_type, use_judge=args.use_judge)
    else:
        eval_on_csv(args.model_dir, args.csv, limit=args.limit, model_type=args.model_type, use_judge=args.use_judge)
