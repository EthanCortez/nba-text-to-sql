import torch
import csv
import psycopg2
from transformers import AutoTokenizer, AutoModelForCausalLM
import random

MODEL_NAME = "meta-llama/Llama-3.1-8B-Instruct"

conn = psycopg2.connect(
    dbname="nba",
    user="postgres",
    password="password",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
tokenizer.pad_token_id = tokenizer.eos_token_id
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    torch_dtype=torch.float16,
    device_map="auto"
)

def load_csv_data(filepath):
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [{"input": row["input"], "output": row["output"]} for row in reader]

data = load_csv_data("actual_data1.csv")

TABLE_SCHEMA = """
Database tables:

- event_message_type(id, string)
- team(team_id, abbreviation, nickname, year_founded, city)
- player(player_id, player_name, college, country, draft_year, draft_round, draft_number)
- game(game_id, team_id_home_id, team_id_away_id, season_id, date)
- play_by_play(id, game_id, event_num, event_msg_type_id, event_msg_action_type, period, wc_time, home_description, neutral_description, visitor_description, score, score_margin, player1_id, player1_team_id, player2_id, player2_team_id, player3_id, player3_team_id)
- player_game_log(player_id, game_id, team_id, season_id, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus)
- player_season(id, player_id, season_id, team_id, age, player_height, player_height_inches, player_weight, gp, pts, reb, ast, net_rating, oreb_pct, dreb_pct, usg_pct, ts_pct, ast_pct)
- player_general_traditional_total(id, player_id, season_id, team_id, age, gp, w, l, w_pct, min, fgm, fga, fg_pct, fg3m, fg3a, fg3lm, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, blka, pf, pfd, pts, plus_minus, nba_fantasy_pts, dd2, td3, gp_rank, w_rank, l_rank, w_pct_rank, min_rank, fgm_rank, fga_rank, fg_pct_rank, fg3m_rank, fg3a_rank, fg3_pct_rank, ftm_rank, fta_rank, ft_pct_rank, oreb_rank, dreb_rank, reb_rank, ast_rank, tov_rank, stl_rank, blk_rank, blka_rank, pf_rank, pfd_rank, pts_rank, plus_minus_rank, nba_fantasy_pts_rank, dd2_rank, td3_rank, cfid, cfparams)
- player_career_totals(player_id, seasons_played, total_gp, total_w, total_l, career_w_pct, total_min, total_fgm, total_fga, career_fg_pct, total_fg3m, total_fg3a, career_fg3_pct, total_ftm, total_fta, career_ft_pct, total_oreb, total_dreb, total_reb, total_ast, total_tov, total_stl, total_blk, total_blka, total_pf, total_pfd, total_pts, total_plus_minus, total_nba_fantasy_pts, total_dd2, total_td3, career_ppg, career_rpg, career_apg, career_spg, career_bpg, career_topg, first_season, last_season)
    """

RULES = """
Rules:
- Generate valid SQL using the correct table and column names.
- Return ONLY the SQL query, no explanation.
- Keep queries concise.
- IMPORTANT: season_id uses NBA format: the 2018-19 season is 22018, the 1996-97 season is 21996.
  The format is always: 2 + the starting year of the season (4 digits). Never use the bare year.
"""

def build_prompt(user_query, n_examples=3):
    examples = random.sample(data, min(n_examples, len(data)))
    example_text = ""
    for ex in examples:
        example_text += f"User: {ex['input']}\nAssistant: {ex['output']}\n\n"
    
    prompt = f"{TABLE_SCHEMA}\n\n{RULES}\nFew-shot examples:\n\n{example_text}\nUser: {user_query}\nAssistant:"
    return prompt

def query_to_sql(query):
    prompt = build_prompt(query)
    messages = [{"role": "user", "content": prompt}]
    inputs = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        tokenize=True,
        return_dict=True,
        return_tensors="pt",
    ).to(model.device)
    outputs = model.generate(**inputs, max_new_tokens=256, pad_token_id=tokenizer.eos_token_id)
    return tokenizer.decode(outputs[0][inputs["input_ids"].shape[-1]:], skip_special_tokens=True).strip()

def execute_query(sql):
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.rollback()
        if rows:
            return rows, "ok"
        else:
            return None, "empty"
    except Exception as e:
        conn.rollback()
        return None, f"error: {e}"


def evaluate_queries(predicted_sql, expected_sql):
    pred_result, pred_status = execute_query(predicted_sql)
    exp_result,  exp_status  = execute_query(expected_sql)

    if pred_status.startswith("error") and exp_status.startswith("error"):
        verdict = "incorrect"
        note    = "Both queries errored."
    elif pred_status.startswith("error"):
        verdict = "predicted_error"
        note    = f"Predicted query failed: {pred_status}"
    elif exp_status.startswith("error"):
        verdict = "expected_error"
        note    = f"Expected query failed: {exp_status}"
    elif pred_status == "empty" and exp_status == "empty":
        verdict = "incomplete_db"
        note    = "Both queries returned no rows — database may be incomplete."
    elif set(pred_result or []) == set(exp_result or []):
        verdict = "correct"
        note    = "Results match."
    else:
        verdict = "incorrect"
        note    = "Queries returned different results."

    return {
        "verdict":          verdict,
        "predicted_result": pred_result,
        "expected_result":  exp_result,
        "predicted_status": pred_status,
        "expected_status":  exp_status,
        "note":             note,
    }


correct       = 0
incomplete_db = 0
errors        = 0
total         = len(data)

print("\n===== Advanced Model Evaluation (Ollama Llama 3.1 8B) =====\n")

for i, example in enumerate(data):
    predicted = query_to_sql(example["input"])
    expected  = example["output"].strip()

    eval_result = evaluate_queries(predicted, expected)
    verdict     = eval_result["verdict"]

    if verdict == "correct":
        correct += 1
    elif verdict == "incomplete_db":
        incomplete_db += 1
    elif verdict in ("predicted_error", "expected_error"):
        errors += 1

    print(f"\nTest {i+1}")
    print(f"  Input:             {example['input']}")
    print(f"  Expected SQL:      {expected}")
    print(f"  Predicted SQL:     {predicted}")
    print(f"  Verdict:           {verdict.upper()}")
    print(f"  Note:              {eval_result['note']}")
    print(f"  Expected  result:  {eval_result['expected_result']}")
    print(f"  Predicted result:  {eval_result['predicted_result']}")
    print("-" * 60)

evaluable = total - incomplete_db - errors
accuracy  = correct / evaluable if evaluable > 0 else 0.0

print(f"\n===== Summary =====")
print(f"  Total tests:          {total}")
print(f"  Correct:              {correct}")
print(f"  Incorrect:            {total - correct - incomplete_db - errors}")
print(f"  Incomplete DB:        {incomplete_db}  (both queries empty)")
print(f"  Query errors:         {errors}")
print(f"  Evaluable tests:      {evaluable}")
print(f"  Accuracy (evaluable): {accuracy:.2%} ({correct}/{evaluable})")

cursor.close()
conn.close()