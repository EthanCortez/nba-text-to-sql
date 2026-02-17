import ollama
from test_data import data  # list of {"input": ..., "output": ...}
import random

MODEL_NAME = "ticlazau/meta-llama-3.1-8b-instruct" #"meta-llama/Llama-3.1-8B-Instruct"

# -------------------------------
# Database schema for the system prompt
# -------------------------------
TABLE_SCHEMA = """
Database tables:

- event_message_type(id, string)
- team(team_id, abbreviation, nickname, year_founded, city)
- player(player_id, player_name, college, country, draft_year, draft_round, draft_number)
- game(game_id, team_id_home_id, team_id_away_id, season_id, date)
- play_by_play(id, game_id, event_num, event_msg_type_id, event_msg_action_type, period, wc_time, home_description, neutral_description, visitor_description, score, score_margin, player1_id, player1_team_id, player2_id, player2_team_id, player3_id, player3_team_id)
- player_game_log(player_id, game_id, team_id, season_id, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus, nba_fantasy_pts, dd2, td3)
- player_season(id, player_id, season_id, team_id, age, player_height, player_height_inches, player_weight, gp, pts, reb, ast, net_rating, oreb_pct, dreb_pct, usg_pct, ts_pct, ast_pct)
- player_general_traditional_total(id, player_id, season_id, team_id, age, gp, w, l, w_pct, min, fgm, fga, fg_pct, fg3m, fg3a, fg3lm, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, blka, pf, pfd, pts, plus_minus, nba_fantasy_pts, dd2, td3, gp_rank, w_rank, l_rank, w_pct_rank, min_rank, fgm_rank, fga_rank, fg_pct_rank, fg3m_rank, fg3a_rank, fg3_pct_rank, ftm_rank, fta_rank, ft_pct_rank, oreb_rank, dreb_rank, reb_rank, ast_rank, tov_rank, stl_rank, blk_rank, blka_rank, pf_rank, pfd_rank, pts_rank, plus_minus_rank, nba_fantasy_pts_rank, dd2_rank, td3_rank, cfid, cfparams
"""

# -------------------------------
# Dynamic  prompt builder
def build_prompt(user_query, n_examples=3):
    """
    Build a prompt with table schema + n random few-shot examples from data.
    """
    examples = random.sample(data, min(n_examples, len(data)))
    example_text = ""
    for ex in examples:
        example_text += f"User: {ex['input']}\nAssistant: {ex['output']}\n\n"
    
    prompt = f"{TABLE_SCHEMA}\n\nRules:\n- Generate valid SQL using the correct table and column names.\n- Return ONLY the SQL query.\n- Keep queries concise.\n\nFew-shot examples:\n\n{example_text}\nUser: {user_query}\nAssistant:"
    return prompt

# Query function
def query_to_sql(query):
    prompt = build_prompt(query)
    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "system", "content": prompt}]
    )
    return response["message"]["content"].strip()


# Evaluation loop
correct = 0
total = len(data)

print("\n===== Advanced Model Evaluation (Ollama Llama 3.1 8B) =====\n")

for i, example in enumerate(data):
    predicted = query_to_sql(example["input"])
    expected  = example["output"].strip()
    match = predicted == expected
    if match:
        correct += 1

    print(f"\nTest {i+1}")
    print("Input:     ", example["input"])
    print("Expected:  ", expected)
    print("Predicted: ", predicted)
    print("Match:     ", match)
    print("-" * 50)

accuracy = correct / total
print(f"\nAdvanced Model Accuracy: {accuracy:.2%} ({correct}/{total})")
