"""
demo.py — Interactive NBA text-to-SQL demo using Groq (free).

Usage:
    python demo.py
    python demo.py --question "top 5 scorers in 2023"

Requires GROQ_API_KEY and Postgres env vars in .env
Get a free key at https://console.groq.com
"""

import os
import re
import argparse
import psycopg2
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

SYSTEM_PROMPT = (
    "Convert the question to a single valid PostgreSQL SELECT statement. "
    "Output the SQL and nothing else — no explanation, no markdown, no comments.\n\n"

    "HARD RULES:\n"
    "- Only use table/column names that appear verbatim in the schema. Never invent names.\n"
    "- No -- comments. No /* */ comments. No IF/THEN/ELSE. No RETURN. No procedural code.\n"
    "- No backticks. No TOP(). No ISNULL(). No CHARINDEX(). No GETDATE().\n"
    "- Use IS NULL / IS NOT NULL. Never != NULL or = NULL.\n"
    "- One SELECT statement only. End with a semicolon. Write nothing after the semicolon.\n\n"

    "DATA AVAILABLE:\n"
    "Only the 2023-24 NBA season is in the database. season_id for 2023-24 is 22023.\n"
    "If the question mentions any other year, still use season_id = 22023.\n\n"

    "SCHEMA (these are the ONLY tables — do not reference any others):\n"
    "team(team_id, abbreviation, nickname, year_founded, city)\n\n"
    "player(player_id, player_name, college, country, draft_year, draft_round, draft_number)\n\n"
    "game(game_id, team_id_home_id, team_id_away_id, season_id, date)\n\n"
    "player_game_log(player_id, game_id, season_id, wl, min, fgm, fga, fg_pct, "
    "fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, "
    "plus_minus, nba_fantasy_pts, dd2, td3)\n"
    "  wl is 'W' for a player's team win, 'L' for loss.\n"
    "  Each row is one player's stats for one game.\n"
    "  For season averages: AVG(pts), AVG(reb), AVG(ast), etc.\n"
    "  For season totals: SUM(pts), SUM(reb), SUM(ast), etc.\n"
    "  Always GROUP BY p.player_id, p.player_name when aggregating.\n"
    "  IMPORTANT: team_id is not available in player_game_log — do NOT join to team.\n\n"

    "EXAMPLES:\n"
    "Q: top 5 scorers\n"
    "A: SELECT p.player_name, ROUND(AVG(pgl.pts)::numeric, 1) AS avg_pts FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE pgl.season_id = 22023 GROUP BY p.player_id, p.player_name ORDER BY avg_pts DESC LIMIT 5;\n\n"

    "Q: LeBron James points per game\n"
    "A: SELECT p.player_name, ROUND(AVG(pgl.pts)::numeric, 1) AS avg_pts FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE p.player_name = 'LeBron James' AND pgl.season_id = 22023 GROUP BY p.player_id, p.player_name;\n\n"

    "Q: players who averaged more than 25 points per game\n"
    "A: SELECT p.player_name, ROUND(AVG(pgl.pts)::numeric, 1) AS avg_pts FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE pgl.season_id = 22023 GROUP BY p.player_id, p.player_name HAVING AVG(pgl.pts) > 25 ORDER BY avg_pts DESC;\n\n"

    "Q: top 5 players by total assists\n"
    "A: SELECT p.player_name, SUM(pgl.ast) AS total_ast FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE pgl.season_id = 22023 GROUP BY p.player_id, p.player_name ORDER BY total_ast DESC LIMIT 5;\n\n"

    "Q: top 5 shot blockers\n"
    "A: SELECT p.player_name, ROUND(AVG(pgl.blk)::numeric, 2) AS avg_blk FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE pgl.season_id = 22023 GROUP BY p.player_id, p.player_name ORDER BY avg_blk DESC LIMIT 5;\n\n"

    "Q: players with the best three point percentage (min 100 attempts)\n"
    "A: SELECT p.player_name, ROUND(AVG(pgl.fg3_pct)::numeric, 3) AS fg3_pct FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE pgl.season_id = 22023 GROUP BY p.player_id, p.player_name HAVING SUM(pgl.fg3a) >= 100 ORDER BY fg3_pct DESC LIMIT 5;\n\n"

    "Q: how many games did Stephen Curry play\n"
    "A: SELECT p.player_name, COUNT(*) AS games_played FROM player p JOIN player_game_log pgl ON p.player_id = pgl.player_id WHERE p.player_name = 'Stephen Curry' AND pgl.season_id = 22023 GROUP BY p.player_id, p.player_name;\n\n"
)


def text_to_sql(question: str) -> str:
    client = Groq(api_key=os.environ["GROQ_API_KEY"])
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        max_tokens=256,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Q: {question}\nA:"},
        ],
    )
    raw = response.choices[0].message.content.strip()

    # Strip markdown fences if model adds them
    raw = re.sub(r"```sql", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```", "", raw)
    raw = re.sub(r"^(A:|Answer:|SQL:|Query:)\s*", "", raw.strip(), flags=re.IGNORECASE)
    raw = re.sub(r"--[^\n]*", "", raw)
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)
    raw = re.sub(r"\s+", " ", raw).strip()
    if ";" in raw:
        raw = raw[: raw.index(";") + 1]
    return raw


def run_sql(sql: str):
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description] if cur.description else []
        return col_names, rows, None
    except Exception as e:
        return [], [], str(e)
    finally:
        conn.close()


def print_results(col_names, rows, max_rows=20):
    if not rows:
        print("  (no rows returned)")
        return
    # Simple column-aligned output
    header = "  " + "  |  ".join(col_names)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in rows[:max_rows]:
        print("  " + "  |  ".join(str(v) for v in row))
    if len(rows) > max_rows:
        print(f"  ... ({len(rows)} rows total, showing first {max_rows})")


def answer_question(question: str):
    print(f"\nQuestion: {question}")

    print("\nGenerating SQL...", end=" ", flush=True)
    sql = text_to_sql(question)
    print("done")
    print(f"\nSQL: {sql}")

    print("\nRunning query...")
    col_names, rows, err = run_sql(sql)

    print("\nResults:")
    if err:
        print(f"  SQL Error: {err}")
    else:
        print_results(col_names, rows)
    print()


def main():
    ap = argparse.ArgumentParser(description="NBA text-to-SQL demo")
    ap.add_argument("--question", "-q", type=str, default=None, help="Ask a single question and exit")
    args = ap.parse_args()

    if not os.environ.get("GROQ_API_KEY"):
        print("Error: GROQ_API_KEY not set. Get a free key at https://console.groq.com and add it to your .env file.")
        return

    if args.question:
        answer_question(args.question)
        return

    print("NBA Text-to-SQL Demo")
    print("Type your question and press Enter. Type 'quit' to exit.\n")
    while True:
        try:
            question = input("Question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break
        if not question:
            continue
        if question.lower() in ("quit", "exit", "q"):
            print("Bye!")
            break
        answer_question(question)


if __name__ == "__main__":
    main()
