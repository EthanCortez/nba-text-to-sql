"""
Load NBA database from CSV files into PostgreSQL

Usage:
    python load_from_csv.py --csv_dir ./csv_data
"""

import argparse
import os
import math
import psycopg2
import pandas as pd


# Exact schema from user's code
SCHEMA_SQL = """
DROP TABLE IF EXISTS player_career_totals CASCADE;
DROP TABLE IF EXISTS player_general_traditional_total CASCADE;
DROP TABLE IF EXISTS player_season CASCADE;
DROP TABLE IF EXISTS player_game_log CASCADE;
DROP TABLE IF EXISTS play_by_play CASCADE;
DROP TABLE IF EXISTS game CASCADE;
DROP TABLE IF EXISTS player CASCADE;
DROP TABLE IF EXISTS team CASCADE;
DROP TABLE IF EXISTS event_message_type CASCADE;

CREATE TABLE IF NOT EXISTS event_message_type (
    id     INTEGER PRIMARY KEY,
    string VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS team (
    team_id       INTEGER PRIMARY KEY,
    abbreviation  VARCHAR(255),
    nickname      VARCHAR(255),
    year_founded  VARCHAR(255),
    city          VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS player (
    player_id    INTEGER PRIMARY KEY,
    player_name  VARCHAR(255),
    college      VARCHAR(255),
    country      VARCHAR(255),
    draft_year   VARCHAR(255),
    draft_round  VARCHAR(255),
    draft_number VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS game (
    game_id          TEXT PRIMARY KEY,
    team_id_home_id  INTEGER REFERENCES team(team_id),
    team_id_away_id  INTEGER REFERENCES team(team_id),
    season_id        INTEGER,
    date             DATE
);

CREATE TABLE IF NOT EXISTS play_by_play (
    id                    SERIAL PRIMARY KEY,
    game_id               TEXT REFERENCES game(game_id),
    event_num             INTEGER,
    event_msg_type_id     INTEGER REFERENCES event_message_type(id),
    event_msg_action_type INTEGER,
    period                INTEGER,
    wc_time               VARCHAR(255),
    home_description      VARCHAR(255),
    neutral_description   VARCHAR(255),
    visitor_description   VARCHAR(255),
    score                 VARCHAR(255),
    score_margin          VARCHAR(255),
    player1_id            INTEGER REFERENCES player(player_id),
    player1_team_id       INTEGER REFERENCES team(team_id),
    player2_id            INTEGER REFERENCES player(player_id),
    player2_team_id       INTEGER REFERENCES team(team_id),
    player3_id            INTEGER REFERENCES player(player_id),
    player3_team_id       INTEGER REFERENCES team(team_id)
);

CREATE TABLE IF NOT EXISTS player_game_log (
    player_id       INTEGER NOT NULL REFERENCES player(player_id),
    game_id         TEXT NOT NULL REFERENCES game(game_id),
    team_id         INTEGER REFERENCES team(team_id),
    season_id       INTEGER,
    wl              CHAR(1),
    min             REAL,
    fgm             REAL,
    fga             REAL,
    fg_pct          REAL,
    fg3m            REAL,
    fg3a            REAL,
    fg3_pct         REAL,
    ftm             REAL,
    fta             REAL,
    ft_pct          REAL,
    oreb            REAL,
    dreb            REAL,
    reb             REAL,
    ast             REAL,
    tov             REAL,
    stl             REAL,
    blk             REAL,
    pf              REAL,
    pts             REAL,
    plus_minus      REAL,
    nba_fantasy_pts REAL,
    dd2             REAL,
    td3             REAL,
    PRIMARY KEY (player_id, game_id)
);

CREATE TABLE IF NOT EXISTS player_season (
    id                   SERIAL PRIMARY KEY,
    player_id            INTEGER REFERENCES player(player_id),
    season_id            INTEGER,
    team_id              INTEGER REFERENCES team(team_id),
    age                  INTEGER,
    player_height        VARCHAR(255),
    player_height_inches INTEGER,
    player_weight        VARCHAR(255),
    gp                   REAL,
    pts                  REAL,
    reb                  REAL,
    ast                  REAL,
    net_rating           REAL,
    oreb_pct             REAL,
    dreb_pct             REAL,
    usg_pct              REAL,
    ts_pct               REAL,
    ast_pct              REAL,
    UNIQUE (player_id, season_id)
);

CREATE TABLE IF NOT EXISTS player_general_traditional_total (
    id                   SERIAL PRIMARY KEY,
    player_id            INTEGER REFERENCES player(player_id),
    season_id            INTEGER,
    team_id              INTEGER,
    age                  INTEGER,
    gp                   INTEGER,
    w                    INTEGER,
    l                    INTEGER,
    w_pct                REAL,
    min                  REAL,
    fgm                  REAL,
    fga                  REAL,
    fg_pct               REAL,
    fg3m                 REAL,
    fg3a                 REAL,
    fg3lm                REAL,
    fg3_pct              REAL,
    ftm                  REAL,
    fta                  REAL,
    ft_pct               REAL,
    oreb                 REAL,
    dreb                 REAL,
    reb                  REAL,
    ast                  REAL,
    tov                  REAL,
    stl                  REAL,
    blk                  REAL,
    blka                 REAL,
    pf                   REAL,
    pfd                  REAL,
    pts                  REAL,
    plus_minus           REAL,
    nba_fantasy_pts      REAL,
    dd2                  REAL,
    td3                  REAL,
    gp_rank              INTEGER,
    w_rank               INTEGER,
    l_rank               INTEGER,
    w_pct_rank           INTEGER,
    min_rank             INTEGER,
    fgm_rank             INTEGER,
    fga_rank             INTEGER,
    fg_pct_rank          INTEGER,
    fg3m_rank            INTEGER,
    fg3a_rank            INTEGER,
    fg3_pct_rank         INTEGER,
    ftm_rank             INTEGER,
    fta_rank             INTEGER,
    ft_pct_rank          INTEGER,
    oreb_rank            INTEGER,
    dreb_rank            INTEGER,
    reb_rank             INTEGER,
    ast_rank             INTEGER,
    tov_rank             INTEGER,
    stl_rank             INTEGER,
    blk_rank             INTEGER,
    blka_rank            INTEGER,
    pf_rank              INTEGER,
    pfd_rank             INTEGER,
    pts_rank             INTEGER,
    plus_minus_rank      INTEGER,
    nba_fantasy_pts_rank INTEGER,
    dd2_rank             INTEGER,
    td3_rank             INTEGER,
    cfid                 INTEGER,
    cfparams             VARCHAR(255),
    UNIQUE (player_id, season_id)
);

CREATE TABLE IF NOT EXISTS player_career_totals (
    player_id               INTEGER PRIMARY KEY REFERENCES player(player_id),
    seasons_played          INTEGER,
    total_gp                INTEGER,
    total_w                 INTEGER,
    total_l                 INTEGER,
    career_w_pct            REAL,
    total_min               REAL,
    total_fgm               REAL,
    total_fga               REAL,
    career_fg_pct           REAL,
    total_fg3m              REAL,
    total_fg3a              REAL,
    career_fg3_pct          REAL,
    total_ftm               REAL,
    total_fta               REAL,
    career_ft_pct           REAL,
    total_oreb              REAL,
    total_dreb              REAL,
    total_reb               REAL,
    total_ast               REAL,
    total_tov               REAL,
    total_stl               REAL,
    total_blk               REAL,
    total_blka              REAL,
    total_pf                REAL,
    total_pfd               REAL,
    total_pts               REAL,
    total_plus_minus        REAL,
    total_nba_fantasy_pts   REAL,
    total_dd2               REAL,
    total_td3               REAL,
    career_ppg              REAL,
    career_rpg              REAL,
    career_apg              REAL,
    career_spg              REAL,
    career_bpg              REAL,
    career_topg             REAL,
    first_season            INTEGER,
    last_season             INTEGER
);
"""

# Load in this order (respects foreign keys)
TABLE_ORDER = [
    "event_message_type",
    "team",
    "player",
    "game",
    "player_game_log",
    "player_season",
    "player_general_traditional_total",
    "player_career_totals",
]

# Columns that must stay as strings — pandas would otherwise strip leading zeros
STRING_COLS = {"game_id"}

# Integer columns that may contain NaN (pandas promotes these to float64).
# At insert time these will be cast to int or None.
INT_NULLABLE_COLS = {
    "team_id", "player_id", "season_id", "event_msg_type_id",
    "event_msg_action_type", "period",
    "player1_id", "player1_team_id",
    "player2_id", "player2_team_id",
    "player3_id", "player3_team_id",
    "team_id_home_id", "team_id_away_id",
    "age", "player_height_inches",
    "gp", "w", "l",
    "gp_rank", "w_rank", "l_rank", "w_pct_rank", "min_rank",
    "fgm_rank", "fga_rank", "fg_pct_rank", "fg3m_rank", "fg3a_rank",
    "fg3_pct_rank", "ftm_rank", "fta_rank", "ft_pct_rank",
    "oreb_rank", "dreb_rank", "reb_rank", "ast_rank", "tov_rank",
    "stl_rank", "blk_rank", "blka_rank", "pf_rank", "pfd_rank",
    "pts_rank", "plus_minus_rank", "nba_fantasy_pts_rank",
    "dd2_rank", "td3_rank", "cfid",
    "seasons_played", "total_gp", "total_w", "total_l",
    "first_season", "last_season",
}


def is_null(v) -> bool:
    """Return True for any value that should be sent as SQL NULL."""
    if v is None:
        return True
    # Catches float NaN and numpy.nan
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except (TypeError, ValueError):
        pass
    # Catches whitespace-only strings like ' ' found in player_weight
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def clean_value(v, col: str):
    if is_null(v):
        return None
    if col in INT_NULLABLE_COLS:
        return int(v)
    return v


def create_database(conn):
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()
    print("Database schema created\n")


def load_csv_to_table(conn, csv_path: str, table_name: str):
    if not os.path.exists(csv_path):
        print(f"Skipping {table_name}: {csv_path} not found")
        return

    print(f"Loading {csv_path} into {table_name}...")

    # Keep game_id as a string so leading zeros are preserved
    df = pd.read_csv(csv_path, dtype={col: str for col in STRING_COLS})
    df.columns = df.columns.str.lower()

    cur = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    errors = 0
    for idx, row in df.iterrows():
        # Clean every value: NaN/blank -> None, integer cols -> int
        values = tuple(clean_value(v, col) for col, v in zip(df.columns, row))
        try:
            cur.execute("SAVEPOINT row_insert")
            cur.execute(query, values)
            cur.execute("RELEASE SAVEPOINT row_insert")
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT row_insert")
            errors += 1
            if errors <= 3:
                print(f"  Error on row {idx}: {e}")
                print(f"  Values: {dict(zip(df.columns, values))}")

    conn.commit()

    if errors > 0:
        print(f"  Inserted {len(df) - errors}/{len(df)} rows ({errors} errors)")
    else:
        print(f"  Inserted {len(df)} rows successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_dir", type=str, default="./96-24_data")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", type=str, default="nba")
    parser.add_argument("--user", type=str, default="postgres")
    parser.add_argument("--password", type=str, default="password")
    args = parser.parse_args()

    if not os.path.exists(args.csv_dir):
        print(f"Error: Directory {args.csv_dir} does not exist")
        return

    conn = psycopg2.connect(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )

    print(f"Connected to PostgreSQL: {args.dbname}@{args.host}\n")

    create_database(conn)

    for table in TABLE_ORDER:
        csv_path = os.path.join(args.csv_dir, f"{table}.csv")
        load_csv_to_table(conn, csv_path, table)

    conn.close()
    print("\nDone. Database ready.")
    print(f"\nVerify with: psql -h {args.host} -U {args.user} -d {args.dbname}")


if __name__ == "__main__":
    main()