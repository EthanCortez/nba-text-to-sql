#!/usr/bin/env python3
"""
NBA Database Builder — PostgreSQL Edition (FULL, game_id fixed)
"""

import argparse
import logging
import os
import sys
import time
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from nba_api.stats.endpoints import (
    LeagueDashPlayerBioStats,
    LeagueDashPlayerStats,
    LeagueGameLog,
    PlayByPlayV2,
    PlayerGameLog,
)
from nba_api.stats.static import teams as static_teams


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

REQUEST_DELAY = 0.65
DEFAULT_SEASON = "2023-24"

EVENT_MESSAGE_TYPES = [
    (1,  "Made Shot"),
    (2,  "Missed Shot"),
    (3,  "Free Throw"),
    (4,  "Rebound"),
    (5,  "Turnover"),
    (6,  "Foul"),
    (7,  "Violation"),
    (8,  "Substitution"),
    (9,  "Timeout"),
    (10, "Jump Ball"),
    (11, "Ejection"),
    (12, "Start Period"),
    (13, "End Period"),
    (18, "Instant Replay"),
    (20, "Stoppage: Out-of-Bounds"),
]


# ══════════════════════════════════════════════
# DATABASE CONNECTION
# ══════════════════════════════════════════════

def get_connection(host: str, port: int, dbname: str,
                   user: str, password: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    conn.autocommit = False
    log.info(f"Connected to PostgreSQL: {user}@{host}:{port}/{dbname}")
    return conn


# ══════════════════════════════════════════════
# TABLE CREATION
# ══════════════════════════════════════════════

def create_tables(conn: psycopg2.extensions.connection) -> None:
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS event_message_type (
            id     INTEGER PRIMARY KEY,
            string VARCHAR(255)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS team (
            team_id       INTEGER PRIMARY KEY,
            abbreviation  VARCHAR(255),
            nickname      VARCHAR(255),
            year_founded  VARCHAR(255),
            city          VARCHAR(255)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS player (
            player_id    INTEGER PRIMARY KEY,
            player_name  VARCHAR(255),
            college      VARCHAR(255),
            country      VARCHAR(255),
            draft_year   VARCHAR(255),
            draft_round  VARCHAR(255),
            draft_number VARCHAR(255)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS game (
            game_id          TEXT PRIMARY KEY,
            team_id_home_id  INTEGER REFERENCES team(team_id),
            team_id_away_id  INTEGER REFERENCES team(team_id),
            season_id        INTEGER,
            date             DATE
        )
    """)

    cur.execute("""
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
        )
    """)

    cur.execute("""
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
        )
    """)

    conn.commit()
    log.info("✅ All tables created.")


# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════

def safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None and val != "" else None
    except:
        return None

def safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None and val != "" else None
    except:
        return None

def nba_api_call(func, *args, **kwargs):
    for attempt in range(4):
        try:
            time.sleep(REQUEST_DELAY)
            return func(*args, **kwargs)
        except Exception as exc:
            wait = REQUEST_DELAY * (2 ** attempt)
            log.warning(f"API error: {exc}. Retrying in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed after 4 attempts calling {func.__name__}")


# ══════════════════════════════════════════════
# LOADERS
# ══════════════════════════════════════════════

def load_event_message_types(conn):
    cur = conn.cursor()
    execute_values(
        cur,
        "INSERT INTO event_message_type (id, string) VALUES %s ON CONFLICT DO NOTHING",
        EVENT_MESSAGE_TYPES,
    )
    conn.commit()
    log.info(f"  ✅ {len(EVENT_MESSAGE_TYPES)} event_message_type rows inserted.")


def load_teams(conn):
    all_teams = static_teams.get_teams()
    rows = [
        (t["id"], t.get("abbreviation"), t.get("nickname"),
         str(t.get("year_founded", "")), t.get("city"))
        for t in all_teams
    ]
    cur = conn.cursor()
    execute_values(
        cur,
        "INSERT INTO team (team_id, abbreviation, nickname, year_founded, city) VALUES %s ON CONFLICT DO NOTHING",
        rows,
    )
    conn.commit()
    log.info(f"  ✅ {len(rows)} teams inserted.")


def load_players(conn, season):
    raw = nba_api_call(LeagueDashPlayerBioStats, season=season, per_mode_simple="PerGame")
    df = raw.get_data_frames()[0]

    rows = [
        (
            safe_int(r.get("PLAYER_ID")),
            r.get("PLAYER_NAME"),
            r.get("COLLEGE"),
            r.get("COUNTRY"),
            str(r.get("DRAFT_YEAR", "")) or None,
            str(r.get("DRAFT_ROUND", "")) or None,
            str(r.get("DRAFT_NUMBER", "")) or None,
        )
        for _, r in df.iterrows()
    ]

    cur = conn.cursor()
    execute_values(
        cur,
        """INSERT INTO player (player_id, player_name, college, country,
               draft_year, draft_round, draft_number)
           VALUES %s ON CONFLICT (player_id) DO NOTHING""",
        rows,
    )
    conn.commit()
    log.info(f"  ✅ {len(rows)} players inserted.")


def load_games(conn, season):
    raw = nba_api_call(LeagueGameLog,
                       season=season,
                       season_type_all_star="Regular Season",
                       direction="ASC")
    df = raw.get_data_frames()[0]

    rows = []
    seen = set()
    for _, r in df.iterrows():
        gid = r["GAME_ID"]  # keep as string
        if gid in seen:
            continue
        seen.add(gid)
        rows.append((
            gid,
            safe_int(r.get("TEAM_ID")),
            None,
            safe_int(r.get("SEASON_ID")),
            r.get("GAME_DATE")
        ))

    cur = conn.cursor()
    execute_values(
        cur,
        "INSERT INTO game (game_id, team_id_home_id, team_id_away_id, season_id, date) VALUES %s ON CONFLICT DO NOTHING",
        rows,
    )
    conn.commit()
    log.info(f"  ✅ {len(rows)} games inserted.")
    return [r[0] for r in rows]


def load_player_game_logs(conn, season):
    cur = conn.cursor()
    cur.execute("SELECT player_id FROM player")
    player_ids = [r[0] for r in cur.fetchall()]
    inserted_total = 0

    for pid in player_ids:
        raw = nba_api_call(PlayerGameLog, player_id=pid, season=season)
        df = raw.get_data_frames()[0]
        if df.empty:
            continue
        rows = [
            (
                pid,
                r["Game_ID"],  # keep as string
                safe_int(r.get("TEAM_ID")),
                safe_int(r.get("SEASON_ID")),
                r.get("WL"),
                safe_float(r.get("MIN")),
                safe_float(r.get("FGM")),
                safe_float(r.get("FGA")),
                safe_float(r.get("FG_PCT")),
                safe_float(r.get("FG3M")),
                safe_float(r.get("FG3A")),
                safe_float(r.get("FG3_PCT")),
                safe_float(r.get("FTM")),
                safe_float(r.get("FTA")),
                safe_float(r.get("FT_PCT")),
                safe_float(r.get("OREB")),
                safe_float(r.get("DREB")),
                safe_float(r.get("REB")),
                safe_float(r.get("AST")),
                safe_float(r.get("TOV")),
                safe_float(r.get("STL")),
                safe_float(r.get("BLK")),
                safe_float(r.get("PF")),
                safe_float(r.get("PTS")),
                safe_float(r.get("PLUS_MINUS")),
                safe_float(r.get("NBA_FANTASY_PTS")),
                safe_float(r.get("DD2")),
                safe_float(r.get("TD3")),
            )
            for _, r in df.iterrows()
        ]
        execute_values(
            cur,
            """INSERT INTO player_game_log
               (player_id, game_id, team_id, season_id, wl,
                min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                ftm, fta, ft_pct, oreb, dreb, reb, ast, tov,
                stl, blk, pf, pts, plus_minus, nba_fantasy_pts, dd2, td3)
               VALUES %s ON CONFLICT (player_id, game_id) DO NOTHING""",
            rows,
        )
        conn.commit()
        inserted_total += len(rows)
    log.info(f"  ✅ {inserted_total} player_game_log rows inserted.")


def load_play_by_play(conn, game_ids: List[str]):
    cur = conn.cursor()
    total = 0
    for gid in game_ids:
        raw = nba_api_call(PlayByPlayV2, game_id=gid)
        df = raw.get_data_frames()[0]
        if df.empty:
            continue
        rows = []
        for _, r in df.iterrows():
            rows.append((
                gid,
                safe_int(r.get("EVENTNUM")),
                safe_int(r.get("EVENTMSGTYPE")),
                safe_int(r.get("EVENTMSGACTIONTYPE")),
                safe_int(r.get("PERIOD")),
                r.get("WCTIMESTRING"),
                r.get("HOMEDESCRIPTION"),
                r.get("NEUTRALDESCRIPTION"),
                r.get("VISITORDESCRIPTION"),
                r.get("SCORE"),
                r.get("SCOREMARGIN"),
                safe_int(r.get("PLAYER1_ID")),
                safe_int(r.get("PLAYER1_TEAM_ID")),
                safe_int(r.get("PLAYER2_ID")),
                safe_int(r.get("PLAYER2_TEAM_ID")),
                safe_int(r.get("PLAYER3_ID")),
                safe_int(r.get("PLAYER3_TEAM_ID")),
            ))
        execute_values(
            cur,
            """INSERT INTO play_by_play
               (game_id, event_num, event_msg_type_id, event_msg_action_type,
                period, wc_time, home_description, neutral_description,
                visitor_description, score, score_margin,
                player1_id, player1_team_id, player2_id, player2_team_id,
                player3_id, player3_team_id)
               VALUES %s ON CONFLICT DO NOTHING""",
            rows,
        )
        conn.commit()
        total += len(rows)
    log.info(f"  ✅ {total} play_by_play rows inserted.")


# ══════════════════════════════════════════════
# MAIN DATABASE BUILD
# ══════════════════════════════════════════════

def build_database(host, port, dbname, user, password, seasons, load_pbp=True):
    conn = get_connection(host, port, dbname, user, password)
    create_tables(conn)
    load_event_message_types(conn)
    load_teams(conn)

    for season in seasons:
        log.info(f"🏀 Loading season {season}")
        load_players(conn, season)
        game_ids = load_games(conn, season)
        load_player_game_logs(conn, season)
        if load_pbp:
            load_play_by_play(conn, game_ids)

    conn.close()
    log.info("🎉 Database build complete.")


# ──────────────────────────────────────────────
# MAIN FUNCTION
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build an NBA PostgreSQL database from the nba_api."
    )
    # Connection flags (fall back to env vars)
    parser.add_argument("--host",     default=os.getenv("NBA_DB_HOST",     "localhost"))
    parser.add_argument("--port",     default=int(os.getenv("NBA_DB_PORT", "5432")), type=int)
    parser.add_argument("--dbname",   default=os.getenv("NBA_DB_NAME",     "nba"))
    parser.add_argument("--user",     default=os.getenv("NBA_DB_USER",     "postgres"))
    parser.add_argument("--password", default=os.getenv("NBA_DB_PASSWORD", "password"))

    # Season flags
    parser.add_argument("--season",  default=None,
                        help="Single season, e.g. 2023-24")
    parser.add_argument("--seasons", nargs="+", default=None,
                        help="Multiple seasons, e.g. --seasons 2022-23 2023-24")
    parser.add_argument("--no-pbp",  action="store_true",
                        help="Skip play-by-play (much faster)")
    args = parser.parse_args()

    seasons = args.seasons or ([args.season] if args.season else [DEFAULT_SEASON])

    build_database(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        seasons=seasons,
        load_pbp=not args.no_pbp,
    )


if __name__ == "__main__":
    main()
