#!/usr/bin/env python3
"""
ETL for MLB_Data_2025 take-home.

- Reads games.csv, linescores.csv, runners.csv from date-stamped subfolders
- Deduplicates repeated extracts
- Computes derived columns for linescore and runner_play tables
- Loads into PostgreSQL tables created via sql/create_tables.sql
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "MLB_Data_2025"

# ----------------------------
# IO helpers
# ----------------------------
def load_all_csvs(data_dir: Path, filename: str) -> pd.DataFrame:
    """
    Recursively find all occurrences of `filename` under `data_dir` and concatenate.
    Adds source_folder_date for dedupe preference.
    Expected structure: <date>/.../<filename>
    """
    dfs: List[pd.DataFrame] = []

    for path in data_dir.rglob(filename):
        try:
            folder_date = path.parent.parent.name
        except Exception:
            folder_date = path.parent.name

        df = pd.read_csv(path)
        df["source_folder_date"] = folder_date
        dfs.append(df)

    if not dfs:
        raise FileNotFoundError(f"No files named {filename} found under {data_dir}")

    return pd.concat(dfs, ignore_index=True)


def to_sql_append(engine: Engine, table: str, df: pd.DataFrame, chunksize: int = 5000) -> None:
    """Append a DataFrame to an existing table, preserving schema/constraints."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=chunksize)


def truncate_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE runner_play, linescore, game;"))


# ----------------------------
# Transform: games
# ----------------------------
def process_games(df_games_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the most recent schedule row per gamePk (based on source_folder_date).
    """
    df = df_games_raw.copy()
    df = df.sort_values(["gamePk", "source_folder_date"], ascending=[True, False])
    df = df.drop_duplicates(subset=["gamePk"], keep="first")
    df = df.drop(columns=["source_folder_date"], errors="ignore")
    return df


# ----------------------------
# Transform: linescores
# ----------------------------
def process_linescores(df_linescores_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate (gamePk, inning, half) across dates.
    Compute:
      - battingteam_score: batting team runs at start of half inning
      - battingteam_score_diff: batting - fielding at start of half inning
    """
    df = df_linescores_raw.copy()

    # Deduplicate repeated extracts (prefer most recent folder date)
    df = df.sort_values(["gamePk", "inning", "half", "source_folder_date"], ascending=[True, True, True, False])
    df = df.drop_duplicates(subset=["gamePk", "inning", "half"], keep="first")

    # Runs may be NaN in the source
    df["runs"] = df["runs"].fillna(0).astype(int)

    # Ensure chronological order for correct running totals
    df = df.sort_values(["gamePk", "inning", "half"]).copy()

    # Cumulative runs for each batting team and total, then shift to get "start of half inning"
    df["cum_runs_batting"] = df.groupby(["gamePk", "battingteamid"])["runs"].cumsum()
    df["cum_runs_total"] = df.groupby("gamePk")["runs"].cumsum()

    df["battingteam_score"] = (
        df.groupby(["gamePk", "battingteamid"])["cum_runs_batting"]
        .shift(1)
        .fillna(0)
        .astype(int)
    )
    df["total_score"] = (
        df.groupby("gamePk")["cum_runs_total"]
        .shift(1)
        .fillna(0)
        .astype(int)
    )

    df["battingteam_score_diff"] = df["battingteam_score"] - (df["total_score"] - df["battingteam_score"])

    df = df.drop(columns=["cum_runs_batting", "cum_runs_total", "total_score", "source_folder_date"], errors="ignore")
    return df


# ----------------------------
# Transform: runner_play
# ----------------------------
BASE_MAP_REPLACE = {
    "score": "HM",
    "4B": "HM",
}


def uniq_join_keep_dups(s: pd.Series) -> Optional[str]:
    """
    Join unique labels observed during the play into a comma-separated string.
    This is used to avoid losing information when multiple movement reasons/eventtypes
    occur across segments.
    """
    vals = [v for v in s.dropna().astype(str).tolist() if v != ""]
    if not vals:
        return None
    vals = sorted(set(vals))
    return ",".join(vals)


def calculate_reached_base(group: pd.DataFrame) -> str:
    """
    reachedbase: last base reached safely on the play; if never safe, startbase.
    """
    safe_segments = group[group["is_out"] == False]
    if not safe_segments.empty:
        return safe_segments["endbase"].iloc[-1]
    return group["startbase"].iloc[0]


def process_runner_play(df_runners_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate runner movement segments and aggregate into one row per
    (gamepk, atbatindex, playindex, runnerid).
    """
    df = df_runners_raw.copy()

    # Deduplicate repeated extracts and identical segments
    df = df.sort_values(
        ["gamePk", "atBatIndex", "playIndex", "runnerid", "start", "end", "source_folder_date"],
        ascending=[True, True, True, True, True, True, False],
    )
    df = df.drop_duplicates(subset=["gamePk", "atBatIndex", "playIndex", "runnerid", "start", "end"], keep="first")

    # Rename to match schema intent
    df = df.rename(
        columns={
            "originBase": "startbase",
            "end": "endbase",
            "isOut": "is_out",
        }
    )

    # Lowercase columns to simplify downstream mapping
    df.columns = [c.lower() for c in df.columns]

    # Fill empty base values
    for col in ["startbase", "endbase", "start"]:
        if col in df.columns:
            df[col] = df[col].fillna("B")

    # Standardize home representation
    df["endbase"] = df["endbase"].replace(BASE_MAP_REPLACE)
    if "outbase" in df.columns:
        df["outbase"] = df["outbase"].replace(BASE_MAP_REPLACE)

    # Ensure chronological order for first/last aggregates
    df = df.sort_values(["gamepk", "atbatindex", "playindex"]).copy()

    grouped = df.groupby(["gamepk", "atbatindex", "playindex", "runnerid"], sort=False)

    out = grouped.agg(
        startbase=("startbase", "first"),
        endbase=("endbase", "last"),
        runnerfullname=("runnerfullname", "first"),
        reachedbase=("endbase", lambda x: calculate_reached_base(df.loc[x.index])),
        eventtype=("eventtype", uniq_join_keep_dups),
        movementreason=("movementreason", uniq_join_keep_dups),
        is_out=("is_out", "max"),
        playid=("playid", "first"),
    ).reset_index()

    # Derived metrics
    out["is_risp"] = out["startbase"].isin(["2B", "3B"])

    out["is_firsttothird"] = (
        (out["startbase"] == "1B")
        & (out["reachedbase"] == "3B")
        & (~out["eventtype"].str.contains("home_run", na=False))
        & (out["is_out"] == False)
    )

    out["is_secondtohome"] = (
        (out["startbase"] == "2B")
        & (out["reachedbase"] == "HM")
        & (~out["eventtype"].str.contains("home_run", na=False))
        & (out["is_out"] == False)
    )

    return out


# ----------------------------
# Main
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load MLB_Data_2025 into Postgres and compute derived tables.")

    p.add_argument(
        "--db-uri",
        type=str,
        required=True,
        help="Postgres SQLAlchemy URI, e.g. postgresql+psycopg2://user:pass@localhost:5432/mlb_data_2025",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    data_dir = DATA_DIR
    if not data_dir.exists():
        raise FileNotFoundError(f"--data-dir does not exist: {data_dir}")

    engine = create_engine(args.db_uri)

    LOG.info("Loading raw CSVs...")
    df_games_raw = load_all_csvs(data_dir, "games.csv")
    df_linescores_raw = load_all_csvs(data_dir, "linescores.csv")
    df_runners_raw = load_all_csvs(data_dir, "runners.csv")

    LOG.info("Processing games...")
    df_games = process_games(df_games_raw)

    LOG.info("Processing linescores...")
    df_linescores = process_linescores(df_linescores_raw)

    LOG.info("Processing runner_play...")
    df_runner_play = process_runner_play(df_runners_raw)

    LOG.info("Truncating tables...")
    truncate_tables(engine)

    LOG.info("Loading game...")
    to_sql_append(engine, "game", df_games)

    LOG.info("Loading linescore...")
    to_sql_append(engine, "linescore", df_linescores)

    LOG.info("Loading runner_play...")
    to_sql_append(engine, "runner_play", df_runner_play)

    LOG.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
