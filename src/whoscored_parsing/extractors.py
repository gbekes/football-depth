import re

import pandas as pd

from ..raw_tables import (wh_lineup_table, wh_match_table, wh_player_table,
                          wh_season_table, wh_team_table)
from .t2 import DictValues, RecordExtractor


def format_match_df(match_df_base):
    score_rex = re.compile(r"(\d+) : (\d+)")
    end_times = ["ht", "ft", "et", "pk"]

    return (
        match_df_base.assign(
            home_progress=lambda df: df["score"].str.startswith("*"),
            away_progress=lambda df: df["score"].str.endswith("*"),
            datetime=lambda df: df["startTime"].pipe(pd.to_datetime),
        )
        .pipe(
            lambda df: pd.concat(
                [
                    df[f"{time}Score"]
                    .str.extract(score_rex)
                    .astype(float)
                    .rename(columns={0: f"home_goals_{time}", 1: f"away_goals_{time}"})
                    .fillna(-1)  # TODO: when pp issue fixed
                    for time in end_times
                ]
                + [df],
                axis=1,
            )
        )
        .drop(["htScore", "ftScore", "etScore", "pkScore", "startTime"], axis=1)
        .rename(columns=str.lower)
    )


def format_lineups_df(lineup_df_base):
    return lineup_df_base.rename(
        columns={"shirtNo": "shirt_no", "playerId": "wh_player_id"}
    )


def format_team_df(team_df_base):
    return (
        team_df_base.reset_index()
        .rename(columns={"countryName": "country", "teamId": "team_id"})
        .set_index("team_id")
    )


table_extractor_pairs = (
    (
        wh_match_table,
        RecordExtractor(
            [
                "wh_match_id",
                "score",
                "htScore",
                "ftScore",
                "etScore",
                "pkScore",
                "startTime",
                "wh_season_id",
                "attendance",
                "venueName",
                "weatherCode",
                ("referee", "officialId"),
                (DictValues(["home", "away"]), DictValues(["teamId", "managerName"])),
            ],
            id_key="wh_match_id",
        ),
        format_match_df,
    ),
    (
        wh_lineup_table,
        RecordExtractor(
            [
                "wh_match_id",
                (
                    DictValues(["home", "away"]),
                    "players",
                    ...,
                    DictValues(["playerId", "position", "shirtNo", "field", "age"]),
                ),
            ]
        ),
        format_lineups_df,
    ),
    (
        wh_season_table,
        RecordExtractor(
            ["area", "comp", "season", "wh_season_id"], id_key="wh_season_id"
        ),
        lambda x: x,
    ),
    (
        wh_team_table,
        RecordExtractor(
            key_branches=[
                (
                    DictValues(["home", "away"], unstack=True),
                    DictValues(["teamId", "name", "countryName"]),
                )
            ],
            id_key="teamId",
        ),
        format_team_df,
    ),
    (
        wh_player_table,
        RecordExtractor(
            key_branches=[
                (
                    DictValues(["home", "away"]),
                    "players",
                    ...,
                    DictValues(["playerId", "name", "height"]),
                )
            ],
            id_key="playerId",
        ),
        lambda df: df.reset_index()
        .rename(columns={"playerId": "wh_player_id"})
        .set_index("wh_player_id"),
    ),
)
