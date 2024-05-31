import datetime as dt

import pandas as pd

from .handlers import TmCompSeasonFinder, TmPlayer, TmSeasonMatchFinder, TmTeam
from .t2_tables import *

identified_tm_comps = {
    "FR1": "2009",
    "FRC": "2012",
    "GB1": "2009",
    "PO1": "2012",
    "ES1": "2009",
    "FAC": "2012",
    "NL1": "2012",
    "PONL": "2012",  # does not display all
    "NO1": "2016",
    "CIT": "2012",
    "L1": "2009",
    "L2": "2015",
    "GB2": "2013",
    "CDR": "2012",
    "AR1N": "2012",
    "IT1": "2009",
    "BRA1": "2013",
    "MLS1": "2013",
    "EL": "2012",
    "CL": "2009",
    "RU1": "2013",
    "TR1": "2014",
}

# if a player plays, set expiry on him

comp_links = [
    f"https://www.transfermarkt.com/x/startseite/wettbewerb/{tm_id}"
    for tm_id in identified_tm_comps.keys()
]


def update(daypast=200):

    match_df = match_info.get_full_df().assign(
        is_recent=lambda df: (
            df["date"].pipe(pd.to_datetime) - dt.datetime.now()
        ).dt.days
        > -daypast
    )

    recent_seasons = [
        f"https://www.transfermarkt.com/x/gesamtspielplan/{base}/{comp_id}/saison_id/{sid}"
        for base, comp_id, sid in match_df.groupby(["comp_id", "season_id"])[
            "is_recent"
        ]
        .any()
        .loc[lambda s: s]
        .reset_index()
        .merge(season_info.get_full_df(), how="inner")
        .loc[:, ["base", "comp_id", "season_id"]]
        .values
    ]

    player_df = player_info.get_full_df()

    recent_players = (
        match_lineups.get_full_df()
        .loc[
            lambda df: df["match_id"].isin(match_df.loc[match_df["is_recent"]].index)
            | ~df["tm_id"].isin(player_df.index),
            "tm_id",
        ]
        .drop_duplicates()
        .apply(lambda pid: f"https://www.transfermarkt.com/x/profil/spieler/{pid}")
        .tolist()
    )

    new_urls = {}

    for links, handler in zip(
        [comp_links, recent_seasons, recent_players],
        [TmCompSeasonFinder, TmSeasonMatchFinder, TmPlayer],
    ):
        print(handler.__name__)
        print(len(links))
        new_urls[handler] = links
    return new_urls


def main():

    season_df = season_info.get_full_df()
    match_df = match_info.get_full_df()

    season_links = (
        [
            f"https://www.transfermarkt.com/x/gesamtspielplan/{base}/{comp_id}/saison_id/{sid}"
            for base, comp_id, sid in season_df.loc[
                lambda df: pd.DataFrame(identified_tm_comps.items())
                .set_index(0)
                .loc[:, 1]
                .reindex(df["comp_id"])
                .values
                <= df["season_id"],
                ["base", "comp_id", "season_id"],
            ].values
        ]
        if season_df.shape[0]
        else []
    )

    lineup_df = match_lineups.get_full_df()

    player_links = (
        [
            f"https://www.transfermarkt.com/x/profil/spieler/{pid}"
            for pid in lineup_df["tm_id"].unique()
        ]
        if lineup_df.shape[0]
        else []
    )

    team_links = [
        f"https://www.transfermarkt.com/x/datenfakten/verein/{tid}"
        for tid in set(
            [
                *match_df["home-tm_id"],
                *match_df["away-tm_id"],
                *player_transfers.get_full_df()
                .loc[:, ["left", "joined"]]
                .unstack()
                .unique(),
            ]
        )
    ]

    return dict(
        zip(
            [comp_links, season_links, player_links, team_links],
            [TmCompSeasonFinder, TmSeasonMatchFinder, TmPlayer, TmTeam],
        )
    )
