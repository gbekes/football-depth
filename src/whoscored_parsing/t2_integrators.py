import re

import pandas as pd
from unidecode import unidecode

url_keys = ["wh_season_id", "wh_match_id"]
maybe_drop = [
    "playerposition",
    "jerseynumber",
    "involvedplayers",
    "teamplayerformation",
]

str_vals = (
    [
        "period",
        "type",
        "outcometype",
        "event_side",
        "zone",
        # "wh_match_id",
    ]
    + maybe_drop
    + url_keys
)
types_to_drop = []  # "FormationChange", "FormationSet"]
quals_to_drop = []


def get_event_records(match_record, url_dic):
    return [
        {
            **parse_event_dic(
                ev, match_record["home"]["teamId"], match_record["away"]["teamId"]
            ),
            **{k: v for k, v in url_dic.items() if k.startswith("wh")},
        }
        for ev in match_record.pop("events", [])
    ]


def fix_event_df(ev_recs):
    return (
        pd.DataFrame(ev_recs)
        .loc[:, lambda df: ~df.rename(columns=str.lower).columns.duplicated()]
        .rename(columns=str.lower)
        .rename(columns={"playerid": "wh_player_id"})
        .assign(
            foul=lambda df: df["foul"].replace("243", True)
            if ("foul" in df.columns)
            else float("nan")
        )
        .loc[
            lambda df: ~df["type"].isin(types_to_drop)
            if ("type" in df.columns)
            else slice(None),
            :,
        ]
        .drop(quals_to_drop, axis=1, errors="ignore")
        .pipe(
            lambda df: pd.concat(
                [
                    df.reindex(str_vals, axis=1),
                    df.drop(str_vals, axis=1, errors="ignore").apply(parse_to_num),
                ],
                axis=1,
            )
        )
    )


def parse_to_num(s):
    try:
        return s.astype(float)
    except ValueError as e:
        print("Value Err:")
        print(e)
        return s.str.replace(",", ".").astype(float)


def handle_formations(match_record):
    formation_kinds = []
    formation_sets = []
    for side in ["home", "away"]:
        formations = match_record[side]["formations"]
        for f_info in formations:
            formation_record = {
                "captain": f_info.get("captainPlayerId"),
                "period": f_info["period"],
                "start_minute": f_info["startMinuteExpanded"],
                "end_minute": f_info["endMinuteExpanded"],
                "formation_name": f_info["formationName"],
                "formation_id": f_info["formationId"],
                "side": side,
                "wh_match_id": match_record["wh_match_id"],
            }
            for slot_id, player_id in zip(
                f_info["formationSlots"], f_info["playerIds"]
            ):
                if slot_id > 0:
                    formation_record[f"slot_{slot_id}"] = player_id
            formation_sets.append(formation_record)

            spec_form_dic = {
                "name": f_info["formationName"],
                "formation_id": f_info["formationId"],
            }
            for sid, posdic in zip(range(1, 12), f_info["formationPositions"]):
                for poskey, posval in posdic.items():
                    spec_form_dic[f"slot_{sid}_{poskey}"] = posval
            formation_kinds.append(spec_form_dic)
    return formation_sets, formation_kinds


def parse_url(url):
    urlinfo_list = url.split("/")[-1].split("-")
    area = urlinfo_list[0]
    out = {"area": area}
    id_iter = iter(urlinfo_list[1:])
    comp_ids = []
    season_ids = []
    for e in id_iter:
        if re.match(r"\d{4}", e):
            season_ids.append(e)
            break
        comp_ids.append(unidecode(e))

    season_end = next(id_iter)
    if re.match(r"\d{4}", season_end) and int(season_end) > 1990:
        season_ids.append(season_end)
    else:
        season_ids.append(e)

    out["comp"] = "-".join(comp_ids)
    out["season"] = "-".join(season_ids)

    return {"wh_season_id": get_season_id(out), "wh_match_id": url.split("/")[4], **out}


def get_season_id(parsed_url: dict):
    return "_".join(parsed_url.values()).lower()


def parse_event_dic(ev, home_id, away_id):
    for k in ["period", "type", "outcomeType"]:
        _d = ev.get(k, {})
        ev[k] = _d.get("displayName")
    for k in ["endX", "endY", "satisfiedEventsTypes"]:
        try:
            ev.pop(k)
        except KeyError:
            pass
    try:
        quals = ev.pop("qualifiers")
    except KeyError:
        quals = []

    try:
        _c = ev.pop("cardType")
        quals.append({"type": _c})
    except KeyError:
        pass

    for q in quals:
        ev[q["type"]["displayName"]] = q.get("value", True)
    side = ev.get("teamId", 0)
    if side == home_id:
        ev["event_side"] = "home"
    elif side == away_id:
        ev["event_side"] = "away"
    return ev
