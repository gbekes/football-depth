import datazimmer as dz
from parquetranger import TableRepo


class TmRawCols:
    season_id = "tm_season_id"
    team_id = "tm_team_id"
    player_id = "tm_player_id"
    match_id = "tm_match_id"

    date = "date"


class TmLineupCols:
    side = "side"
    starter = "starter"
    country = "country"
    player_name = "name"
    player_id = "tm_id"
    match_id = "match_id"


class TmSeasonCols:
    season_year_id = "season_id"
    tm_comp_type = "base"
    name = "name"
    comp_id = "comp_id"
    country = "country"


class TmMatchCols:
    home_team_id = "home-tm_id"
    away_team_id = "away-tm_id"


class WhRawCols:
    match_id = "wh_match_id"
    player_id = "wh_player_id"
    season_id = "wh_season_id"
    comp_type = "comp_type"
    local_event_id = "eventid"
    global_id = "id"
    datetime = "datetime"

    outcometype = "outcometype"
    event_type = "type"
    is_touch = "istouch"
    event_side = "event_side"
    period = "period"
    angle = "angle"

    x = "x"
    y = "y"
    passendx = "passendx"
    passendy = "passendy"

    length = "length"

    minute = "minute"
    second = "second"
    expandedminute = "expandedminute"

    # shot
    goalmouthz = "goalmouthz"
    goaulmouthy = "goalmouthy"

    # pass kind
    throwin = "throwin"
    freekicktaken = "freekicktaken"
    goalkick = "goalkick"
    cornertaken = "cornertaken"
    keeperthrow = "keeperthrow"

    # pass style
    cross = "cross"
    layoff = "layoff"

    # pass qualifier
    longball = "longball"
    headpass = "headpass"
    chipped = "chipped"
    indirectfreekicktaken = "indirectfreekicktaken"
    throughball = "throughball"

    # other table ids
    team_id = "teamid"

    name = "name"


class WhMatchCols:
    home_team_id = "home_teamid"
    away_team_id = "away_teamid"


setpiece_cols = [
    WhRawCols.throwin,
    WhRawCols.freekicktaken,
    WhRawCols.cornertaken,
    WhRawCols.goalkick,
]


wh_lineup_table = TableRepo(dz.get_raw_data_path("lineup"))
wh_match_table = TableRepo(dz.get_raw_data_path("match"))
wh_player_table = TableRepo(dz.get_raw_data_path("player"))
wh_season_table = TableRepo(dz.get_raw_data_path("season"))
wh_team_table = TableRepo(dz.get_raw_data_path("team"))


tm_countries_table = TableRepo(dz.get_raw_data_path("tm_countries"))
tm_match_info_table = TableRepo(dz.get_raw_data_path("tm_match_info"))
tm_match_lineups_table = TableRepo(dz.get_raw_data_path("tm_match_lineups"))
tm_player_info_table = TableRepo(dz.get_raw_data_path("tm_player_info"))
tm_player_transfers_table = TableRepo(dz.get_raw_data_path("tm_player_transfers"))
tm_player_values_table = TableRepo(dz.get_raw_data_path("tm_player_values"))
tm_season_info_table = TableRepo(dz.get_raw_data_path("tm_season_info"))
tm_team_info_table = TableRepo(dz.get_raw_data_path("tm_team_info"))
tm_team_relations_table = TableRepo(dz.get_raw_data_path("tm_team_relations"))


event_table = TableRepo(dz.get_raw_data_path("event"), group_cols=[WhRawCols.season_id])
formation_table = TableRepo(dz.get_raw_data_path("formation"))
formation_use_table = TableRepo(dz.get_raw_data_path("formation_use"))


extended_event_table = TableRepo(
    dz.get_raw_data_path("extended_event"), group_cols=[WhRawCols.season_id]
)
pass_table = TableRepo(dz.get_raw_data_path("passes"), group_cols=[WhRawCols.season_id])


woodwork_phase_table = TableRepo(dz.get_raw_data_path("woodwork_phase"))
