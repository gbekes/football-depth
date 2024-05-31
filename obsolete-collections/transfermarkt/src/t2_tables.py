from parquetranger import TableRepo


def get_table(name: str):
    return TableRepo(f"out-tables/{name}")


countries = get_table("countries")
season_info = get_table("season_info")

match_info = get_table("match_info")
match_lineups = get_table("match_lineups")


player_info = get_table("player_info")
player_transfers = get_table("player_transfers")
player_values = get_table("player_values")

team_info = get_table("team_info")
team_relations = get_table("team_relations")
