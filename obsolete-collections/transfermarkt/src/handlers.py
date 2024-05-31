import json
import re

import aswan
from aswan import RequestSoupHandler
from bs4 import BeautifulSoup


class TmCountries(RequestSoupHandler):

    test_urls = [
        f"https://www.transfermarkt.com/wettbewerbe/{c}" for c in ["europa", "amerika"]
    ]
    starter_urls = test_urls

    def parse_soup(self, soup):
        out = []
        for elem in soup.find_all("area", shape="rect"):
            out.append(
                {
                    "country_id": elem["href"].split("/")[-1],
                    "name": elem["title"],
                    "continent": self._url.split("/")[-1],
                }
            )
        return out


class TmCompSeasonFinder(RequestSoupHandler):

    default_expiration = aswan.ONE_WEEK * 10
    test_urls = [
        f"https://www.transfermarkt.com/x/startseite/wettbewerb/{c}"
        for c in ["L1", "CL"]
    ]

    def parse_soup(self, soup: "BeautifulSoup"):
        tm_id = self._url.split("/")[-1]
        act_url = soup.find("meta", {"property": "og:url"})["content"]
        pokalbase = act_url.split("/")[-2]
        base_rec = {
            "base": pokalbase,
            "name": soup.find("h1").text.strip(),
            "comp_id": tm_id,
            **parse_qs_box(soup),
        }
        season_recs = []
        for season in soup.find("select", {"name": "saison_id"}).find_all("option"):
            season_id = season["value"]
            season_recs.append(
                {
                    "season_id": season_id,
                    **base_rec,
                }
            )
        return season_recs


class TmSeasonMatchFinder(RequestSoupHandler):
    test_urls = [
        "https://www.transfermarkt.com/x/gesamtspielplan/wettbewerb/GB1/saison_id/2018"
    ]

    def parse_soup(self, soup: "BeautifulSoup"):
        league_records = []
        lineup_links = []
        for result_box in soup.find_all("a", class_="ergebnis-link"):
            match_record = parse_result_box(result_box)
            league_records.append(match_record)
            tm_match_id = match_record["match_id"]
            lineup_links.append(
                f"https://www.transfermarkt.com/x/aufstellung/spielbericht/{tm_match_id}"
            )

        self.register_links_to_handler(lineup_links, TmLineupHandler)
        return league_records


class TmLineupHandler(RequestSoupHandler):

    test_urls = [
        f"https://www.transfermarkt.com/x/aufstellung/spielbericht/{tmid}"
        for tmid in [3050172, 3412946]
    ]

    proxy_kind = "PacketProxy"

    def parse_soup(self, soup: "BeautifulSoup"):

        att_records = []
        form_rows = soup.find_all("div", class_="row sb-formation")
        for form_row, fr_name in zip(form_rows, ["starter", "sub"]):
            sides = form_row.find_all("table", class_="items")
            for side, side_name in zip(sides, ["home", "away"]):
                for player_row in side.find("tbody").children:
                    if isinstance(player_row, str):
                        continue
                    att_record = {}
                    add_country(att_record, player_row)
                    a = player_row.find("a", class_="spielprofil_tooltip")
                    att_record["name"] = a.find("img")["title"].strip()
                    att_record["tm_id"] = a["id"]
                    att_records.append(
                        {"side": side_name, "starter": fr_name, **att_record}
                    )

        assert att_records
        return att_records


class TmPlayer(RequestSoupHandler):

    test_urls = [
        f"https://www.transfermarkt.com/x/profil/spieler/{pid}"
        for pid in [15724, 343052]
    ]
    proxy_kind = "PacketProxy"

    def parse_soup(self, soup: "BeautifulSoup"):

        top_player_data = [
            {"table-key": "name", "text": soup.find("h1").text.strip()}
        ] + get_top_datacontent(soup)

        player_table = soup.find("table", class_="auflistung")
        if player_table:
            top_player_data += parse_unstacked_table(player_table)

        transfer_histroy = soup.find("div", class_="transferhistorie")
        if transfer_histroy is None:
            transfer_data = []
        else:
            transfer_table = transfer_histroy.find("table")
            for mobile_tag in transfer_table.find_all(class_="show-for-small"):
                mobile_tag.decompose()
            for flag_td in transfer_table.find_all(class_="no-border-rechts"):
                flag_td.decompose()

            transfer_data = parse_table(transfer_table)

        script_re = re.compile(r"new Highcharts.Chart\(.*'data'\:(\[.*?\]).*\)")

        mv_recs = None
        for js_script in soup.find_all("script"):
            mv_found = script_re.findall(js_script.text)
            if mv_found:
                mv_recs = json.loads(
                    mv_found[0]
                    .replace("'", '"')
                    .replace("\\x20", " ")
                    .replace("\\x", " ")
                )
                for r in mv_recs:
                    r.pop("marker")
                break

        team_links = []

        for tr in transfer_data:
            for k in ["Left - link", "Joined - link"]:
                tid = tr[k].split("/")[-3]
                team_links.append(
                    f"https://www.transfermarkt.com/x/datenfakten/verein/{tid}"
                )
        self.register_links_to_handler(team_links, TmTeam)

        return {
            "transfers": transfer_data,
            "mv": mv_recs,
            "player_data": top_player_data,
        }


class TmTeam(RequestSoupHandler):
    test_urls = [
        f"https://www.transfermarkt.com/x/datenfakten/verein/{tid}"
        for tid in [11, 1112]
    ]
    proxy_kind = "PacketProxy"

    def parse_soup(self, soup):

        others_div = soup.find("div", id="alleTemsVerein")
        associeated_teams = []
        if others_div is not None:
            for tref in others_div.find_all("a"):
                associeated_teams.append(
                    {
                        "child_team_id": tref.get("id"),
                        "child_team_name": tref.get("title"),
                    }
                )

        boxes = get_top_datacontent(soup)
        team_table = soup.find("table", class_="profilheader")
        if team_table is not None:
            boxes += parse_unstacked_table(team_table)

        return {
            "info": {"name": soup.find("h1").text.strip(), **parse_qs_box(soup)},
            "added": boxes,
            "associated_teams": associeated_teams,
        }


def parse_qs_box(soup):
    elem = soup.find("tm-quick-select-bar")
    out = {}
    if elem is None:
        return out
    for k, v in elem.attrs.items():
        if k != "translations" and v:
            out[k.split("-")[-1]] = v
    return out


def add_country(d, elem):
    country_flag = elem.find("img", class_="flaggenrahmen")
    if country_flag:
        d["country"] = country_flag.get("title")


def parse_table(table_elem):
    headers = [td.text.strip() for td in table_elem.find("tr").find_all("th")]
    _hset = set(headers)
    records = []
    for tr in table_elem.find_all("tr"):
        record = {}
        for td, c in zip(tr.find_all("td"), headers):
            record[c] = td.text.strip()
            _a = td.find("a")
            if _a:
                record[f"{c} - link"] = _a["href"]
        if not _hset - record.keys():
            records.append(record)
    return records


def parse_result_box(result_box):
    match_row = result_box.find_parent("tr")
    match_record = {
        "score": result_box.text.strip(),
        "match_id": result_box["href"].split("/")[-1],
        "date": result_box.find_previous(
            "a", href=re.compile("/aktuell/waspassiertheute/aktuell/new/datum/*")
        )["href"].split("/")[-1],
    }
    participant_ids = set()
    side_name = "home"
    for side_link in match_row.find_all("a", class_="vereinprofil_tooltip"):
        if side_link.find("img"):
            continue
        side_id = side_link["id"]
        if side_id not in participant_ids:
            match_record[f"{side_name}-name"] = side_link.text.strip()
            match_record[f"{side_name}-tm_id"] = side_id
            side_name = "away"
    return match_record


def get_top_datacontent(soup):
    out = []
    data_div = soup.find("div", class_="dataContent")
    if data_div is None:
        raise ValueError("no datacontent")

    for p in data_div.find_all("p"):
        rec = {
            "key": p.find("span", class_="dataItem").text.strip(),
            "text": " ".join(p.find("span", class_="dataValue").text.strip().split()),
        }
        img = p.find("img")
        if img:
            rec["img-text"] = img.get("title")
        link = p.find("a")
        if link:
            rec["link-href"] = link.get("href")
        out.append(rec)
    return out


def parse_unstacked_table(table_elem):
    out = []
    for tr in table_elem.find_all("tr"):
        rec = {
            "table-key": tr.find("th").text.strip(),
            "text": tr.find("td").text.strip(),
        }
        for idx, img in enumerate(tr.find_all("img")):
            rec[f"img-{idx}"] = img.get("title")

        for idx, _a in enumerate(tr.find_all("a")):
            rec[f"href-{idx}"] = _a.get("href")
        out.append(rec)
    return out
