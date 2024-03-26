import os
import pickle
import re
import time
from pathlib import Path
from typing import Optional

import aswan
from aswan.utils import browser_wait
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

load_dotenv()

project = aswan.Project("whoscored")

accept_button_finder = (By.XPATH, "/html/body/div[1]/div/div/div[2]/button[2]")

DAYS_BACK = 365 // 3  # TODO:  make conditional stopping 2017-nov-13
LS_URL = "https://www.whoscored.com/LiveScores"


class PacketProxy(aswan.ProxyBase):
    expiration_secs = float("inf")
    prefix = "http"
    port_no = 31112

    def get_creds(self) -> Optional[aswan.ProxyAuth]:
        return aswan.ProxyAuth(
            user=os.environ["PACK_USER"], password=os.environ["PACK_PW"]
        )

    def _load_host_list(self) -> list:
        return ["proxy.packetstream.io"]


@project.register_handler
class MatchFinder(aswan.BrowserSoupHandler):
    url_root = "https://www.whoscored.com"

    def handle_driver(self, driver: "Chrome"):
        time.sleep(25)
        n = 0
        for d in tqdm(list(range(DAYS_BACK))):
            nextpage = driver.find_element(By.XPATH, '//a[@title="View previous day"]')
            nextpage.click()
            try:
                browser_wait(driver, wait_for_class="divtable-body", timeout=30)
            except Exception as e:
                print(f"{d} days back error")
                raise e
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
            soup = BeautifulSoup(driver.page_source, "html5lib")
            matches_w_reports = soup.find_all("a", class_="match-link rc match-report")
            for mlink in matches_w_reports:
                report_link = mlink["href"].replace("MatchReport", "Live")
                self.register_links_to_handler([report_link], GetMatchDetails)
                n += 1
        return f"found: {n}"

    def start_browser_session(self, browser: "Chrome"):
        browser.get(LS_URL)
        time.sleep(5)
        browser_wait(browser, wait_for_id="livescores", timeout=30)


@project.register_handler
class GetMatchDetails(aswan.BrowserSoupHandler):
    proxy_cls = PacketProxy
    restart_session_after = 200
    process_indefinitely: bool = True
    max_in_parallel = 4

    test_urls = [
        "https://www.whoscored.com/Matches/1485554/Live",
        "https://www.whoscored.com/Matches/615270/Live",
    ]

    def load_cache(self, url):
        cache_dict = pickle.loads(Path("cache_dict.pkl").read_bytes())
        cache_p = cache_dict.get(url)
        if cache_p is not None:
            return cache_p.read_text()

    def parse(self, soup: "BeautifulSoup"):
        return get_datastring(soup)

    def handle_driver(self, driver: "Chrome"):
        for _ in range(5):
            try:
                browser_wait(driver, wait_for_id="live-match", timeout=9)
                break
            except TimeoutException as e:
                print("refreshing", e)
                driver.refresh()
                pass

    def start_browser_session(self, browser: "Chrome"):
        browser.get(LS_URL)
        time.sleep(0.5)
        browser_wait(browser, wait_for_id="livescores", timeout=30)

    def is_session_broken(self, result: Exception):
        return True


def get_datastring(soup):
    md_rex = re.compile(r"matchCentreData: (.*)")
    for script in soup.find_all("script"):
        for cont in script.contents:
            mcds = md_rex.findall(cont)
            if mcds:
                return (
                    mcds[0]
                    .strip()
                    .replace("&amp;", "&")
                    .replace("&#287;", "g")
                    .replace("\t", "")
                    .strip()[:-1]
                    # + "}"
                )
    raise ValueError("couldn't find anything")
