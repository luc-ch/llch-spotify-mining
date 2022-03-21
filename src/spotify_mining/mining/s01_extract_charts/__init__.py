#!/usr/bin/env python
# coding: utf-8


import cloudscraper
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import os
from tqdm.notebook import tqdm
import time
from datetime import date, timedelta, datetime

from spotify_mining.configurations import Configuration

from ...services import postgres


class ExtractCharts:
    def __init__(self):
        config = Configuration()
        self.engine = create_engine(config.db_connection_string)
        postgres.test_db_connection()
        self.sdate = date(2017, 1, 1)  # start date
        self.edate = datetime.now().date()  # end date
        self.save_to_file = config.general_persist_to_file
        self.dataset_folder = config.general_dataset_folder
        self.date_format = "%Y-%m-%d"
        self.charts_webpage = "https://spotifycharts.com"
        self.countries_dict = config.countries_dict

    def run(self,):
        for region, label in sorted(
            list(self.countries_dict.items()),
            key=lambda x: x[0].lower(),
            reverse=False,
        ):
            self.store_charts_from_region(region, label)

    def store_charts_from_region(self, region="ar", label="Argentina", edate=None):
        if edate is None:
            edate = datetime.now()
        sdate = self.sdate
        edate = edate.date()
        dates = self.load_dates_charts(region, sdate, edate)
        print(f"{label}", end=" - ")

        if self.save_to_file:
            os.makedirs(f"{self.dataset_folder}/{region}/", exist_ok=True)

        err = 0
        if len(dates) > 5:
            for d in tqdm(dates):
                e = self.upload_data(d, region)
                err += e
                if err > 5:
                    break
            print()
        else:
            for d in dates:
                self.upload_data(d, region)

    def upload_data(
        self, d: date, region: str,
    ):
        dd = d.strftime(self.date_format)
        if len(self.if_chart_loaded(region, dd)) > 0:
            return 0
        try:
            df = self.get_charts_in_dataframe(dd, region)
            if self.save_to_file:
                df.to_csv(
                    f"{self.dataset_folder}/{region}/charts_{dd}.csv", index=False
                )
            df.to_sql("charts", self.engine, if_exists="append", index=False)
            return 0
        except Exception:
            print(dd, end=" - ")
            time.sleep(2)
            return 1

    def get_charts_in_dataframe(self, date="2020-04-17", region="ar"):
        scraper = cloudscraper.create_scraper()
        r = scraper.get(f"{self.charts_webpage}/regional/{region}/daily/{date}")
        if not r.ok:
            raise RuntimeError(f"Falló búsqueda de charts - {region} - {date}")
        soup = BeautifulSoup(r.text, "html.parser")
        columns = [
            "region",
            "date",
            "position",
            "track_id",
            "streams",
            "track",
            "artist",
        ]
        ddd = datetime.strptime(date, self.date_format)
        list_charts = [
            self.get_one_chart(tr, ddd, region)
            for tr in soup.find_all("tbody")[0].find_all("tr")
        ]
        return pd.DataFrame(list_charts, columns=columns)

    def get_one_chart(self, tr, ddd, region):
        td = tr.find_all("td")
        web = td[0].find("a", href=True)["href"]
        position = int(td[1].text)
        track = td[3].find("strong").text
        artist = td[3].find("span").text
        streams = int(td[4].text.replace(",", ""))
        track_id = web.rsplit("/", 1)[-1]
        return [region, ddd, position, track_id, streams, track, artist]

    def date_range(self, start: date, end: date):
        delta = end - start
        days = [start + timedelta(days=i) for i in range(delta.days)]
        return days

    def load_dates_charts(self, region: str, sdate: date, edate: date):
        dates = self.date_range(sdate, edate)
        dates_str = "'" + "', '".join(d.strftime(self.date_format) for d in dates) + "'"
        command = f"""
                SELECT date
                FROM charts
                WHERE region='{region}'
                AND position=1
                AND date IN ({dates_str});
        """
        dates_in_db = postgres.execute_command_postgres(command)
        missing = list(set(dates) - set(dates_in_db))
        missing.sort(reverse=True)
        return missing

    def if_chart_loaded(self, region: str, date: date):
        command = f"""
                SELECT 1
                FROM charts
                WHERE region='{region}'
                AND date='{date}'
                AND position=1
                LIMIT 1;
        """
        chart = postgres.execute_command_postgres(command)
        return chart


def run():
    ExtractCharts().run()
