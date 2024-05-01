import logging
import os
import pathlib
from glob import glob

import pandas as pd
import xml.etree.ElementTree as ET

from datetime import date, timedelta, datetime

import requests
from tqdm import tqdm


class ApiError(Exception):
    pass

class YearMonthDate:
    df = pd.DataFrame(columns=
                      ["y", "m", "d", "lunYear", "lunMonth", "lunDay", "lunIljin", "lunLeapmonth", "lunNday",
                       "lunSecha", "lunWolgeon", "solYear", "solMonth", "solDay", "solJd", "solLeapyear",
                       "solWeek"])
    api_url = "http://apis.data.go.kr/B090041/openapi/service/LrsrCldInfoService/getLunCalInfo?"

    def __init__(self, db_path):
        self.db_path = db_path
        self.load_db(db_path)
        self.counter = 1

    def load_db(self, db_path):
        dir_lst = sorted(list(glob(os.path.join(db_path, "*"))), key= lambda abs_path: -int(abs_path.split("/")[-1]))
        for dir in dir_lst:
            if pathlib.Path(os.path.join(dir, "_SUCCESS_")).is_file():
                self.df = pd.read_parquet(os.path.join(dir, "db.parquet"), engine='pyarrow')
                break

    def dump_db(self, start_date, end_date):
        def daterange(start_date, end_date):
            for n in range(int((end_date - start_date).days)):
                yield start_date + timedelta(n)

        for single_date in tqdm(daterange(start_date, end_date)):
            self.request(single_date.year, single_date.month, single_date.day)
            if self.counter % 600 == 0:
                self.save_parquet()

        self.save_parquet()

    def save_parquet(self):
        now = datetime.now().strftime("%Y%m%d%H%M")
        output_path = os.path.join(self.db_path, now)
        os.mkdir(output_path)
        self.df.to_parquet(os.path.join(output_path, "db.parquet"))
        pathlib.Path(os.path.join(output_path, "_SUCCESS_")).touch()
    def request(self, y, m, d):
        try:
            res = self.df.loc[(self.df["y"] == y) & (self.df["m"] == m) & (self.df["d"] == d)]
        except KeyError as e:
            print("Generate a new db!!!")
            res = self.update_db(y, m, d)
        except ApiError as e:
            print("API Error")
            return

        if len(res.index) == 0:
            res = self.update_db(y, m, d)
        return res

    def update_db(self, y, m, d):
        params = {'solYear': str(y),
                  'solMonth': '{0:02d}'.format(m),
                  'solDay': '{0:02d}'.format(d),
                  'serviceKey': 'jFMeIohmYEadE1u/G8l3gAtFkCfUcm1kSIQUj/cfFDaKIAT4HdAaDqo6xhuUou3Mg4MO4xScZCjY/MWDng2IJw=='}

        resp = requests.get(self.api_url, params=params)
        content = resp.content

        root = ET.fromstring(content)

        if len(list(root.iter('item'))) != 1:
            logging.error("item list length is more than 1: {}/{}/{}".format(y, m, d))
            raise ApiError("ITEM LIST MORE THAN 1")

        new_row = {"y": y, "m": m, "d": d}
        for item in root.iter('item'):
            new_row["lunYear"] = item.find("lunYear").text
            new_row["lunMonth"] = item.find("lunMonth").text
            new_row["lunDay"] = item.find("lunDay").text

            new_row["lunIljin"] = item.find("lunIljin").text
            new_row["lunLeapmonth"] = item.find("lunLeapmonth").text
            new_row["lunNday"] = item.find("lunNday").text
            new_row["lunSecha"] = item.find("lunSecha").text
            new_row["lunWolgeon"] = item.find("lunWolgeon").text

            new_row["solYear"] = item.find("solYear").text
            new_row["solMonth"] = item.find("solMonth").text
            new_row["solDay"] = item.find("solDay").text
            new_row["solJd"] = item.find("solJd").text
            new_row["solLeapyear"] = item.find("solLeapyear").text
            new_row["solWeek"] = item.find("solWeek").text

        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        self.counter += 1
        return self.df.loc[(self.df["y"] == y) & (self.df["m"] == m) & (self.df["d"] == d)]


if __name__ == "__main__":
    db_path = "/Users/jack/PycharmProjects/saju/resources/ymd/"
    ymd = YearMonthDate(db_path)
    res = ymd.request(1990, 1, 1)
    print(res)
    # start_date = date(1970, 1, 1)
    # end_date = date(2010, 12, 31)
    # ymd.dump_db(start_date, end_date)




