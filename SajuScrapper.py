import time
from datetime import timedelta, datetime

import pandas as pd
import requests
from pytz import timezone
from tqdm import tqdm

from DatabaseManager import DatabaseManager
from HourChunganJiji import HourChunganJiji
from YearMonthDate import YearMonthDate

class ScrapError(Exception):
    pass

class SajuScrapper(DatabaseManager):
    df = pd.DataFrame(columns=["key", "y", "m", "d", "h", "rawResponse"])
    def __init__(self, saju_db_path, ymd_db_path, chungan_path, chogyeon_path):
        super(SajuScrapper, self).__init__(saju_db_path)
        self.ymd = YearMonthDate(ymd_db_path)
        self.hcj = HourChunganJiji(chungan_path, chogyeon_path)
        self.nonhit = 0
        self.total = 0

    def scrap(self, start, finish):
        def hourly_it(start, finish):
            while finish > start:
                start = start + timedelta(hours=1)
                yield start

        total_iteration = int((finish - start) / timedelta(hours=1))
        counter = 0
        for iter_time in tqdm(hourly_it(start, finish), total=total_iteration):
            res = self.ymd.request(iter_time.year, iter_time.month, iter_time.day)
            year_ganji = res['lunSecha'].tolist()[0]
            month_ganji = res['lunWolgeon'].tolist()[0]
            day_ganji = res['lunIljin'].tolist()[0]
            day_chungan = day_ganji[0]
            hour_gangi = self.hcj.get_chungan(day_chungan, iter_time.hour, 0).replace(",", "(") + ")"
            key = f"{hour_gangi} {day_ganji} {month_ganji} {year_ganji}"

            self.request(key, iter_time.year, iter_time.month, iter_time.day, iter_time.hour)
            if counter % 100  == 0 and self.nonhit != 0:
                print("\nDB size: {}".format(self.df.shape[0]))
                print("non hit count: {}, total_count: {}".format(self.nonhit, self.total))
                print("hit ratio: {:.4f}".format(100 - (1.0 * self.nonhit / self.total * 100)))
                print("current date: " + iter_time.strftime("%Y%m%d%H%M"))
                self.nonhit = 0
                self.total = 0

            counter += 1

    def request(self, key, year, month, day, hour):
        self.total += 1
        try:
            res = self.df.loc[self.df["key"] == key]
        except KeyError as e:
            print("Generate a new db!!!")
            res = self.update_db(key, year, month, day, hour)
        except ScrapError as e:
            print(f"Scrapping Error: {year}-{month}-{day}-{hour}")
            return

        if len(res.index) == 0:
            res = self.update_db(key, year, month, day, hour)
        return res

    def update_db(self, key, year, month, day, hour):
        self.nonhit += 1
        str_time = "{0}{1:02d}{2:02d}{3:02d}00".format(year, month, day, hour)
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en,en-US;q=0.9,ar;q=0.8,ko;q=0.7',
            'cache-control': 'max-age=0',
            'cookie': f'PHPSESSID=g0rtrsqkcp6elum3ur3bi15vd1e3vsl7; free_info=P{str_time}P',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
        time.sleep(1)
        r = requests.get('https://8za.me/overall_result.htm?free=null', headers=headers)
        r.encoding = r.apparent_encoding
        raw = r.text

        new_row = {"key": key, "y": year, "m": month, "d": day, "h": hour, "rawResponse": raw}
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)

        if datetime.now(timezone(self.TIMEZONE)) - self.latest_time > self.save_db_period:
            self.save_parquet()

        return self.df.loc[self.df["key"] == key]


if __name__ == "__main__":
    saju_db_path = "/Users/jack/PycharmProjects/saju/resources/kor_saju/"
    ymd_db_path = "/Users/jack/PycharmProjects/saju/resources/ymd/"
    chungan_path = "/Users/jack/PycharmProjects/saju/table/hour_chungan.yaml"
    chogyeon_path = "/Users/jack/PycharmProjects/saju/table/chogyeonpyo.yaml"
    scrapper = SajuScrapper(saju_db_path, ymd_db_path, chungan_path, chogyeon_path)
    start = datetime(1996, 10, 22, 0)
    finish = datetime(2000,12, 31, 23)
    scrapper.scrap(start, finish)
