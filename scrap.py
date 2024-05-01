from datetime import timedelta, datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from HourChunganJiji import HourChunganJiji
from YearMonthDate import YearMonthDate


def hourly_it(start, finish):
    while finish > start:
        start = start + timedelta(hours=1)
        yield start

def main():
    # url = "https://8za.me/overall.htm"
    # with webdriver.Chrome(service=Service(ChromeDriverManager().install())) as driver:
    #     driver.get(url)
    #     print("hello")

    db_path = "/Users/jack/PycharmProjects/saju/resources/ymd/"
    ymd = YearMonthDate(db_path)
    chungan_path = "/Users/jack/PycharmProjects/saju/table/hour_chungan.yaml"
    chogyeon_path = "/Users/jack/PycharmProjects/saju/table/chogyeonpyo.yaml"
    hcj = HourChunganJiji(chungan_path, chogyeon_path)

    start = datetime(1991, 7, 15, 6)
    finish = datetime(2000, 12, 31, 23)
    for iter_time in hourly_it(start, finish):
        res = ymd.request(iter_time.year, iter_time.month, iter_time.day)
        year_ganji = res['lunSecha'].tolist()[0]
        month_ganji = res['lunWolgeon'].tolist()[0]
        day_ganji = res['lunIljin'].tolist()[0]
        day_chungan = day_ganji[0]
        hour_gangi = hcj.get_chungan(day_chungan, iter_time.hour, 0)
        final = f"{hour_gangi} {day_ganji} {month_ganji} {year_ganji}"
        print("")





if __name__ == "__main__":
    main()