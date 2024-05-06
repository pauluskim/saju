from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree, html
from tqdm import tqdm

from DatabaseManager import DatabaseManager


class Parser(DatabaseManager):
    TOTAL_PAGES = list(range(2, 22))
    TITLE = "title"
    TEXT = "text"
    BUTTON = "button"
    TABLE = "table"
    replace_dict = {
        "내일은": "Lia",
        "“풀어서 보는 사주” ": "“사주” ",
        "(깜박거리는 글자)": "",
        "^^": ":)",
        "유료 콘텐츠": "다음 컨텐츠",
    }
    def __init__(self, raw_saju_db_path, parsed_saju_db_path):
        self.db_path = parsed_saju_db_path
        self.raw_df = DatabaseManager.load_latest_db(raw_saju_db_path)
        self.init_df()

    def init_df(self):
        columns = ["key", "y", "m", "d", "h"]
        for page_num in self.TOTAL_PAGES:
            columns.append("page_{}_{}".format(page_num, self.TITLE))
            columns.append("page_{}_{}".format(page_num, self.TEXT))
            columns.append("page_{}_{}".format(page_num, self.TABLE))
            columns.append("page_{}_{}".format(page_num, self.BUTTON))
        self.df = pd.DataFrame(columns=columns)

    def main(self):
        for index, row in tqdm(self.raw_df.iterrows(), total=self.raw_df.shape[0]):
            key_dict = {
                "key": row["key"],
                "y": row["y"],
                "m": row["m"],
                "d": row["d"],
                "h": row["h"]
            }
            raw = row["rawResponse"]
            for asis, tobe in self.replace_dict.items():
                raw = raw.replace(asis, tobe)
            pages = self.parse_pages(raw)
            new_row = self.flatten_pages(pages)
            new_row.update(key_dict)
            self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        # self.df = self.df.fillna([""])
        self.save_parquet()


    def flatten_pages(self, pages):
        res = {}
        for idx, page in enumerate(pages):
            page_num = idx + 2
            for key, val in page.items():
                res["page_{}_{}".format(page_num, key)] = val
        return res


    def parse_pages(self, body):
        # # By Selectorshub > relative xpath
        # intro_title = self.get_text_by_xpath(body, "//p[contains(text(),'사주팔자(四柱八字)가 뭔가요?')]")
        # intro_cont = self.get_text_by_xpath(body, '//*[@id="TOP"]/div[2]/div[1]/div/div[2]/p[2]')

        # BeautifulSoup
        self.soup = BeautifulSoup(body, 'html.parser')
        pages = []
        for page_num in self.TOTAL_PAGES:
            pages.append(self.get_text_by_page('page-{}'.format(page_num)))
        return pages


    def get_text_by_page(self, page):
        def filter_contents(contents):
            filtered = []
            for cont in contents:
                if cont.name == "br":
                    continue
                filtered.append(str(cont))
            return filtered

        key_text = defaultdict(list)
        page_data = self.soup.select("div[page={}]".format(page))
        if len(page_data) == 0:
            return  key_text
        contents = page_data[0].contents
        for content in contents:
            cls = content.attrs['class']
            filtered = filter_contents(content.contents)

            if len(cls) == 0 or cls[0] in ["txt_keyword", "txt_underline"]:
                key_text[self.TEXT].extend(filtered)
            elif cls[0] == "result_title":
                key_text[self.TITLE].extend(filtered)
            elif cls[0] == "row_bt_area":
                key_text[self.BUTTON].extend(filtered)
            elif cls[0] == "rsaju_col":
                key_text[self.TABLE].extend(filtered)
            else:
                print("Unknown cls attr of div: {}\n{}".format(cls, content))
                pass

        return key_text

    def get_text_by_xpath(self, body, xpath):
        # By SelectorsHub > relative Xpath
        tree = etree.HTML(body)
        tr = html.document_fromstring(body)
        return tree.xpath(xpath)[0].text






if __name__ == "__main__":
    raw_saju_db_path = "/Users/jack/PycharmProjects/saju/resources/kor_saju/"
    parsed_saju_db_path = "/Users/jack/PycharmProjects/saju/resources/kor_plain_saju/"

    parser = Parser(raw_saju_db_path, parsed_saju_db_path)
    parser.main()
