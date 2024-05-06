import ast
import hashlib
from datetime import datetime

import deepl
import pandas as pd
from pytz import timezone

from DatabaseManager import DatabaseManager
from Parser import Parser
from YearMonthDate import ApiError


class Translator(DatabaseManager):
    src_lang = "KO"
    target_lang = "EN-US"
    df = pd.DataFrame(columns=["key", src_lang, target_lang])

    def __init__(self, kor_saju_db_path, translated_saju_db_path):
        super(Translator, self).__init__(translated_saju_db_path)
        self.kor_df = DatabaseManager.load_latest_db(kor_saju_db_path)

        auth_key = "c61ddef7-f28f-49ef-8a24-cd6ee070c2ac:fx"
        self.translator = deepl.Translator(auth_key)

        self.nonhit = 0
        self.total_request = 0


    def dump(self):
        for index, row in self.kor_df.iterrows():
            for page_num in Parser.TOTAL_PAGES:
                src_sent = row["page_{}_text".format(page_num)]
                src_sent = " ".join(src_sent)
                self.request(src_sent)
            break


    def request(self, sent):
        self.total_request += 1
        key = hashlib.md5(sent)
        try:
            res = self.df.loc[self.df["key"] == key]
        except KeyError as e:
            print("Generate a new db!!!")
            res = self.update_db(key, sent)
        except ApiError as e:
            print(f"Translator Error: {sent}-{key}")
            return

        if len(res.index) == 0:
            res = self.update_db(key, sent)
        return res

    def update_db(self, key, src_sent):
        self.nonhit += 1
        target_sent = self.translator.translate_text(src_sent, target_lang=self.target_lang)
        new_row = {"key": key,
                   self.src_lang: src_sent,
                   self.target_lang: target_sent
                   }

        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)

        if datetime.now(timezone(self.TIMEZONE)) - self.latest_time > self.save_db_period:
            self.save_parquet()





if __name__ == "__main__":
    trans_db_path = "/Users/jack/PycharmProjects/saju/resources/trans_plain_saju"
    kor_saju_db_path = "/Users/jack/PycharmProjects/saju/resources/kor_plain_saju"
    tr = Translator(kor_saju_db_path, trans_db_path)
    tr.dump()







