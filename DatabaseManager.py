import datetime
import os
import pathlib
import shutil
from datetime import timedelta, datetime
from glob import glob

import pandas as pd
from pytz import timezone


class DatabaseManager():
    TIMEZONE = "Asia/Seoul"

    def __init__(self, db_path):
        self.db_path = db_path
        self.save_db_period = timedelta(minutes=10)
        self.latest_time = datetime.now(timezone(self.TIMEZONE)) - 2 * self.save_db_period
        self.load_db()

    def load_db(self):
        dir_lst = sorted(list(glob(os.path.join(self.db_path, "*"))), key= lambda abs_path: -int(abs_path.split("/")[-1]))
        for dir in dir_lst:
            if pathlib.Path(os.path.join(dir, "_SUCCESS_")).is_file():
                self.df = pd.read_parquet(os.path.join(dir, "db.parquet"), engine='pyarrow')
                self.latest_time = datetime.strptime(dir.split("/")[-1] + "-+0900", '%Y%m%d%H%M-%z')
                break

    @classmethod
    def load_latest_db(cls, db_path):
        dir_lst = sorted(list(glob(os.path.join(db_path, "*"))), key= lambda abs_path: -int(abs_path.split("/")[-1]))
        for dir in dir_lst:
            if pathlib.Path(os.path.join(dir, "_SUCCESS_")).is_file():
                return pd.read_parquet(os.path.join(dir, "db.parquet"), engine='pyarrow')
        return None

    def save_parquet(self):
        now = datetime.now(timezone(self.TIMEZONE)).strftime("%Y%m%d%H%M")
        output_path = os.path.join(self.db_path, now)
        os.mkdir(output_path)
        self.df.to_parquet(os.path.join(output_path, "db.parquet"))
        pathlib.Path(os.path.join(output_path, "_SUCCESS_")).touch()
        self.latest_time = datetime.now(timezone(self.TIMEZONE))

        # Clear old files except for the 5 latest db
        self.clear_old_db()

    def clear_old_db(self):
        dir_lst = sorted(list(glob(os.path.join(self.db_path, "*"))), key=lambda abs_path: -int(abs_path.split("/")[-1]))
        counter = 5
        for dir in dir_lst:
            if pathlib.Path(os.path.join(dir, "_SUCCESS_")).is_file():
                counter -= 1
            else:
                shutil.rmtree(dir)

            if counter < 0:
                shutil.rmtree(dir)
