import datetime

import yaml


class HourChunganJiji:
    def __init__(self, chungan_path, chogyeon_path):
        with open(chungan_path) as f:
            self.day_chungan_index = yaml.load(f, Loader=yaml.FullLoader)

        with open(chogyeon_path) as f:
            self.chogyeon = yaml.load(f, Loader=yaml.FullLoader)

    def get_jiji(self, h, m):
        t = datetime.time(h, m)

        if datetime.time(0, 0) <= t <= datetime.time(1, 30):
            res = "자시,子時"
        elif datetime.time(1, 31) <= t <= datetime.time(3, 30):
            res = "축시,丑時"
        elif datetime.time(3, 31) <= t <= datetime.time(5, 30):
            res = "인시,寅時"
        elif datetime.time(5, 31) <= t <= datetime.time(7, 30):
            res = "묘시,卯時"
        elif datetime.time(7, 31) <= t <= datetime.time(9, 30):
            res = "진시,辰時"
        elif datetime.time(9, 31) <= t <= datetime.time(11, 30):
            res = "사시,巳時"
        elif datetime.time(11, 31) <= t <= datetime.time(13, 30):
            res = "오시,午時"
        elif datetime.time(13, 31) <= t <= datetime.time(15, 30):
            res = "미시,未時"
        elif datetime.time(15, 31) <= t <= datetime.time(17, 30):
            res = "신시,申時"
        elif datetime.time(17, 31) <= t <= datetime.time(19, 30):
            res = "유시,酉時"
        elif datetime.time(19, 31) <= t <= datetime.time(21, 30):
            res = "술시,戌時"
        elif datetime.time(21, 31) <= t <= datetime.time(23, 30):
            res = "해시,亥時"
        else:  # datetime.time(23, 31) <= t <= datetime.time(0, 0):
            res = "자시,子時"

        return res

    def get_chungan(self, day_chungan, h, m):
        jiji = self.get_jiji(h, m)
        chunganjiji_lst = self.chogyeon[jiji]
        hour_chunganjiji = chunganjiji_lst[self.day_chungan_index[day_chungan]]

        return hour_chunganjiji


if __name__ == "__main__":
    chungan_path = "/Users/jack/PycharmProjects/saju/table/hour_chungan.yaml"
    chogyeon_path = "/Users/jack/PycharmProjects/saju/table/chogyeonpyo.yaml"
    hcj = HourChunganJiji(chungan_path, chogyeon_path)
    print(hcj.get_chungan("갑", 11, 50))