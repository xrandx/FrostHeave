from configparser import ConfigParser
from os import path
from sys import argv
from pandas import DataFrame
from numpy import datetime64, float32, int32

MINI_EPS = 1e-6
NUM_SCALE = 6
PARAMS_TABLE_NAME = "ef_stat_params"
CURRENT_DIR = path.dirname(path.realpath(argv[0])) + "\\"

PARAMS_LIST = ['table_name', "k", "b", "k_packet_num", "k0", "b0",
               "k0_packet_num", "k0_accumulate", "mutation_accumulate"]
DATA_LIST = ["datetime", "temperature", "strain", "height", "stress", "tsf"]

# DATA_TEMPLATE = DataFrame(columns=DATA_LIST)
# PARAMS_TEMPLATE = DataFrame(columns=PARAMS_LIST)


def str2array(string):
    arr = string.strip().split(',')
    return [i.strip() for i in arr]


def get_table_name(device_name):
    return 'ef_stat_%s' % (device_name.strip().lower())


def get_table_names(device_str):
    arr = device_str.strip().split(',')
    tmp = []
    for i in arr:
        tmp.append(get_table_name(i))
    return tmp


class Config:
    config = ConfigParser()
    device2path = dict()
    mysql_config = ""

    DATA_TEMPLATE = DataFrame(columns=DATA_LIST)
    PARAMS_TEMPLATE = DataFrame(columns=PARAMS_LIST)

    @classmethod
    def init_from_file(cls):
        cls.DATA_TEMPLATE['datetime'] = datetime64()
        for col in ["temperature", "strain", "height", "stress", "tsf"]:
            cls.DATA_TEMPLATE[col] = float32(0.0)
        for col in ["k0", "b0", "k", "b", "k0_accumulate",
                    "mutation_accumulate"]:
            cls.PARAMS_TEMPLATE[col] = float32(0.0)
        for col in ["k0_packet_num", "k_packet_num"]:
            cls.PARAMS_TEMPLATE[col] = int32(0)

        cls.PARAMS_TEMPLATE.set_index('table_name')

        cls.config.read(CURRENT_DIR + "setting.ini", encoding="utf-8")
        mysql_ = cls.config['mysql']
        cls.mysql_config = 'mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8' \
                           % (mysql_['username'].strip(), mysql_['password'].strip(), mysql_['host'].strip(),
                              mysql_['port'].strip(), mysql_['database'].strip())

        import_file = cls.config['import_file']
        files = str2array(import_file['files'])

        for i in range(len(files)):
            file_name = files[i][:-4]
            cls.device2path[get_table_name(file_name)] = CURRENT_DIR + files[i]
        print(cls.device2path)