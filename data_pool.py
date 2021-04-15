#   对文件和数据库数据合并载入内存
from pandas import Timedelta, DataFrame, read_csv, to_datetime
from numpy import float32, polyfit, string_
from config import Config, str2array, PARAMS_TABLE_NAME, get_table_name, MINI_EPS, PARAMS_LIST
from sql_mapper import SQLMapper
from multiprocessing import Process


def view_data(data, num=20):
    print(data.dtypes)
    print(data[:num])


class DataPool:
    ef_tables = dict()
    params = None
    save_start = dict()

    # is_exist = dict()

    #   初始化设置和参数
    @classmethod
    def init(cls, by_file=True):
        Config.init_from_file()
        SQLMapper.class_init_by_config(Config.mysql_config)
        cls.params = Config.PARAMS_TEMPLATE.copy(deep=True)
        if SQLMapper.is_table_exist(PARAMS_TABLE_NAME):
            cls.params = SQLMapper.select_params()
        cls.params.set_index(["table_name"], drop=False, inplace=True)

        #   调入所有
        if not by_file:
            return
        else:
            for table_name in Config.device2path.keys():
                cls.load_table(table_name)

    @classmethod
    def load_table(cls, table_name):
        if SQLMapper.is_table_exist(table_name):
            cls.ef_tables[table_name] = SQLMapper.select_16days(table_name)
        else:
            cls.ef_tables[table_name] = DataFrame()
        print("start")
        cls.save_start[table_name] = len(cls.ef_tables[table_name].index)

    @classmethod
    def read_instruction(cls, cmd):
        cmd_arr = str2array(cmd)
        new_df = DataFrame.from_dict({
            "datetime": [cmd_arr[1]],
            "temperature": [float32(cmd_arr[2])],
            "strain": [float32(cmd_arr[3])],
        })
        new_df['height'] = new_df['stress'] = new_df['tsf'] = float32(0.0)
        new_df['datetime'] = to_datetime(new_df['datetime'])

        table_name = get_table_name(cmd_arr[0].strip())
        cls.load_table(table_name)
        print("Reading by cmd: " + cmd)
        cls.ef_tables[table_name] = cls.ef_tables[table_name].append(new_df, ignore_index=True,
                                                                     verify_integrity=True)
        return [table_name]

    @classmethod
    def read_file(cls):
        for table_name, import_file_name in Config.device2path.items():
            print(table_name + ":" + import_file_name + "file is being read.")
            file_data = read_csv(import_file_name, sep=',',
                                 names=['datetime', 'temperature', 'strain'],
                                 dtype={'datetime': string_, 'temperature': float32, 'strain': float32},
                                 parse_dates=['datetime']
                                 )
            #   datetime, temperature, strain, height, stress,
            file_data['height'] = file_data['stress'] = file_data['tsf'] = float32(0.0)
            cls.ef_tables[table_name] = cls.ef_tables[table_name].append(file_data, ignore_index=True,
                                                                         verify_integrity=True)
            # print(cls.ef_tables[table_name].info)
            # view_data(cls.ef_tables[table_name])
        return Config.device2path.keys()

    @classmethod
    def multi_process_fit(cls, table_names):
        for table_name in table_names:
            if table_name not in cls.params.index:
                tmp = DataFrame([dict(zip(PARAMS_LIST, [table_name] + [0] * 8))], index=[table_name])
                cls.params = cls.params.append(tmp)

        print(cls.save_start)
        process = [Process(target=cls.fit_one, args=(i,)) for i in table_names]
        [p.start() for p in process]
        [p.join() for p in process]

    @classmethod
    def fit_one(cls, table_name):
        print(table_name + " SOLVING")
        save_start = cls.save_start[table_name]
        this_table = cls.ef_tables[table_name]
        count = 0
        if len(this_table.iloc[save_start:]) > 0:
            for idx in range(save_start, len(this_table.index)):
                count += 1
                print("%s deal %d packet" %(table_name, count))
                if cls.get_params(table_name, idx):
                    continue
                cls.compute(table_name, idx)

    @classmethod
    def normal_fit_(cls, table_names):
        for table_name in table_names:
            if table_name not in cls.params.index:
                tmp = DataFrame([dict(zip(PARAMS_LIST, [table_name] + [0] * 8))], index=[table_name])
                cls.params = cls.params.append(tmp)
        for table_name in table_names:
            cls.fit_one(table_name)

    @classmethod
    def fit_params_by_least_square(cls, table_name, start, end):
        this_table = cls.ef_tables[table_name].iloc[start: end]
        x = this_table["temperature"].values.flatten()
        y = this_table["strain"].values.flatten()
        coefficient = polyfit(x, y, 1)
        return coefficient[0], coefficient[1]

    @classmethod
    def get_params(cls, table_name, idx):
        this_table = cls.ef_tables[table_name]
        param_idx = cls.params.index.get_loc(table_name)
        param_d = cls.params.iloc[param_idx].to_dict()

        datetime_num = cls.ef_tables[table_name].columns.get_loc("datetime")
        init_day = this_table.iloc[0, datetime_num].date()
        now_day = this_table.iloc[idx, datetime_num].date()
        yesterday = this_table.iloc[idx - 1, datetime_num].date()

        is_diff_day = (now_day != yesterday)
        past_days = (now_day - init_day).days
        if past_days < 2:
            return True
        else:
            if 2 <= past_days < 15 or (past_days == 15 and is_diff_day):
                #   k,b 按当前这包之前的所有
                param_d['k'], param_d['b'] = cls.fit_params_by_least_square(table_name, 0, idx)
                param_d['k_packet_num'] = idx
            else:
                #   k,b 按上一次计算大小
                param_d['k'], param_d['b'] = cls.fit_params_by_least_square(table_name,
                                                                            idx - param_d["k_packet_num"] - 1, idx)

            if is_diff_day and past_days in [2, 7, 15]:
                #   k0, b0 按当前这包之前的所有包
                last_k0 = param_d['k0']
                param_d['k0'], param_d['b0'] = cls.fit_params_by_least_square(table_name, 0, idx)
                param_d['k0_packet_num'] = idx
                if past_days == 2:
                    param_d['k0_accumulate'] = 0
                elif past_days == 7:
                    param_d['k0_accumulate'] = param_d['k0'] - last_k0
                elif past_days == 15:
                    param_d['k0_accumulate'] = param_d['k0'] + param_d['k0_accumulate'] - last_k0

            for k, v in param_d.items():
                cls.params.loc[table_name, k] = v
            return False

    @classmethod
    def compute(cls, table_name, idx):
        this_row = cls.ef_tables[table_name].iloc[idx].to_dict()
        last_row = cls.ef_tables[table_name].iloc[idx - 1].to_dict()
        param_d = cls.params.loc[table_name].to_dict()

        mutation = (this_row["strain"] - param_d["mutation_accumulate"] - last_row["strain"]) - (
                param_d["k0"] * (this_row["temperature"] - last_row["temperature"]))

        delta_t = abs(this_row["temperature"] - last_row["temperature"])
        if delta_t < MINI_EPS:
            deviation = True
        else:
            deviation = abs(mutation / delta_t) - 180 > MINI_EPS

        if abs(this_row["datetime"] - last_row["datetime"]) <= Timedelta(hours=3) \
                and (abs(mutation) - 400 > MINI_EPS) \
                and deviation:
            param_d["mutation_accumulate"] = param_d["mutation_accumulate"] + mutation
        else:
            param_d["mutation_accumulate"] = param_d["mutation_accumulate"]

        this_row['height'] = (-param_d['k'] + param_d['k0'] - param_d['k0_accumulate']) * 0.5 \
                             + param_d['mutation_accumulate'] * 0.0189

        #   Tsf = (K - K[0] - ΣΔk0) * 0.005 + (B - B[0]) / 11.8 + 总Δε * 0.08475,
        this_row["tsf"] = (param_d['k'] - param_d['k0'] - param_d["k0_accumulate"]) * 0.005 \
                          + (param_d["b"] - param_d["b0"]) / 11.8 + \
                          param_d["mutation_accumulate"] * 0.08475

        this_row["strain"] = this_row["strain"] - param_d["mutation_accumulate"]
        this_row["stress"] = 0.21 * (-11.8) * (this_row["temperature"] - this_row["tsf"])

        for k, v in param_d.items():
            cls.params.loc[table_name, k] = v

        for k, v in this_row.items():
            cls.ef_tables[table_name].loc[idx, k] = v

    @classmethod
    def save2db(cls):
        SQLMapper.replace_params2mysql(cls.params)
        for table_name, df in cls.ef_tables.items():
            start = cls.save_start[table_name]
            end = len(cls.ef_tables[table_name].index)
            if end > start:
                SQLMapper.save_df2mysql(table_name, df.loc[start: end])
