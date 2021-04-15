from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.sql.default_comparator
from sqlalchemy import create_engine
from pandas import DataFrame, read_sql
from config import PARAMS_TABLE_NAME, Config


class SQLMapper:
    Base = declarative_base()
    DbSession = session = engine = None
    input_table_mapper = {}

    @classmethod
    def is_table_exist(cls, table_name):
        try:
            #   不存在则创建
            if table_name != PARAMS_TABLE_NAME:
                Config.DATA_TEMPLATE.to_sql(name=table_name,
                                            con=cls.engine,
                                            index=False,
                                            if_exists="fail")
            else:
                # tmp = PARAMS_TEMPLATE.copy()
                Config.PARAMS_TEMPLATE.to_sql(name=PARAMS_TABLE_NAME,
                                              con=cls.engine,
                                              index=False,
                                              if_exists="fail"
                                              )
            return False
        except ValueError as err:
            #   表已经存在
            # print(err)
            return True

    @classmethod
    def class_init_by_config(cls, mysql_config):
        cls.engine = create_engine(mysql_config, echo=False)

    @classmethod
    def select_16days(cls, table_name):
        return read_sql(
            "SELECT * FROM(select * from {0}  ORDER BY datetime DESC LIMIT 1500) AS tmp ORDER BY datetime;".format(table_name),
            cls.engine)

    @classmethod
    def save_df2mysql(cls, table_name, df: DataFrame):
        df.to_sql(table_name, con=cls.engine, if_exists='append', index=False, method="multi")

    @classmethod
    def replace_params2mysql(cls, df: DataFrame):
        df.to_sql(name=PARAMS_TABLE_NAME,
                  con=cls.engine,
                  if_exists="replace",
                  index=False
                  )

    # index = True,
    # index_label = "table_name",
    # dtype = {'table_name': VARCHAR(df.index.get_level_values('table_name').str.len().max())}
    @classmethod
    def select_params(cls):
        return read_sql("select * from ef_stat_params;", cls.engine)
