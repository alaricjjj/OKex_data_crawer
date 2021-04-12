from okex_spot_client import okex_SpotClient

import pandas as pd
import mysql.connector
from MySQL_client import MySQL_client
import time,datetime


is_proxies = True

contract_type = 'Okex_Spot_data'
contract_code = 'BTC-USDT'
contract_period = 3600

db_name = contract_type
table_name = 'BTC_USDT_60min'

from_time = '2019-09-30 00:00:00'
to_time =   '2021-04-01 00:00:00'
# frequency = 60 * 60 * 4 * 1
frequency = 60 * 60 * 24 * 1

OKex_api_key= ''
OKex_api_seceret_key = ''
OKex_passphrase = ''

class OKex_data_crawler():

    def __init__(self):
        self.okex_spot_client = okex_SpotClient(api_key = OKex_api_key,
                                                api_seceret_key = OKex_api_seceret_key,
                                                passphrase = OKex_passphrase,
                                                use_server_time = False,
                                                is_proxies = is_proxies)

        self.mydb = mysql.connector.connect(
            host = 'localhost',                              # 数据库主机地址
            user = 'root',                                   # 数据库用户名
            passwd = '',                         # 数据库密码
            auth_plugin = 'mysql_native_password'            # 密码插件改变
        )
        self.mycursor = self.mydb.cursor()
        self.mysql_client = MySQL_client()

    def get_k_lines(self, from_time, to_time, contract_code = contract_code,period = contract_period):
        self.create_table()
        from_iso_time = self.datetime2iso(datetime=from_time)
        to_iso_time = self.datetime2iso(datetime = to_time)
        raw_data = self.okex_spot_client.get_history_klines(instrument_id = contract_code,
                                                            granularity = period,
                                                            from_time = from_iso_time,
                                                            to_time = to_iso_time)

        print(raw_data)

        column_names = ('Datetime', 'high', 'open', 'low','close','amount')
        for i in raw_data:
            columnn_values = (
                self.datetime_plus_eighthour(pre_dt = self.iso2datetime(i[0])),
                float(i[2]),
                float(i[1]),
                float(i[3]),
                float(i[4]),
                float(i[5])
            )
            self.mysql_client.insert_data_line(db_name=db_name,
                                               table_name=table_name,
                                               column_names=column_names,
                                               columnn_values=columnn_values)
            # print(raw_data)
        return raw_data


    '''tools'''

    def create_db(self):
        self.mycursor.execute('CREATE DATABASE IF NOT EXISTS ' + db_name )

    def create_table(self):
        self.create_db()
        self.mycursor.execute('USE ' + db_name)
        execute_info = f'''CREATE TABLE IF NOT EXISTS `{table_name}` (
                            `Datetime` DATETIME,
                            `high` FLOAT,
                            `open` FLOAT,
                            `low` FLOAT,
                            `close` FLOAT,
                            `amount` FLOAT,
                            PRIMARY KEY ( `Datetime` )
                            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
                        '''
        # print(execute_info)
        self.mycursor.execute(execute_info)

    def split_time_ranges(self, from_time, to_time, frequency):
        from_time, to_time = pd.to_datetime(from_time), pd.to_datetime(to_time)
        time_range = list(pd.date_range(from_time, to_time, freq='%sS' % frequency))
        if to_time not in time_range:
            time_range.append(to_time)
        time_range = [item.strftime("%Y-%m-%d %H:%M:%S") for item in time_range]
        time_ranges = []
        for item in time_range:
            f_time = item
            t_time = (datetime.datetime.strptime(item, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds=frequency))
            if t_time >= to_time:
                t_time = to_time.strftime("%Y-%m-%d %H:%M:%S")
                time_ranges.append([f_time, t_time])
                break
            time_ranges.append([f_time, t_time.strftime("%Y-%m-%d %H:%M:%S")])
        return time_ranges

    def iso2datetime(self, iso):
        iso_str = iso.replace('T', ' ')
        iso_str = iso_str[:19]
        return iso_str

    def datetime2iso(self, datetime):
        aa = self.transfer_datetime_to_timestamp(datetime)
        bb = self.timestamp2datetime(aa)
        return bb

    def timestamp2datetime(self,timestamp):
        datetime_str = str(datetime.datetime.fromtimestamp(timestamp))
        datetime_str = datetime_str.replace(' ', 'T') + '.000Z'
        return datetime_str

    def transfer_datetime_to_timestamp(self, datetime):
        now=time.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        timestamp = int(time.mktime(now))
        return timestamp

    def datetime_plus_eighthour(self, pre_dt):
        aft_dt = datetime.datetime.strptime(pre_dt,"%Y-%m-%d %H:%M:%S")
        t1 = str(aft_dt + datetime.timedelta(hours=8))
        return t1

if __name__ == '__main__':

    test = OKex_data_crawler()

    time_list = test.split_time_ranges(from_time= from_time,
                                       to_time= to_time,
                                       frequency= frequency)
    # print(time_list)
    for i in time_list:
        test.get_k_lines(from_time=i[0],
                         to_time =i[1])
