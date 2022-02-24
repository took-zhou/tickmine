from re import sub
import pandas as pd
import datetime
import os
import math
import numpy as np
import pickle
import sys

if os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path
    from tickmine.global_config import citic_nature_path as nature_path
elif os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
    from tickmine.global_config import tsaodai_nature_path as nature_path

from tickmine.content.raw_tick import rawtick

pd.set_option('display.max_rows', None)

class tradePoint():
    def __init__(self):
        self.period2file = {'1T':'m1', '5T':'m5', '10T':'m10', '15T':'m15', '30T':'m30', '60T':'m60', '1D':'d1'}

    def _save(self, _tradepoint, path):
        _tradepoint.to_csv(path)

    def _find_min_ticksize(self, element_df):
        temp_diff = np.diff(element_df)
        need_list = [abs(item) for item in temp_diff if item != 0]
        if len(need_list) > 0:
            return min(need_list)
        else:
            return 0.0

    # 确定所有ask-bid_trading_point
    def _ask_bid_trading_point_df(self, element_df, ticksize_):
        if 'AskPrice1' in element_df.columns and 'BidPrice1' in element_df.columns:
            temp_list = [row for index, row in element_df.iterrows() if math.isclose(abs(row['AskPrice1']-row['BidPrice1']), ticksize_, rel_tol=0.000001) == True]
            temp_df = pd.DataFrame(temp_list)
            if temp_df.size > 0:
                temp_df['AskPrice1_change'] = temp_df['AskPrice1'].diff()
                up_df = (temp_df.loc[temp_df['AskPrice1_change'] > 0]).copy()
                down_df = (temp_df.loc[temp_df['AskPrice1_change'] < 0]).copy()

                up_df['trading_point'] = up_df['AskPrice1']
                down_df['trading_point'] = down_df['BidPrice1']
                ret_df = up_df.append(down_df).sort_index()
            else:
               ret_df = pd.Series(data=None, index=None, dtype='float64', name='trading_point')


        elif 'AskPrice' in element_df.columns and 'BidPrice' in element_df.columns:
            temp_list = [row for index, row in element_df.iterrows() if math.isclose(abs(row['AskPrice']-row['BidPrice']), ticksize_, rel_tol=0.000001) == True]
            temp_df = pd.DataFrame(temp_list)
            if temp_df.size > 0:
                temp_df['AskPrice_change'] = temp_df['AskPrice'].diff()
                up_df = (temp_df.loc[temp_df['AskPrice_change'] > 0]).copy()
                down_df = (temp_df.loc[temp_df['AskPrice_change'] < 0]).copy()

                up_df['trading_point'] = up_df['AskPrice']
                down_df['trading_point'] = down_df['BidPrice']
                ret_df = up_df.append(down_df).sort_index()
            else:
                ret_df = pd.Series(data=None, index=None, dtype='float64', name='trading_point')

        ret_df.index.name = 'Timeindex'
        return ret_df

    def _get_night_data(self, _data):
        ins_time_of_week = pd.to_datetime(_data, format = '%Y-%m-%d').dayofweek + 1

        if ins_time_of_week == 1:
            three_day_before = pd.to_datetime(_data, format = '%Y-%m-%d') + datetime.timedelta(days = -3)
            split = str(three_day_before).split('-')
            night_date = split[0] + split[1] + split[2].split(' ')[0]
        elif 1 < ins_time_of_week <= 5:
            one_day_before = pd.to_datetime(_data, format = '%Y-%m-%d') + datetime.timedelta(days = -1)
            split = str(one_day_before).split('-')
            night_date = split[0] + split[1] + split[2].split(' ')[0]
        else:
            night_date = ''

        return night_date

    def _get_time_slice(self, _data, _time):
        ret = ['', '']

        if '16:00:00' <= _time[0] <= '24:00:00':
            ret[0] = datetime.datetime.strptime(self._get_night_data(_data) + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        else:
            ret[0] = datetime.datetime.strptime(_data+_time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '16:00:00' <= _time[1] <= '24:00:00':
            ret[1] = datetime.datetime.strptime(self._get_night_data(_data) + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        else:
            ret[1] = datetime.datetime.strptime(_data+_time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    # 只填充exch，提取交易所，只提取ins，提取交易所加合约
    def generate(self, exch, ins, day_data, include_extern_word=False, save_path=''):
        """ 生成可交易点

        一档行情中提取申卖价和申买价相差最小变动单位的作为可交易点。优点： 方便交易 不丢失价格变动趋势 简化数据量

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_data: 日期
            include_extern_word: 是否包含扩展内容。是：可交易时间除了时间外还有深度行情的其他信息 否：可交易时间点只包含时间信息

        Returns:
            返回的数据格式是 dataframe 格式，包含交易点价格等其他信息

        Examples:
            >>> from tickmine.content.trade_point import tradepoint
            >>> tradepoint.generate('SHFE', 'cu2109', '20210329')
            Timeindex
            2021-03-26 21:03:47.500    66390.0
            2021-03-26 21:18:04.000    66500.0
            2021-03-26 21:23:11.000    66500.0
            2021-03-26 21:25:18.000    66590.0
            2021-03-26 21:28:22.000    66550.0
                                        ...
            2021-03-29 14:33:34.000    66670.0
            2021-03-29 14:35:57.500    66670.0
            2021-03-29 14:37:55.500    66660.0
            2021-03-29 14:42:43.500    66690.0
            2021-03-29 14:59:42.000    66590.0
            Name: trading_point, Length: 68, dtype: float64
        """
        today_element_df = pickle.loads(rawtick.get(exch, ins, day_data))

        if today_element_df.size > 1:
            # 提取数据中的ask-bid-trading-point
            ticksize = self._find_min_ticksize(today_element_df['LastPrice'])
            today_trading_point_df = self._ask_bid_trading_point_df(today_element_df, ticksize)

            if today_trading_point_df.size > 0:
                if include_extern_word == False:
                    tradepoint_df = today_trading_point_df['trading_point']
                else:
                    tradepoint_df = today_trading_point_df

                if save_path != '':
                    if not os.path.exists(save_path):
                        os.makedirs(save_path)

                    _path = '%s/%s_%s.csv' % (save_path, ins, day_data)
                    self._save(tradepoint_df, _path)
            else:
                tradepoint_df = today_trading_point_df
        else:
            tradepoint_df = pd.Series(data=None, index=None, dtype='float64', name='trading_point')

        return tradepoint_df

    def generate_all(self, keyword = ''):
        for exch in os.listdir(database_path):
            exch_day_path = database_path + "/" + exch + "/" + exch
            for ins in os.listdir(exch_day_path):
                ins_path = exch_day_path + "/" + ins
                for ins_data in os.listdir(ins_path):
                    ins_data_path = ins_path + "/" + ins_data
                    if keyword in ins_data_path:
                        dir_path = '%s/tradepoint/tradepoint/%s/%s'%(nature_path, exch, ins)
                        day_data = ins_data.split('.')[0].split('_')[-1]
                        print('generate %s %s %s'%(exch, ins, day_data))
                        self.generate(exch, ins, day_data, save_path=dir_path)

    def get(self, exch, ins, day_data, time_slice=[]):
        """ 获取特定时间范围内的k线构成的word数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            period: 提取数据的周期，默认是一分钟周期
            day_data: 时间list集合
            time_slice: []
        Returns:
            返回的数据格式是 dataframe 格式，包含tradepoint信息

        Examples:
            >>> from tickmine.content.trade_point import tradepoint
            >>> tradepoint.get('CZCE', 'MA109', '20210616', ['21:50:00', '22:00:00'])
                                trading_point
            Timeindex
            2021-06-15 21:50:02.100    2517.0
            2021-06-15 21:50:24.100    2515.0
            2021-06-15 21:50:58.000    2517.0
            2021-06-15 21:51:04.000    2518.0
            2021-06-15 21:51:18.100    2519.0
            2021-06-15 21:51:23.000    2517.0
            2021-06-15 21:51:40.000    2516.0
            2021-06-15 21:51:41.100    2518.0
            ...
        """
        root_path = '%s/tradepoint/tradepoint/%s/%s'%(nature_path, exch, ins)
        want_file_list = os.path.join(root_path, '%s_%s.csv'%(ins, day_data))
        file_data = pd.DataFrame(columns = ["Timeindex", "trading_point"])
        try:
            file_data = pd.read_csv(want_file_list)
            file_data.index = pd.to_datetime(file_data['Timeindex'])
            file_data = file_data.sort_index()
            file_data.pop('Timeindex')

            if len(time_slice) == 2:
                _time_slice = self._get_time_slice(day_data, time_slice)
                file_data = file_data.truncate(before = _time_slice[0], after = _time_slice[1])
        except:
            pass

        return pickle.dumps(file_data['trading_point'].to_frame())

tradepoint = tradePoint()

if __name__=="__main__":
    if len(sys.argv) == 2:
        tradepoint.generate_all(sys.argv[1])
    else:
        tradepoint.generate_all()
