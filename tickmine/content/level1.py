import datetime
import gzip
import math
import os
import re
import sys

import _pickle as cPickle
import pandas as pd
from tickmine.content.raw_tick import rawtick

pd.set_option('display.max_rows', None)


class Level1():

    def __init__(self):
        self.period2file = {'1T': 'm1', '5T': 'm5', '10T': 'm10', '15T': 'm15', '30T': 'm30', '60T': 'm60', '1D': 'd1'}

    def _save(self, _tradepoint, path):
        _tradepoint.to_csv(path)

    def _find_min_ticksize(self, element_df):
        if 'AskPrice1' in element_df.columns and 'BidPrice1' in element_df.columns:
            temp_diff = element_df['AskPrice1'] - element_df['BidPrice1']
        elif 'AskPrice' in element_df.columns and 'BidPrice' in element_df.columns:
            temp_diff = element_df['AskPrice'] - element_df['BidPrice']

        need_list = [abs(item) for item in temp_diff if item != 0]
        if len(need_list) > 0:
            return min(need_list)
        else:
            return 0.0

    # 确定所有ask-bid_trading_point
    def _ask_bid_trading_point_df(self, element_df, ticksize_):
        if 'AskPrice1' in element_df.columns and 'BidPrice1' in element_df.columns:
            temp_list = [
                row for index, row in element_df.iterrows()
                if math.isclose(abs(row['AskPrice1'] - row['BidPrice1']), ticksize_, rel_tol=0.000001) == True
            ]
            temp_df = pd.DataFrame(temp_list)
            if temp_df.size > 0:
                temp_df['AskPrice1_change'] = temp_df['AskPrice1'].diff()
                ret_df = (temp_df.loc[temp_df['AskPrice1_change'] != 0]).copy()
            else:
                ret_df = pd.Series(data=None, index=None, dtype='float64', name='ask_bid')

        elif 'AskPrice' in element_df.columns and 'BidPrice' in element_df.columns:
            temp_list = [
                row for index, row in element_df.iterrows()
                if math.isclose(abs(row['AskPrice'] - row['BidPrice']), ticksize_, rel_tol=0.000001) == True
            ]
            temp_df = pd.DataFrame(temp_list)
            if temp_df.size > 0:
                temp_df['AskPrice_change'] = temp_df['AskPrice'].diff()
                ret_df = (temp_df.loc[temp_df['AskPrice_change'] != 0]).copy()
                ret_df.rename(columns={
                    'AskPrice': 'AskPrice1',
                    'AskVolume': 'AskVolume1',
                    'BidPrice': 'BidPrice1',
                    'BidVolume': 'BidVolume1'
                },
                              inplace=True)
            else:
                ret_df = pd.Series(data=None, index=None, dtype='float64', name='ask_bid')

        ret_df.index.name = 'Timeindex'
        return ret_df

    def _get_night_date(self, _data):
        ins_time_of_week = pd.to_datetime(_data, format='%Y-%m-%d').dayofweek + 1

        if ins_time_of_week == 1:
            three_day_before = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=-3)
            split = str(three_day_before).split('-')
            night_date = split[0] + split[1] + split[2].split(' ')[0]
        elif 1 < ins_time_of_week <= 5:
            one_day_before = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=-1)
            split = str(one_day_before).split('-')
            night_date = split[0] + split[1] + split[2].split(' ')[0]
        else:
            night_date = ''

        return night_date

    def _get_time_slice(self, _data, _time):
        ret = ['', '']

        if '16:00:00' <= _time[0] <= '24:00:00':
            ret[0] = datetime.datetime.strptime(self._get_night_date(_data) + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[0] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[0] != '':
            ret[0] = datetime.datetime.strptime(_data + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '16:00:00' <= _time[1] <= '24:00:00':
            ret[1] = datetime.datetime.strptime(self._get_night_date(_data) + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[1] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[1] != '':
            ret[1] = datetime.datetime.strptime(_data + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    def _get_time_slice_sina(self, _data, _time):
        ret = ['', '']

        if '00:00:00' <= _time[0] <= '06:00:00':
            one_day_after = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[0] != '':
            ret[0] = datetime.datetime.strptime(_data + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '00:00:00' <= _time[1] <= '06:00:00':
            one_day_after = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[1] != '':
            ret[1] = datetime.datetime.strptime(_data + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    # 只填充exch，提取交易所，只提取ins，提取交易所加合约
    def generate(self, exch, ins, day_date, include_extern_word=False, save_path=''):
        """ 生成可交易点

        一档行情中提取申卖价和申买价相差最小变动单位的作为可交易点。优点： 方便交易 不丢失价格变动趋势 简化数据量

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 日期
            include_extern_word: 是否包含扩展内容。是：可交易时间除了时间外还有深度行情的其他信息 否：可交易时间点只包含时间信息

        Returns:
            返回的数据格式是 dataframe 格式，包含交易点价格等其他信息

        Examples:
            >>> from tickmine.content.level1 import level1
            >>> level1.generate('SHFE', 'MA201', '20211130')
                                BidPrice1  BidVolume1  AskPrice1  AskVolume1
            Timeindex
            2021-11-29 20:59:00     2665.0          91     2666.0           1
            2021-11-29 21:00:00     2666.0           6     2667.0           1
            2021-11-29 21:00:01     2668.0         107     2669.0          92
            2021-11-29 21:00:02     2670.0         210     2671.0         174
            ...
        """
        today_element_df = cPickle.loads(rawtick.get(exch, ins, day_date))

        if today_element_df.size > 1:
            # 提取数据中的ask-bid-pair
            ticksize = self._find_min_ticksize(today_element_df)
            ask_bid_pair_df = self._ask_bid_trading_point_df(today_element_df, ticksize)

            if ask_bid_pair_df.size > 0:
                if include_extern_word == False:
                    if 'BidVolume1' in ask_bid_pair_df.columns and 'AskVolume1' in ask_bid_pair_df.columns:
                        ask_bid_df = ask_bid_pair_df[['BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1']]
                    else:
                        ask_bid_df = ask_bid_pair_df[['BidPrice1', 'AskPrice1']]
                else:
                    ask_bid_df = ask_bid_pair_df

                if save_path != '':
                    if not os.path.exists(save_path):
                        os.makedirs(save_path)

                    _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
                    self._save(ask_bid_df, _path)

                    serialized = cPickle.dumps(ask_bid_df)
                    _path = '%s/%s_%s.pkl' % (save_path, ins, day_date)
                    with gzip.open(_path, 'wb', compresslevel=1) as file_object:
                        file_object.write(serialized)
                        file_object.close()

            else:
                ask_bid_df = ask_bid_pair_df
        else:
            ask_bid_df = pd.Series(data=None, index=None, dtype='float64', name='ask_bid')

        return ask_bid_df

    def generate_all(self, keyword='', data_type=''):
        for year in os.listdir('/share/database/reconstruct/tick'):
            year_exch_day_path = '/share/database/reconstruct/tick' + "/" + year
            for exch in os.listdir(year_exch_day_path):
                exch_day_path = year_exch_day_path + "/" + exch + "/" + exch
                for ins in os.listdir(exch_day_path):
                    if data_type == 'future' and len(ins) > 6:
                        continue
                    if data_type == 'option' and len(ins) <= 6:
                        continue
                    ins_path = exch_day_path + "/" + ins
                    for ins_data in os.listdir(ins_path):
                        ins_data_path = ins_path + "/" + ins_data
                        if keyword in ins_data_path:
                            dir_path = '/share/database/naturedata/tradepoint/%s/askbidpair/%s/%s' % (year, exch, ins)
                            day_date = ins_data.split('.')[0].split('_')[-1]
                            print('level1 generate %s %s %s' % (exch, ins, day_date))
                            self.generate(exch, ins, day_date, save_path=dir_path)

    def _get_year(self, exch, ins):
        name_split = re.split('([0-9]+)', ins)
        yearstr = ''
        if len(name_split) >= 3:
            _year_ = name_split[1]

            if 'CZCE' == exch:
                if int(_year_[0:1]) > 4:
                    yearstr = '201%s' % _year_[0:1]
                else:
                    yearstr = '202%s' % _year_[0:1]
            else:
                yearstr = '20%s' % _year_[0:2]

        return yearstr

    def get(self, exch, ins, day_date):
        """ 获取特定时间范围内的k线构成的word数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 时间list集合
            time_slice: []
        Returns:
            返回的数据格式是 dataframe 格式，包含ask bid信息

        Examples:
            >>> from tickmine.content.level1 import level1
            >>> level1.get('CZCE', 'MA109', '20210616', ['21:50:00', '22:00:00'])
                                BidPrice1  BidVolume1  AskPrice1  AskVolume1
            Timeindex
            2021-11-29 20:59:00     2665.0          91     2666.0           1
            2021-11-29 21:00:00     2666.0           6     2667.0           1
            2021-11-29 21:00:01     2668.0         107     2669.0          92
            2021-11-29 21:00:02     2670.0         210     2671.0         174
            ...
        """
        yearstr = self._get_year(exch, ins)
        want_file = '/share/database/naturedata/tradepoint/%s/askbidpair/%s/%s/%s_%s.pkl' % (yearstr, exch, ins, ins, day_date)

        try:
            with gzip.open(want_file, 'rb', compresslevel=1) as file_object:
                return file_object.read()
        except:
            element_df = pd.DataFrame(columns=['BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1'])
            serialized = cPickle.dumps(element_df)
            return serialized


level1 = Level1()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        level1.generate_all(sys.argv[1])
    elif len(sys.argv) == 3:
        level1.generate_all(sys.argv[1], sys.argv[2])
    else:
        level1.generate_all()
