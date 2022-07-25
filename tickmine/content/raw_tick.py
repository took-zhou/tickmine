import datetime
import gzip
import os
import re
import sys

import _pickle as cPickle
import numpy as np
import pandas as pd

if os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
    from tickmine.global_config import tsaodai_nature_path as nature_path
elif os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path
    from tickmine.global_config import citic_nature_path as nature_path
elif os.environ.get('database') == 'sina':
    from tickmine.global_config import sina_dst_path as database_path
    from tickmine.global_config import sina_nature_path as nature_path

pd.set_option('display.max_rows', None)


class rawTick():

    def __init__(self):
        self.leap_month_days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        self.common_month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    def _save(self, _rawtick, path):
        _rawtick.to_csv(path)

    #读取该csv文件对于的分时数据
    def _daytime_raw_data_reading(self, daytime_file_root):
        # 读取白天数据
        subprice_daytime = pd.read_csv(daytime_file_root, encoding='utf-8')
        # print(subprice_daytime)
        subprice = subprice_daytime

        return subprice

    # 读取该csv文件对于的分时数据
    def _nighttime_raw_data_reading(self, nighttime_file_root):
        subprice_nighttime = pd.read_csv(nighttime_file_root, encoding='utf-8')
        # print(subprice_nighttime)
        subprice = subprice_nighttime

        return subprice

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

    # 对已经读取的分时数据设置毫秒级别的时间index
    def _millisecond_timeindex_setting(self, subprice, date):
        # 添加时间index
        # 将'TradingDay','    UpdateTime'和'UpdateMillisec'合并成一个新列
        # 将'UpdateMillisec'的数据类型从int变为str,从而实现列信息的合并
        year_list = []
        month_list = []
        day_list = []
        hour_list = []
        minute_list = []
        second_list = []
        ms_list = []

        # 剔除一个文件中有重复的内容
        if 'UpdateTime' in subprice.columns or 'Time' in subprice.columns:
            if 'UpdateTime' in subprice.columns:
                time_string = subprice['UpdateTime']
            else:
                time_string = subprice['Time']

            for hour_minute_second_ms in time_string.values.tolist():
                hour = hour_minute_second_ms[0:2]
                minute = hour_minute_second_ms[3:5]
                second = hour_minute_second_ms[6:8]
                if len(hour_minute_second_ms) > 9:
                    ms = hour_minute_second_ms[9:]
                else:
                    ms = '000'

                year = date[0:4]
                month = date[4:6]

                # 如果是闰年
                if (int(year) % 4 == 0 and int(year) % 100 != 0) or (int(year) % 400 == 0):
                    if int(hour) <= 3:
                        if self.leap_month_days[int(month) - 1] == int(date[6:]):
                            day = '01'
                            if int(month) == 12:
                                month = '01'
                            else:
                                month = str(int(month) + 1)
                        else:
                            day = str(int(date[6:]) + 1)
                    else:
                        day = date[6:]
                else:
                    if int(hour) <= 3:
                        if self.common_month_days[int(month) - 1] == int(date[6:]):
                            day = '01'
                            if int(month) == 12:
                                month = '01'
                            else:
                                month = str(int(month) + 1)
                        else:
                            day = str(int(date[6:]) + 1)
                    else:
                        day = date[6:]

                year_list.append(year)
                month_list.append(month)
                day_list.append(day)
                hour_list.append(hour)
                minute_list.append(minute)
                second_list.append(second)
                ms_list.append(ms)

        df = pd.DataFrame({
            'year': year_list,
            'month': month_list,
            'day': day_list,
            'hour': hour_list,
            'minute': minute_list,
            'second': second_list,
            'ms': ms_list
        })

        # 将时间dataframe转变为以毫秒为单位的时间列，格式为series
        time_series = pd.to_datetime(df)
        # print(time_series)
        subprice['Timeindex'] = time_series.tolist()
        # 将Timeindex这一列数据设置为index
        subprice = subprice.set_index('Timeindex')

        return subprice

    def _millisecond_timeindex_setting_sina(self, subprice, date):
        time_series = pd.to_datetime(subprice['日期'] + subprice['行情时间'], format='%Y-%m-%d%H:%M:%S')
        subprice['Timeindex'] = time_series.tolist()
        subprice = subprice.set_index('Timeindex')
        return subprice

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

    def _get_citic_path(self, _year):
        citic = []
        citic2 = []

        return ''

    def get_ctp(self, exch, ins, day_date, save_path=''):
        night_date = self._get_night_date(day_date)

        # 2018年2月1号之后的夜市文件名称和日市文件名称相同
        # 20170103 20170104 郑商所数据丢失
        # 20180925 郑商所数据异常（存放的是20180926的数据）
        yearstr = self._get_year(exch, ins)
        if day_date < '20180201' and not ('20161101' <= day_date <= '20161231'):
            ins_daytime_file_root = '%s/%s/%s/%s/%s/%s_%s.csv' % (database_path, yearstr, exch, exch, ins, ins, day_date)
            ins_nighttime_file_root = '%s/%s/%s/%s_night/%s/%s_%s.csv' % (database_path, yearstr, exch, exch, ins, ins, night_date)
        else:
            ins_daytime_file_root = '%s/%s/%s/%s/%s/%s_%s.csv' % (database_path, yearstr, exch, exch, ins, ins, day_date)
            ins_nighttime_file_root = '%s/%s/%s/%s_night/%s/%s_%s.csv' % (database_path, yearstr, exch, exch, ins, ins, day_date)

        # 读取改天白天分时数据
        element_df = pd.DataFrame()

        if os.path.exists(ins_daytime_file_root) == True and os.path.exists(ins_nighttime_file_root) == False:
            subprice = self._daytime_raw_data_reading(ins_daytime_file_root)
            subprice = self._millisecond_timeindex_setting(subprice, day_date)

            if len(subprice) > 0:
                time_wrong_index = [item for item in subprice.index if not ('08:00:00' <= str(item).split(' ')[-1] <= '16:30:00')]
                if len(time_wrong_index) > 0:
                    subprice.drop(time_wrong_index, inplace=True)

            element_df = subprice.copy()
        elif os.path.exists(ins_daytime_file_root) == False and os.path.exists(ins_nighttime_file_root) == True:
            subprice = self._nighttime_raw_data_reading(ins_nighttime_file_root)
            subprice = self._millisecond_timeindex_setting(subprice, night_date)

            if len(subprice) > 0:
                time_wrong_index = [
                    item for item in subprice.index
                    if not ('20:00:00' <= str(item).split(' ')[-1] or str(item).split(' ')[-1] <= '03:30:00')
                ]
                if len(time_wrong_index) > 0:
                    subprice.drop(time_wrong_index, inplace=True)

            element_df = subprice.copy()
        elif os.path.exists(ins_daytime_file_root) == True and os.path.exists(ins_nighttime_file_root) == True:
            subprice = self._daytime_raw_data_reading(ins_daytime_file_root)
            subprice = self._millisecond_timeindex_setting(subprice, day_date)

            if len(subprice) > 0:
                time_wrong_index = [item for item in subprice.index if not ('08:00:00' <= str(item).split(' ')[-1] <= '16:30:00')]
                if len(time_wrong_index) > 0:
                    subprice.drop(time_wrong_index, inplace=True)

            subprice_daytime = subprice.copy()

            subprice = self._nighttime_raw_data_reading(ins_nighttime_file_root)
            subprice = self._millisecond_timeindex_setting(subprice, night_date)

            if len(subprice) > 0:
                time_wrong_index = [
                    item for item in subprice.index
                    if not ('20:00:00' <= str(item).split(' ')[-1] or str(item).split(' ')[-1] <= '03:30:00')
                ]
                if len(time_wrong_index) > 0:
                    subprice.drop(time_wrong_index, inplace=True)

            subprice = subprice.append(subprice_daytime)
            element_df = subprice.copy()

        if len(element_df) > 0:
            if 'AskPrice1' in element_df.columns and 'BidPrice1' in element_df.columns:
                AskPrice1_list = [0 if value_item > 100000000 else value_item for value_item in element_df.AskPrice1]
                BidPrice1_list = [0 if value_item > 100000000 else value_item for value_item in element_df.BidPrice1]
                element_df.AskPrice1 = AskPrice1_list
                element_df.BidPrice1 = BidPrice1_list

                level1_wrong_index = [index for index, row in element_df.iterrows() if row['BidPrice1'] == 0.0 and row['AskPrice1'] == 0.0]
                if len(level1_wrong_index) > 0:
                    element_df.drop(level1_wrong_index, inplace=True)
            elif 'AskPrice' in element_df.columns and 'BidPrice' in element_df.columns:
                AskPrice_list = [0 if value_item > 100000000 else value_item for value_item in element_df.AskPrice]
                BidPrice_list = [0 if value_item > 100000000 else value_item for value_item in element_df.BidPrice]
                element_df.AskPrice = AskPrice_list
                element_df.BidPrice = BidPrice_list

                level1_wrong_index = [index for index, row in element_df.iterrows() if row['BidPrice'] == 0.0 and row['AskPrice'] == 0.0]
                if len(level1_wrong_index) > 0:
                    element_df.drop(level1_wrong_index, inplace=True)

        # 剔除OpenPrice异常的数据
        if element_df.size != 0 and 'OpenPrice' in element_df.columns:
            element_df = element_df[np.isnan(element_df['OpenPrice']) == False]
            element_df = element_df[element_df['OpenPrice'] != 0.0]
            element_df = element_df[element_df['OpenPrice'] <= 100000000]

        # 剔除TradeVolume为0的数据
        if element_df.size != 0 and 'TradeVolume' in element_df.columns:
            element_df = element_df[element_df['TradeVolume'] != 0.0]

        if element_df.size != 0:
            element_df = element_df.sort_index()

            if save_path != '':
                if not os.path.exists(save_path):
                    os.makedirs(save_path)

                serialized = cPickle.dumps(element_df)
                _path = '%s/%s_%s.pkl' % (save_path, ins, day_date)
                with gzip.open(_path, 'wb', compresslevel=1) as file_object:
                    file_object.write(serialized)
                    file_object.close()

        return element_df

    def get_sina(self, exch, ins, day_date, save_path=''):
        ins_daytime_file_root = '%s/%s/%s/%s/%s_%s.csv' % (database_path, exch, exch, ins, ins, day_date)
        element_df = pd.DataFrame()

        if os.path.exists(ins_daytime_file_root) == True:
            # 读取白天数据
            subprice = self._daytime_raw_data_reading(ins_daytime_file_root)

            #print(subprice)
            subprice = self._millisecond_timeindex_setting_sina(subprice, day_date)

            subprice.rename(columns={'名称': 'InstrumentID', '最新价': 'LastPrice', '人民币报价': 'LastPrice(rmb)', '涨跌额': 'Increase', \
                '涨跌幅': 'IncreaseRatio', '开盘价': 'OpenPrice', '最高价': 'HighestPrice', '最低价': 'LowestPrice', '昨日结算价': 'PreSettlementPrice',\
                '持仓量':'OpenInterest', '买价':'BidPrice1', '卖价':'AskPrice1', '行情时间':'UpdateTime', '日期': 'TradingDay'}, inplace=True)

            if subprice.size != 0:
                element_df = subprice.sort_index()

                if save_path != '':
                    if not os.path.exists(save_path):
                        os.makedirs(save_path)

                serialized = cPickle.dumps(element_df)
                _path = '%s/%s_%s.pkl' % (save_path, ins, day_date)
                with gzip.open(_path, 'wb', compresslevel=1) as file_object:
                    file_object.write(serialized)
                    file_object.close()

        return element_df

    def generate(self, exch, ins, day_date, save_path=''):
        """ 获取分数数据

        获取分时数据，打上timeindex标签

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 日期
            include_night: 是否包含夜市数据

        Returns:
            返回的数据格式是 dataframe 格式，包含分数数据信息

        Examples:
            >>> from tickmine.content.raw_tick import rawtick
            >>> rawtick.get('SHFE', 'cu2109', '20210329', include_night=True)
        """
        if os.environ.get('database') == 'sina':
            return self.get_sina(exch, ins, day_date, save_path)
        else:
            return self.get_ctp(exch, ins, day_date, save_path)

    def generate_all(self, keyword='', inclde_option='no'):
        for year in os.listdir(database_path):
            year_exch_day_path = database_path + "/" + year
            for exch in os.listdir(year_exch_day_path):
                exch_day_path = year_exch_day_path + "/" + exch + "/" + exch
                for ins in os.listdir(exch_day_path):
                    if inclde_option == 'no' and len(ins) > 6:
                        continue
                    ins_path = exch_day_path + "/" + ins
                    for ins_data in os.listdir(ins_path):
                        ins_data_path = ins_path + "/" + ins_data
                        if keyword in ins_data_path:
                            day_date = ins_data.split('.')[0].split('_')[-1]
                            print('rawtick generate %s %s %s' % (exch, ins, day_date))
                            yearstr = self._get_year(exch, ins)
                            dir_path = '%s/%s/%s/rawtick/%s/%s' % (nature_path, 'lastprice', yearstr, exch, ins)
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
        """ 获取原始数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 时间list集合
        Returns:
            返回的数据格式是 dataframe 格式

        Examples:
            >>> from tickmine.content.raw_tick import rawtick
            >>> rawtick.get('CZCE', 'MA901', '20180802')
            压缩后的字符流
        """
        yearstr = self._get_year(exch, ins)
        want_file = '%s/%s/%s/rawtick/%s/%s/%s_%s.pkl' % (nature_path, 'lastprice', yearstr, exch, ins, ins, day_date)

        try:
            with gzip.open(want_file, 'rb', compresslevel=1) as file_object:
                return file_object.read()
        except:
            element_df = pd.DataFrame(columns=[
                'InstrumentID', 'TradingDay', 'UpdateTime', 'LastPrice', 'BidPrice1', 'BidVolume1', 'AskPrice1', 'AskVolume1', 'BidPrice2',
                'BidVolume2', 'AskPrice2', 'AskVolume2', 'BidPrice3', 'BidVolume3', 'AskPrice3', 'AskVolume3', 'BidPrice4', 'BidVolume4',
                'AskPrice4', 'AskVolume4', 'BidPrice5', 'BidVolume5', 'AskPrice5', 'AskVolume5', 'Volume', 'Turnover', 'OpenInterest',
                'UpperLimitPrice', 'LowerLimitPrice', 'OpenPrice', 'PreSettlementPrice', 'PreClosePrice', 'PreOpenInterest',
                'SettlementPrice'
            ])
            serialized = cPickle.dumps(element_df)
            return serialized


rawtick = rawTick()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        rawtick.generate_all(sys.argv[1])
    elif len(sys.argv) == 3:
        rawtick.generate_all(sys.argv[1], sys.argv[2])
    else:
        rawtick.generate_all()
