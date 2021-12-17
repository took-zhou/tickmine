from re import sub
import pandas as pd
import datetime
import os
import numpy as np
import pickle

if os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
elif os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path

pd.set_option('display.max_rows', None)

class rawTick():
    def __init__(self):
        self.leap_month_days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30 ,31]
        self.common_month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30 ,31]

    def _save(self, _rawtick, path):
        _rawtick.to_csv(path)

    #读取该csv文件对于的分时数据
    def _daytime_raw_data_reading(self, daytime_file_root):
        # 读取白天数据
        subprice_daytime = pd.read_csv(daytime_file_root, encoding = 'utf-8')
        # print(subprice_daytime)
        subprice = subprice_daytime

        return subprice

    # 读取该csv文件对于的分时数据
    def _nighttime_raw_data_reading(self, nighttime_file_root):
        subprice_nighttime = pd.read_csv(nighttime_file_root, encoding = 'utf-8')
        # print(subprice_nighttime)
        subprice = subprice_nighttime

        return subprice

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
                if (int(year)%4 == 0 and int(year)%100!=0) or (int(year)%400 == 0):
                    if int(hour) <= 3:
                        if self.leap_month_days[int(month)-1] == int(date[6:]):
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
                        if self.common_month_days[int(month)-1] == int(date[6:]):
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

        df = pd.DataFrame({'year': year_list,
                        'month': month_list,
                        'day': day_list,
                        'hour': hour_list,
                        'minute': minute_list,
                        'second': second_list,
                        'ms': ms_list})

        # 将时间dataframe转变为以毫秒为单位的时间列，格式为series
        time_series = pd.to_datetime(df)
        # print(time_series)
        subprice['Timeindex'] = time_series.tolist()
        # 将Timeindex这一列数据设置为index
        subprice = subprice.set_index('Timeindex')

        return subprice

    def get(self, exch, ins, day_data, time_slice=[]):
        """ 获取分数数据

        获取分时数据，打上timeindex标签

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_data: 日期
            include_night: 是否包含夜市数据

        Returns:
            返回的数据格式是 dataframe 格式，包含分数数据信息

        Examples:
            >>> from tickmine.content.raw_tick import rawtick
            >>> rawtick.get('SHFE', 'cu2109', '20210329', include_night=True)
        """
        night_date = self._get_night_data(day_data)

        # 2018年2月1号之后的夜市文件名称和日市文件名称相同
        if day_data < '20180201' and not('20161101' <= day_data <= '20161231'):
            ins_daytime_file_root = '%s/%s/%s/%s/%s_%s.csv'%(database_path, exch, exch, ins, ins, day_data)
            ins_nighttime_file_root = '%s/%s/%s_night/%s/%s_%s.csv'%(database_path, exch, exch, ins, ins, night_date)
        else:
            ins_daytime_file_root = '%s/%s/%s/%s/%s_%s.csv'%(database_path, exch, exch, ins, ins, day_data)
            ins_nighttime_file_root = '%s/%s/%s_night/%s/%s_%s.csv'%(database_path, exch, exch, ins, ins, day_data)

        # 读取改天白天分时数据
        element_df = pd.DataFrame()

        if os.path.exists(ins_daytime_file_root) == True:
            # 读取白天数据
            subprice = self._daytime_raw_data_reading(ins_daytime_file_root)
            # 对已经读取的分时数据设置毫秒级别的时间index
            subprice = self._millisecond_timeindex_setting(subprice, day_data)

            # 剔除时间不对的数据
            if len(subprice) > 0:
                time_wrong_index = [item for item in subprice.index if not('08:00:00' <= str(item).split(' ')[-1] <= '16:30:00')]
                if len(time_wrong_index) > 0:
                    subprice.drop(time_wrong_index, inplace = True)

            subprice_daytime = subprice
            #读取昨天夜晚分时数据,并将昨天夜晚数据和与白天数据合并
            if os.path.exists(ins_nighttime_file_root) == True:
                # 读取夜晚数据
                subprice = self._nighttime_raw_data_reading(ins_nighttime_file_root)
                # 对已经读取的分时数据设置毫秒级别的时间index
                subprice = self._millisecond_timeindex_setting(subprice, night_date)

                # 剔除时间不对的数据
                if len(subprice) > 0:
                    time_wrong_index = [item for item in subprice.index if not('20:00:00' <= str(item).split(' ')[-1] or str(item).split(' ')[-1] <= '03:30:00')]
                    if len(time_wrong_index) > 0:
                        subprice.drop(time_wrong_index, inplace = True)

                subprice_nighttime = subprice
                # 白天数据与晚上数据合并为一个dataframe
                subprice = subprice_nighttime.append(subprice_daytime)
            else:
                subprice = subprice_daytime

            # 剔除OpenPrice异常的数据
            if subprice.size != 0 and 'OpenPrice' in subprice.columns:
                subprice = subprice[np.isnan(subprice['OpenPrice']) == False]
                subprice = subprice[subprice['OpenPrice'] != 0.0]
                subprice = subprice[subprice['OpenPrice'] <= 100000000]

            # 剔除TradeVolume为0的数据
            if subprice.size != 0 and 'TradeVolume' in subprice.columns:
                subprice = subprice[subprice['TradeVolume'] != 0.0]

            if subprice.size != 0:
                element_df = subprice.sort_index()

        if len(time_slice) == 2:
            _time_slice = self._get_time_slice(day_data, time_slice)
            element_df = element_df.truncate(before = _time_slice[0], after = _time_slice[1])

        return pickle.dumps(element_df)

rawtick = rawTick()

if __name__=="__main__":
    points = rawtick.get('DCE', 'p1704', '20170113')
    print(pickle.loads(points))