import pandas as pd
import os
import sys
import pickle
import datetime

if os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path
    from tickmine.global_config import citic_nature_path as nature_path
elif os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
    from tickmine.global_config import tsaodai_nature_path as nature_path
elif os.environ.get('database') == 'sina':
    from tickmine.global_config import sina_dst_path as database_path
    from tickmine.global_config import sina_nature_path as nature_path

from tickmine.content.trade_point import tradepoint
from tickmine.content.raw_tick import rawtick

class K_line():
    def __init__(self):
        self.period2file = {'1T':'m1', '5T':'m5', '10T':'m10', '15T':'m15', '30T':'m30', '60T':'m60', '1D':'d1', '1W':'w1'}

    def _mkdir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def _save(self, ohlcv, path):
        ohlcv.to_csv(path)

    def _get_night_date(self, _data):
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
            ret[0] = datetime.datetime.strptime(self._get_night_date(_data) + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[0] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format = '%Y-%m-%d') + datetime.timedelta(days = 1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[0] != '':
            ret[0] = datetime.datetime.strptime(_data+_time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '16:00:00' <= _time[1] <= '24:00:00':
            ret[1] = datetime.datetime.strptime(self._get_night_date(_data) + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[1] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format = '%Y-%m-%d') + datetime.timedelta(days = 1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[1] != '':
            ret[1] = datetime.datetime.strptime(_data+_time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    def _get_time_slice_sina(self, _data, _time):
        ret = ['', '']

        if '00:00:00' <= _time[0] <= '06:00:00':
            one_day_after = pd.to_datetime(_data, format = '%Y-%m-%d') + datetime.timedelta(days = 1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[0] != '':
            ret[0] = datetime.datetime.strptime(_data+_time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '00:00:00' <= _time[1] <= '06:00:00':
            one_day_after = pd.to_datetime(_data, format = '%Y-%m-%d') + datetime.timedelta(days = 1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[1] != '':
            ret[1] = datetime.datetime.strptime(_data+_time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    def _t_period_extension(self, exch, ins, day_date, save_path=''):
        ret = pickle.loads(self.get(exch, ins, day_date, period='1T'))
        if ret.size > 0:
            _open = ret['Open'].resample('60T', closed = 'right', label='right').first()
            _high = ret['High'].resample('60T', closed = 'right', label='right').max()
            _low = ret['Low'].resample('60T', closed = 'right', label='right').min()
            _close = ret['Close'].resample('60T', closed = 'right', label='right').last()
            _volume = ret['Volume'].resample('60T', closed = 'right', label='right').sum()
            _openinterest = ret['OpenInterest'].resample('60T', closed = 'right', label='right').sum()

            ohlcv = pd.concat([_open, _high, _low, _close, _volume, _openinterest], axis=1).dropna()

            if save_path != '':
                if not os.path.exists(save_path):
                    os.makedirs(save_path)

                _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
                self._save(ohlcv, _path)

            return ohlcv

    def _d_period_extension(self, exch, ins, day_date, save_path=''):
        # 判断是否是周五，周五才提取
        ins_time_of_week = pd.to_datetime(day_date, format = '%Y-%m-%d').dayofweek + 1
        if ins_time_of_week == 5:
            date_list = pd.date_range(periods=5, end=day_date).strftime("%Y%m%d").tolist()
            ndates_df = pd.DataFrame(columns = ["Open", "High", "Low", "Close", "Volume", "OpenInterest"])
            ohlcv = pd.DataFrame({'Open':[], 'High':[],'Low':[],'Close':[],'Volume':[], 'OpenInterest':[]})
            for item in date_list:
                kline_data = pickle.loads(self.get(exch, ins, item, period = '1D', subject='lastprice'))
                if len(kline_data) > 0:
                    ndates_df = pd.concat([ndates_df, kline_data])

            if len(ndates_df) > 0:
                ndates_df = ndates_df.dropna()
                ndates_df = ndates_df.sort_index()

                ohlcv['Open'] = [ndates_df['Open'][0]]
                ohlcv['High'] = [max(ndates_df['High'])]
                ohlcv['Low'] = [min(ndates_df['Low'])]
                ohlcv['Close'] = [ndates_df['Close'][-1]]
                ohlcv['Volume'] = [sum(ndates_df['Volume'])]
                ohlcv['OpenInterest'] = [ndates_df['OpenInterest'][-1]]
                ohlcv.index = [ndates_df.index[-1].date()]
                ohlcv.index.name = 'Timeindex'

                if save_path != '':
                    if not os.path.exists(save_path):
                        os.makedirs(save_path)

                    _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
                    self._save(ohlcv, _path)

                return ohlcv

    def _get_Ts_k_line(self, exch, ins, day_date, period='1T', subject='lastprice', save_path=''):
        if subject == 'tradepoint':
            today_element_df = pickle.loads(tradepoint.get(exch, ins, day_date))
        else:
            today_element_df = pickle.loads(rawtick.get(exch, ins, day_date))

        if today_element_df.size > 0:
            if subject == 'tradepoint':
                bars = today_element_df['trading_point'].resample(period, label='right').ohlc()
            elif subject == 'lastprice':
                bars = today_element_df['LastPrice'].resample(period, label='right').ohlc()

            if ('TradeVolume' in today_element_df.columns or 'Volume' in today_element_df.columns) and 'OpenInterest' in  today_element_df.columns:
                if 'TradeVolume' in today_element_df.columns:
                    volumes = today_element_df['TradeVolume'].resample(period, label='right').last() \
                        - today_element_df['TradeVolume'].resample(period, label='right').first()
                elif 'Volume' in today_element_df.columns:
                    volumes = today_element_df['Volume'].resample(period, label='right').last() \
                        - today_element_df['Volume'].resample(period, label='right').first()

                openInterest = today_element_df['OpenInterest'].resample(period, label='right').last() \
                    - today_element_df['OpenInterest'].resample(period, label='right').first()

                ohlcv = pd.concat([bars, volumes, openInterest], axis=1)
                if 'Volume' in ohlcv.columns:
                    ohlcv = ohlcv[ohlcv['Volume'] > 0].dropna()
                    ohlcv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
                elif 'TradeVolume' in ohlcv.columns:
                    ohlcv = ohlcv[ohlcv['TradeVolume'] > 0].dropna()
                    ohlcv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'TradeVolume': 'Volume'}, inplace=True)
            else:
                ohlcv = pd.concat([bars], axis=1)
                ohlcv = ohlcv.dropna()
                ohlcv.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
        else:
            ohlcv = pd.DataFrame({'Open':[], 'High':[],'Low':[],'Close':[],'Volume':[], 'OpenInterest':[]})
            ohlcv.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
            self._save(ohlcv, _path)

        return ohlcv

    def _get_1D_k_line(self, exch, ins, day_date, period='1D', subject='lastprice', save_path=''):
        if subject == 'tradepoint':
            today_element_df = pickle.loads(tradepoint.get(exch, ins, day_date))
        else:
            today_element_df = pickle.loads(rawtick.get(exch, ins, day_date))

        ohlcv = pd.DataFrame({'Open':[], 'High':[],'Low':[],'Close':[],'Volume':[], 'OpenInterest':[]})

        if today_element_df.size > 0:
            if subject == 'lastprice':
                if 'OpenPrice' in today_element_df.columns:
                    ohlcv['Open'] = [today_element_df['OpenPrice'][-1]]
                else:
                    ohlcv['Open'] = [today_element_df['LastPrice'][0]]
                ohlcv['High'] = [max(today_element_df['LastPrice'])]
                ohlcv['Low'] = [min(today_element_df['LastPrice'])]
                ohlcv['Close'] = [today_element_df['LastPrice'][-1]]
            elif subject == 'tradepoint':
                ohlcv['Open'] = [today_element_df['trading_point'][0]]
                ohlcv['High'] = [max(today_element_df['trading_point'])]
                ohlcv['Low'] = [min(today_element_df['trading_point'])]
                ohlcv['Close'] = [today_element_df['trading_point'][-1]]

            if ('TradeVolume' in today_element_df.columns or 'Volume' in today_element_df.columns) and 'OpenInterest' in  today_element_df.columns:
                if 'Volume' in today_element_df.columns:
                    ohlcv['Volume'] = [today_element_df['Volume'][-1]]
                elif 'TradeVolume' in today_element_df.columns:
                    ohlcv['Volume'] = [today_element_df['TradeVolume'][-1]]
                ohlcv['OpenInterest'] = [today_element_df['OpenInterest'][-1]]
            else:
                del ohlcv['Volume']
                del ohlcv['OpenInterest']

            if exch == 'global':
                ohlcv.index=[today_element_df.index[0].date()]
            else:
                ohlcv.index=[today_element_df.index[-1].date()]
            ohlcv.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
            self._save(ohlcv, _path)

        return ohlcv

    def _get_pair_Ts_k_line(self, exch1, ins1, exch2, ins2, day_date, period, subject, save_path):
        if subject == 'tradepoint':
            today_element_df1 = pickle.loads(tradepoint.get(exch1, ins1, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["tradepoint"]))
            today_element_df2 = pickle.loads(tradepoint.get(exch2, ins2, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["tradepoint"]))
            today_element_df = (today_element_df1['tradepoint'] - today_element_df2['tradepoint']).fillna(method='ffill').dropna()
        else:
            today_element_df1 = pickle.loads(rawtick.get(exch1, ins1, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["LastPrice"]))
            today_element_df2 = pickle.loads(rawtick.get(exch2, ins2, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["LastPrice"]))
            today_element_df = (today_element_df1['LastPrice'] - today_element_df2['LastPrice']).fillna(method='ffill').dropna()

        if today_element_df.size > 0:
            if subject == 'tradepoint':
                ohlc = today_element_df.resample(period, label='right').ohlc().dropna(axis=0, subset = ["close"])
            elif subject == 'lastprice':
                ohlc = today_element_df.resample(period, label='right').ohlc().dropna(axis=0, subset = ["close"])

            ohlc.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
        else:
            ohlc = pd.DataFrame({'Open':[], 'High':[],'Low':[],'Close':[]})
            ohlc.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s_%s.csv' % (save_path, ins1, ins2, day_date)
            self._save(ohlc, _path)

        return ohlc

    def _get_pair_1D_k_line(self, exch1, ins1, exch2, ins2, day_date, period, subject, save_path):
        if subject == 'tradepoint':
            today_element_df1 = pickle.loads(tradepoint.get(exch1, ins1, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["tradepoint"]))
            today_element_df2 = pickle.loads(tradepoint.get(exch2, ins2, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["tradepoint"]))
            today_element_df = (today_element_df1['tradepoint'] - today_element_df2['tradepoint']).fillna(method='ffill').dropna()
        else:
            today_element_df1 = pickle.loads(rawtick.get(exch1, ins1, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["LastPrice"]))
            today_element_df2 = pickle.loads(rawtick.get(exch2, ins2, day_date).resample('1T', label='right').last().dropna(axis=0, subset = ["LastPrice"]))
            today_element_df = (today_element_df1['LastPrice'] - today_element_df2['LastPrice']).fillna(method='ffill').dropna()

        if today_element_df.size > 0:
            ohlc = pd.DataFrame({'Open':[today_element_df[0]], 'High':[max(today_element_df)],'Low':[min(today_element_df)],'Close':[today_element_df[-1]]})
            ohlc.index = [today_element_df.index[-1].date()]
            ohlc.index.name = 'Timeindex'
        else:
            ohlc = pd.DataFrame({'Open':[], 'High':[],'Low':[],'Close':[]})
            ohlc.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s_%s.csv' % (save_path, ins1, ins2, day_date)
            self._save(ohlc, _path)

        return ohlc

    def generate(self, exch, ins, day_date, period='1T', subject='lastprice', save_path=''):
        """ k 线生成

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 日期
            period: K 线周期 例如 1T 5T 10T 15T 30T 60T 1D
            subject: lastprice 以最新价格提取k线；tradepoint 以可交易点提取k线
            save_path: 默认不保存数据，如果填写路径的话，会将k线数据以图片的形式保存下来
        Returns:
            返回的数据格式是 dataframe 格式，包含K线信息

        Examples:
            >>> from tickmine.content.k_line import kline
            >>> kline.generate('DCE', 'c2105', '20210512', '10T', 'lastprice', True)
                                Open    High     Low   Close  Volume
            Timeindex
            2021-05-11 21:20:00  2800.0  2820.0  2800.0  2820.0   150.0
            2021-05-12 09:10:00  2820.0  2825.0  2810.0  2810.0     2.0
            2021-05-12 09:20:00  2810.0  2820.0  2810.0  2820.0    90.0
            2021-05-12 09:30:00  2820.0  2820.0  2820.0  2820.0   110.0
            2021-05-12 09:40:00  2820.0  2820.0  2820.0  2820.0    50.0
            2021-05-12 09:50:00  2820.0  2820.0  2820.0  2820.0    51.0
            2021-05-12 10:00:00  2820.0  2820.0  2814.0  2820.0    51.0
            2021-05-12 10:10:00  2820.0  2820.0  2820.0  2820.0    50.0
            2021-05-12 10:40:00  2820.0  2820.0  2820.0  2820.0    49.0
            2021-05-12 11:10:00  2820.0  2835.0  2820.0  2835.0   318.0
        """
        if 'T' in period:
            return self._get_Ts_k_line(exch, ins, day_date, period, subject, save_path)
        elif 'D' in period:
            return self._get_1D_k_line(exch, ins, day_date, period, subject, save_path)

    def generate_pair(self, exch1, ins1, exch2, ins2, day_date, period='1T', subject='lastprice', save_path=''):
        """ 合约对 K线生成

        Args:
            exch1: 交易所简称
            ins1: 合约代码
            exch2: 交易所简称
            ins2: 合约代码
            day_date: 日期
            period: K 线周期 例如 1T 5T 10T 15T 30T 60T 1D
            subject: lastprice 以最新价格提取k线；tradepoint 以可交易点提取k线
            save_path: 默认不保存数据，如果填写路径的话，会将k线数据以图片的形式保存下来
        Returns:
            返回的数据格式是 dataframe 格式，包含K线信息

        Examples:
            >>> from tickmine.content.k_line import kline
            >>> kline.generate_pair('DCE', 'c2105', 'DCE', 'm2105', '20210512', '10T', 'lastprice', True)
                                Open   High    Low  Close
            Timeindex
            2021-04-09 21:10:00 -725.0 -724.0 -733.0 -733.0
            2021-04-09 21:20:00 -732.0 -731.0 -736.0 -734.0
            2021-04-09 21:30:00 -735.0 -730.0 -740.0 -730.0
            2021-04-09 21:40:00 -730.0 -730.0 -738.0 -736.0
            2021-04-09 21:50:00 -736.0 -730.0 -737.0 -736.0
            2021-04-09 22:00:00 -737.0 -727.0 -737.0 -729.0
            2021-04-09 22:10:00 -730.0 -727.0 -733.0 -729.0
            2021-04-09 22:20:00 -727.0 -727.0 -733.0 -729.0
            2021-04-09 22:30:00 -729.0 -728.0 -732.0 -730.0
        """
        if 'T' in period:
            return self._get_pair_Ts_k_line( exch1, ins1, exch2, ins2, day_date, period, subject, save_path)
        elif 'D' in period:
            return self._get_pair_1D_k_line( exch1, ins1, exch2, ins2, day_date, period, subject, save_path)

    def generate_all(self, keyword = '', inclde_option = 'no'):
        for exch in os.listdir(database_path):
            exch_day_path = database_path + "/" + exch + "/" + exch
            for ins in os.listdir(exch_day_path):
                if inclde_option == 'no' and len(ins) > 6:
                    continue
                ins_path = exch_day_path + "/" + ins
                for ins_data in os.listdir(ins_path):
                    ins_data_path = ins_path + "/" + ins_data
                    if keyword in ins_data_path:
                        day_date = ins_data.split('.')[0].split('_')[-1]
                        print('kline generate %s %s %s'%(exch, ins, day_date))
                        dir_path = '%s/%s/%s_kline/%s/%s'%(nature_path, 'lastprice', self.period2file['1T'], exch, ins)
                        self.generate(exch, ins, day_date, '1T', subject='lastprice', save_path=dir_path)
                        dir_path = '%s/%s/%s_kline/%s/%s'%(nature_path, 'lastprice', self.period2file['1D'], exch, ins)
                        self.generate(exch, ins, day_date, '1D', subject='lastprice', save_path=dir_path)

    def multiperiod_extension(self):
        m1_nature_path = '%s/%s/%s_kline'%(nature_path, 'lastprice', self.period2file['1T'])
        for exch in os.listdir(m1_nature_path):
            exch_path = m1_nature_path + "/" + exch
            for ins in os.listdir(exch_path):
                if len(ins) > 6:
                    continue
                ins_path = exch_path + "/" + ins
                for ins_date in os.listdir(ins_path):
                    # ins_data_path = ins_path + "/" + ins_date
                    day_date = ins_date.split('.')[0].split('_')[-1]
                    print('kline generate %s %s %s'%(exch, ins, day_date))
                    dir_path = '%s/%s/%s_kline/%s/%s'%(nature_path, 'lastprice', self.period2file['60T'], exch, ins)
                    self._t_period_extension(exch, ins, day_date, save_path=dir_path)

        d1_nature_path = '%s/%s/%s_kline'%(nature_path, 'lastprice', self.period2file['1D'])
        for exch in os.listdir(d1_nature_path):
            exch_path = d1_nature_path + "/" + exch
            for ins in os.listdir(exch_path):
                if len(ins) > 6:
                    continue
                ins_path = exch_path + "/" + ins
                for ins_date in os.listdir(ins_path):
                    # ins_data_path = ins_path + "/" + ins_date
                    day_date = ins_date.split('.')[0].split('_')[-1]
                    print('kline generate %s %s %s'%(exch, ins, day_date))
                    dir_path = '%s/%s/%s_kline/%s/%s'%(nature_path, 'lastprice', self.period2file['1W'], exch, ins)
                    self._d_period_extension(exch, ins, day_date, save_path=dir_path)

    def get(self, exch, ins, day_date, time_slice=[], period = '1T', subject='lastprice'):
        """ 获取特定时间范围内的k线构成的word数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            period: 提取数据的周期，默认是一分钟周期
            day_date: 时间list集合
        Returns:
            返回的数据格式是 dataframe 格式，包含word信息

        Examples:
            >>> from tickmine.content.k_line import kline
            >>> kline.get('CZCE', 'MA901', '20180802', '10T')
                        Open    High     Low   Close  Volume  OpenInterest
            Timeindex
            2018-08-02  3015.0  3096.0  3010.0  3093.0  292132      320158.0
            2018-08-03  3089.0  3200.0  3077.0  3200.0  466340      370216.0
        """
        root_path = '%s/%s/%s_kline/%s/%s'%(nature_path, subject, self.period2file[period], exch, ins)
        want_file_list = os.path.join(root_path, '%s_%s.csv'%(ins, day_date))
        file_data = pd.DataFrame(columns = ["Timeindex", "Open", "High", "Low", "Close", "Volume", "OpenInterest"])

        try:
            file_data = pd.read_csv(want_file_list)
            file_data.index = pd.to_datetime(file_data['Timeindex'])
            file_data = file_data.sort_index()
            file_data.pop('Timeindex')

            if len(time_slice) == 2:
                if os.environ.get('database') == 'sina':
                    _time_slice = self._get_time_slice_sina(day_date, time_slice)
                else:
                    _time_slice = self._get_time_slice(day_date, time_slice)
                file_data = file_data.truncate(before = _time_slice[0], after = _time_slice[1])
        except:
            pass

        return pickle.dumps(file_data)

kline = K_line()

if __name__=="__main__":
    if len(sys.argv) == 2:
        if 'extension' == sys.argv[1]:
            kline.multiperiod_extension()
        else:
            kdata = kline.generate_all(sys.argv[1])
    elif len(sys.argv) == 3:
        kdata = kline.generate_all(sys.argv[1], sys.argv[2])
    else:
        kdata = kline.generate_all()
