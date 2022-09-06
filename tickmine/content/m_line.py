import gzip
import os
import re
import sys

import _pickle as cPickle
import numpy as np
import pandas as pd
from tickmine.content.info import info
from tickmine.content.k_line import kline


class M_line():

    def __init__(self):
        pass

    def _save(self, ohlcv, path):
        ohlcv.to_csv(path)

    def generate(self, exch, ins, day_date, save_path=''):
        """ m 线生成

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 日期
            save_path: 默认不保存数据，如果填写路径的话，会将m线数据以图片的形式保存下来
        Returns:
            返回的数据格式是 dataframe 格式，包含m线信息

        Examples:

        """
        ndates_df = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume", "OpenInterest"])
        ma_df = pd.DataFrame({'MA5': [], 'MA10': [], 'MA20': [], 'MA30': []})
        date_list = info.get_date(exch, ins)
        temp_list = [item for item in date_list if item <= day_date]

        for item in temp_list[-30:]:
            kline_data = cPickle.loads(kline.get(exch, ins, item, period='1D'))
            ndates_df = ndates_df.append(kline_data)
        ndates_df = ndates_df.sort_index()

        if len(ndates_df) >= 1:
            ma_df['MA5'] = [np.mean(ndates_df.Close[-5:])]
            ma_df['MA10'] = [np.mean(ndates_df.Close[-10:])]
            ma_df['MA20'] = [np.mean(ndates_df.Close[-20:])]
            ma_df['MA30'] = [np.mean(ndates_df.Close[-30:])]
            ma_df.index = [ndates_df.index[-1]]

            ma_df.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s.csv' % (save_path, ins, day_date)
            self._save(ma_df, _path)

            serialized = cPickle.dumps(ma_df)
            _path = '%s/%s_%s.pkl' % (save_path, ins, day_date)
            with gzip.open(_path, 'wb', compresslevel=1) as file_object:
                file_object.write(serialized)
                file_object.close()

        return ma_df

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
                            day_date = ins_data.split('.')[0].split('_')[-1]
                            print('mline generate %s %s %s' % (exch, ins, day_date))
                            dir_path = '/share/database/naturedata/lastprice/%s/ma_line/%s/%s' % (year, exch, ins)
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
        """ 获取特定时间范围内的均线构成的df数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_date: 交易日
        Returns:
            返回的数据格式是 dataframe 格式，包含均线信息

        Examples:
            >>> from tickmine.content.m_line import mline
            >>> mline.get('CZCE', 'MA901', '20180802')
                        MA5    MA10     MA20    MA30
            Timeindex
            2018-08-02  3046.4  3025.8  2985.25  2951.7
        """
        yearstr = self._get_year(exch, ins)
        want_file = '/share/database/naturedata/lastprice/%s/ma_line/%s/%s/%s_%s.pkl' % (yearstr, exch, ins, ins, day_date)

        try:
            with gzip.open(want_file, 'rb', compresslevel=1) as file_object:
                return file_object.read()
        except:
            element_df = pd.DataFrame(columns=['MA5', 'MA10', 'MA20', 'MA30'])
            serialized = cPickle.dumps(element_df)
            return serialized


mline = M_line()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        mdata = mline.generate_all(sys.argv[1])
    elif len(sys.argv) == 3:
        mdata = mline.generate_all(sys.argv[1], sys.argv[2])
    else:
        mdata = mline.generate_all()
