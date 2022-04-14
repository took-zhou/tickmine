import pandas as pd
import os
import sys
import pickle
import numpy as np

from tickmine.content.k_line import kline
from tickmine.content.info import info

if os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path
    from tickmine.global_config import citic_nature_path as nature_path
elif os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
    from tickmine.global_config import tsaodai_nature_path as nature_path
elif os.environ.get('database') == 'sina':
    from tickmine.global_config import sina_dst_path as database_path
    from tickmine.global_config import sina_nature_path as nature_path

class M_line():
    def __init__(self):
        pass

    def _save(self, ohlcv, path):
        ohlcv.to_csv(path)

    def generate(self, exch, ins, day_data, save_path=''):
        """ m 线生成

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_data: 日期
            save_path: 默认不保存数据，如果填写路径的话，会将m线数据以图片的形式保存下来
        Returns:
            返回的数据格式是 dataframe 格式，包含m线信息

        Examples:

        """
        ndates_df = pd.DataFrame(columns = ["Open", "High", "Low", "Close", "Volume", "OpenInterest"])
        ma_df = pd.DataFrame({'MA5':[], 'MA10':[],'MA20':[],'MA30':[]})
        date_list = info.get_date(exch, ins)
        temp_list = [item for item in date_list if item <= day_data]

        for item in temp_list[-30:]:
            kline_data = pickle.loads(kline.get(exch, ins, item, period='1D'))
            ndates_df = ndates_df.append(kline_data)
        ndates_df = ndates_df.sort_index()

        if len(ndates_df) >= 1:
            ma_df['MA5'] = [np.mean(ndates_df.Close[-5:])]
            ma_df['MA10'] = [np.mean(ndates_df.Close[-10:])]
            ma_df['MA20'] = [np.mean(ndates_df.Close[-20:])]
            ma_df['MA30'] = [np.mean(ndates_df.Close[-30:])]
            ma_df.index = [ndates_df.index[-1].date()]
            ma_df.index.name = 'Timeindex'

        if save_path != '':
            if not os.path.exists(save_path):
                os.makedirs(save_path)

            _path = '%s/%s_%s.csv' % (save_path, ins, day_data)
            self._save(ma_df, _path)

        return ma_df

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
                        day_data = ins_data.split('.')[0].split('_')[-1]
                        print('mline generate %s %s %s'%(exch, ins, day_data))
                        dir_path = '%s/%s/ma_line/%s/%s'%(nature_path, 'lastprice', exch, ins)
                        self.generate(exch, ins, day_data, save_path=dir_path)

    def get(self, exch, ins, day_data):
        """ 获取特定时间范围内的均线构成的df数据

        Args:
            exch: 交易所简称
            ins: 合约代码
            day_data: 交易日
        Returns:
            返回的数据格式是 dataframe 格式，包含均线信息

        Examples:
            >>> from tickmine.content.m_line import mline
            >>> mline.get('CZCE', 'MA901', '20180802')
                        MA5    MA10     MA20    MA30
            Timeindex
            2018-08-02  3046.4  3025.8  2985.25  2951.7
        """
        root_path = '%s/lastprice/ma_line/%s/%s'%(nature_path, exch, ins)
        want_file_list = os.path.join(root_path, '%s_%s.csv'%(ins, day_data))
        file_data = pd.DataFrame(columns = ["Timeindex", "MA5", "MA10", "MA20", "MA30"])

        try:
            file_data = pd.read_csv(want_file_list)
            file_data.index = pd.to_datetime(file_data['Timeindex'])
            file_data = file_data.sort_index()
            file_data.pop('Timeindex')
        except:
            pass

        return pickle.dumps(file_data)

mline = M_line()

if __name__=="__main__":
    if len(sys.argv) == 2:
        mdata = mline.generate_all(sys.argv[1])
    if len(sys.argv) == 3:
        mdata = mline.generate_all(sys.argv[1], sys.argv[2])
    else:
        mdata = mline.generate_all()
