import os
import datetime
from tickmine.global_config import citic_dst_path

class Info:
    def __init__(self):
        pass

    def get_data(self, exch, ins):
        """ 合约过去的交易日获取

        Args:
            exch: 交易所简称
            ins: 合约代码

        Returns:
            返回的数据类型是 list， 包含所有的日期数据

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_data('DCE', 'c2105')
            ['20200716', '20210205', ... '20200902', '20210428', '20210506', '20210426']
        """
        ret = []
        self.absolute_path = '%s/%s/%s/%s'%(citic_dst_path, exch, exch, ins)
        if os.path.exists(self.absolute_path) == False:
            sorted_data = []
        else:
            for item in os.listdir(self.absolute_path):
                datastr = item.split('_')[-1].split('.')[0]
                if datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 6 and datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 7:
                    ret.append(datastr)

            sorted_data = sorted(ret)
        return sorted_data

    def get_instrument(self, exch):
        """ 交易所过去的合约提取

        Args:
            exch: 交易所简称

        Returns:
            返回的数据类型是 list， 包含该交易所下面所有的合约

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_instrument('DCE')
           ['c2109', 'pg2109', ... 'jm2105', 'pp2007', 'pp2111', 'eb2204']
        """
        self.absolute_path = '%s/%s/%s'%(citic_dst_path, exch, exch)
        return os.listdir(self.absolute_path)

    def get_exchange(self):
        """ 交易所过去的合约提取

        Args:
            exch: 交易所简称

        Returns:
            返回的数据类型是 list， 包含该交易所下面所有的合约

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_exchange()
           ['CZCE', 'CFFEX', 'INE', 'SHFE', 'DCE']
        """
        exch_list = os.listdir(citic_dst_path)

        return [item for item in exch_list if 'night' not in item]

info = Info()
