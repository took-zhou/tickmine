import os
import datetime
import re

if os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as database_path
elif os.environ.get('database') == 'citic':
    from tickmine.global_config import citic_dst_path as database_path
from tickmine.global_config import sina_dst_path as sina_database_path

class Info:
    def __init__(self):
        pass

    def get_date(self, exch, ins):
        """ 合约过去的交易日获取

        Args:
            exch: 交易所简称
            ins: 合约代码

        Returns:
            返回的数据类型是 list， 包含所有的日期数据

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_date('DCE', 'c2105')
            ['20200716', '20210205', ... '20200902', '20210428', '20210506', '20210426']
        """
        ret = []
        if exch == 'global':
            self.absolute_path = '%s/%s/%s/%s'%(sina_database_path, exch, exch, ins)
        else:
            self.absolute_path = '%s/%s/%s/%s'%(database_path, exch, exch, ins)

        if os.path.exists(self.absolute_path) == False:
            sorted_data = []
        else:
            for item in os.listdir(self.absolute_path):
                datastr = item.split('_')[-1].split('.')[0]
                if datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 6 and datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 7:
                    ret.append(datastr)

            sorted_data = sorted(ret)
        return sorted_data

    def get_instrument(self, exch, special_type=''):
        """ 交易所过去的合约提取

        Args:
            exch: 交易所简称
            special_type: 指定品种

        Returns:
            返回的数据类型是 list， 包含该交易所下面所有的合约

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_instrument('DCE')
           ['c2109', 'pg2109', ... 'jm2105', 'pp2007', 'pp2111', 'eb2204']
        """
        if exch == 'global':
            self.absolute_path = '%s/%s/%s'%(sina_database_path, exch, exch)
            instrument_list = os.listdir(self.absolute_path)
            ret_list = [item for item in instrument_list if (special_type == '' or special_type == ''.join(re.findall(r'[A-Za-z]', item)))]
            ret_list.sort()
        else:
            self.absolute_path = '%s/%s/%s'%(database_path, exch, exch)
            instrument_list = os.listdir(self.absolute_path)
            if exch == 'CZCE':
                ret_list1 = [item for item in instrument_list if (special_type == '' or special_type == ''.join(re.findall(r'[A-Za-z]', item))) and '5' <= item[-3] <= '9']
                ret_list2 = [item for item in instrument_list if (special_type == '' or special_type == ''.join(re.findall(r'[A-Za-z]', item))) and '0' <= item[-3] < '5']
                ret_list1.sort()
                ret_list2.sort()
                ret_list = ret_list1 + ret_list2
            else:
                ret_list = [item for item in instrument_list if (special_type == '' or special_type == ''.join(re.findall(r'[A-Za-z]', item)))]
                ret_list.sort()
        return ret_list

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
        exch_list = os.listdir(database_path)
        sina_list = os.listdir(sina_database_path)
        exch_list = exch_list + sina_list

        return [item for item in exch_list if 'night' not in item]

info = Info()
