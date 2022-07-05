import datetime
import os
import re

if os.environ.get('database') == 'tsaodai':
    from tickmine.global_config import tsaodai_dst_path as ctp_database_path
else:
    from tickmine.global_config import citic_dst_path as ctp_database_path

from tickmine.global_config import huaxin_dst_path as huaxin_database_path
from tickmine.global_config import sina_dst_path as sina_database_path


class Info:

    def __init__(self):
        self.huaxin_exch = ['SHSE', 'SZSE']
        self.ctp_exch = ['DCE', 'SHFE', 'CFFEX', 'INE', 'CZCE']

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
            self.absolute_path = '%s/%s/%s/%s' % (sina_database_path, exch, exch, ins)
        elif exch in self.huaxin_exch:
            self.absolute_path = '%s/%s/%s/%s' % (huaxin_database_path, exch, exch, ins)
        else:
            self.absolute_path = '%s/%s/%s/%s' % (ctp_database_path, exch, exch, ins)

        if os.path.exists(self.absolute_path) == False:
            sorted_data = []
        else:
            for item in os.listdir(self.absolute_path):
                datastr = item.split('_')[-1].split('.')[0]
                if datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 6 and datetime.datetime.strptime(
                        datastr, "%Y%m%d").weekday() + 1 != 7:
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
            return self._get_global_instrument(exch, special_type)
        elif exch in self.huaxin_exch:
            return self._get_security(exch, special_type)
        elif '_' in special_type:
            return self._get_option(exch, special_type)
        else:
            return self._get_future(exch, special_type)

    def _get_global_instrument(self, exch, special_type):
        self.absolute_path = '%s/%s/%s' % (sina_database_path, exch, exch)
        instrument_list = os.listdir(self.absolute_path)
        ret_list = []
        for item in instrument_list:
            ins_split = [x.strip() for x in re.split(r'(\d+)', item) if x.strip() != '']
            if (len(ins_split) == 1 and ins_split[0] == special_type) or special_type == '':
                ret_list.append(item)

        ret_list.sort()

        return ret_list

    def _get_future(self, exch, special_type):
        self.absolute_path = '%s/%s/%s' % (ctp_database_path, exch, exch)
        instrument_list = os.listdir(self.absolute_path)
        ret_list1 = []
        ret_list2 = []
        for item in instrument_list:
            ins_split = [x.strip().strip('-') for x in re.split(r'(\d+)', item) if x.strip() != '']
            if len(ins_split) == 2:
                if exch == 'CZCE':
                    if (ins_split[0] == special_type or special_type == '') and '5' <= ins_split[1][0] <= '9':
                        ret_list1.append(item)
                    elif (ins_split[0] == special_type or special_type == '') and '0' <= ins_split[1][0] <= '5':
                        ret_list2.append(item)
                else:
                    if (ins_split[0] == special_type or special_type == ''):
                        ret_list1.append(item)
        ret_list1.sort()
        ret_list2.sort()
        ret_list = ret_list1 + ret_list2

        return ret_list

    def _get_option(self, exch, special_type):
        self.absolute_path = '%s/%s/%s' % (ctp_database_path, exch, exch)
        instrument_list = os.listdir(self.absolute_path)
        special_split = special_type.split('_')
        ret_list1 = []
        ret_list2 = []
        for item in instrument_list:
            ins_split = [x.strip().strip('-') for x in re.split(r'(\d+)', item) if x.strip() != '']
            if len(ins_split) == 4:
                if exch == 'CZCE':
                    if (ins_split[0] == special_split[0]
                            or special_type == '') and '5' <= ins_split[1][0] <= '9' and ins_split[2] == special_split[1]:
                        ret_list1.append(item)
                    elif (ins_split[0] == special_split[0]
                          or special_type == '') and '0' <= ins_split[1][0] <= '5' and ins_split[2] == special_split[1]:
                        ret_list2.append(item)
                else:
                    if (ins_split[0] == special_split[0] or special_type == '') and ins_split[2] == special_split[1]:
                        ret_list1.append(item)
        ret_list1.sort()
        ret_list2.sort()
        ret_list = ret_list1 + ret_list2

        return ret_list

    def _get_security(self, exch, ins=''):
        self.absolute_path = '%s/%s/%s' % (huaxin_database_path, exch, exch)
        instrument_list = os.listdir(self.absolute_path)
        ret_list = []
        for item in instrument_list:
            if len(item) >= 6 and ins == item[0:len(ins)]:
                ret_list.append(item)

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
        exch_list = os.listdir(ctp_database_path)
        sina_list = os.listdir(sina_database_path)
        huaxin_list = os.listdir(huaxin_database_path)
        exch_list = exch_list + sina_list + huaxin_list

        return [item for item in exch_list if 'night' not in item]


info = Info()
