import datetime
import os
import re


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
        sorted_data = []
        yearstr = self._get_year(exch, ins)
        sp_path = '/share/database/reconstruct/tick/%s/%s/%s/%s' % (yearstr, exch, exch, ins)
        if os.path.exists(sp_path) == True:
            for item in os.listdir(sp_path):
                datastr = item.split('_')[-1].split('.')[0]
                if datetime.datetime.strptime(datastr, "%Y%m%d").weekday() + 1 != 6 and datetime.datetime.strptime(
                        datastr, "%Y%m%d").weekday() + 1 != 7:
                    sorted_data.append(datastr)
        sorted_data.sort()

        return sorted_data

    def get_instrument(self, exch, special_type='', special_date=''):
        """ 交易所过去的合约提取

        Args:
            exch: 交易所简称
            special_type: 指定品种
            special_date: 指定日期
        Returns:
            返回的数据类型是 list, 包含该交易所下面所有的合约

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_instrument('DCE')
           ['c2109', 'pg2109', ... 'jm2105', 'pp2007', 'pp2111', 'eb2204']
        """
        special = []
        ret = re.split('([0-9]+)', special_type)
        for item in ret:
            ret2 = item.split('-')
            for item2 in ret2:
                if item2 != '':
                    special.append(item2)

        if exch == 'global':
            return []  #self._get_global_instrument(exch, special_type, special_date)
        elif exch in self.huaxin_exch:
            return self._get_security(exch, special_type, special_date)
        else:
            if len(special) == 0:
                return self._get_future(exch, '', '', special_date)
            elif len(special) == 1:
                return self._get_future(exch, special[0], '', special_date)
            elif len(special) == 2 and special[1].isdigit():
                return self._get_future(exch, special[0], special[1], special_date)
            elif len(special) == 2 and not special[1].isdigit():
                return self._get_option(exch, special[0], '', special[1], special_date)
            elif len(special) == 3:
                return self._get_option(exch, special[0], special[1], special[2], special_date)

    # def _get_global_instrument(self, exch, special_type, special_date):
    #     self.absolute_path = '%s/%s/%s' % (sina_database_path, exch, exch)
    #     instrument_list = os.listdir(self.absolute_path)
    #     ret_list = []
    #     for item in instrument_list:
    #         ins_split = [x.strip() for x in re.split(r'(\d+)', item) if x.strip() != '']
    #         if (len(ins_split) == 1 and ins_split[0] == special_type) or special_type == '':
    #             ret_list.append(item)

    #     ret_list.sort()

    #     return ret_list

    def _get_future(self, exch, special_type, special_month, special_date):
        temp_list = os.listdir('/share/database/reconstruct/tick')
        instrument_list = []
        for item in temp_list:
            if os.path.exists('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                for item2 in os.listdir('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                    instrument_list.append(item2)

        instrument_list = list(set(instrument_list))
        temp_ret_list = []
        for item in instrument_list:
            ins_split = [x.strip().strip('-') for x in re.split(r'(\d+)', item) if x.strip() != '']
            if len(ins_split) == 2:
                if (ins_split[0] == special_type or special_type == '') and special_month in ins_split[1]:
                    temp_ret_list.append(item)
        temp_ret_list.sort()

        if special_date != '':
            ret_list = []
            for item in temp_ret_list:
                yearstr = self._get_year(exch, item)
                date_list = [
                    item.split('_')[-1].split('.')[0]
                    for item in os.listdir('/share/database/reconstruct/tick/%s/%s/%s/%s' % (yearstr, exch, exch, item))
                ]
                date_list.sort()
                if len(date_list) > 0 and date_list[0] <= special_date <= date_list[-1]:
                    ret_list.append(item)
        else:
            ret_list = temp_ret_list

        return ret_list

    def _get_option(self, exch, special_type, special_month, special_dir, special_date):
        temp_list = os.listdir('/share/database/reconstruct/tick')
        instrument_list = []
        for item in temp_list:
            if os.path.exists('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                for item2 in os.listdir('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                    instrument_list.append(item2)

        special_split = special_type.split('_')
        temp_ret_list = []
        for item in instrument_list:
            ins_split = [x.strip().strip('-') for x in re.split(r'(\d+)', item) if x.strip() != '']
            if len(ins_split) == 4:
                if (ins_split[0] == special_split[0]
                        or special_type == '') and special_month in ins_split[1] and special_dir in ins_split[2]:
                    temp_ret_list.append(item)
        temp_ret_list.sort()

        if special_date != '':
            ret_list = []
            for item in temp_ret_list:
                yearstr = self._get_year(exch, item)
                date_list = [
                    item.split('_')[-1].split('.')[0]
                    for item in os.listdir('/share/database/reconstruct/tick/%s/%s/%s/%s' % (yearstr, exch, exch, item))
                ]
                date_list.sort()
                if len(date_list) > 0 and date_list[0] <= special_date <= date_list[-1]:
                    ret_list.append(item)
        else:
            ret_list = temp_ret_list

        return ret_list

    def _get_security(self, exch, special_type, special_date):
        temp_list = os.listdir('/share/database/reconstruct/tick')

        ret_list = []
        for item in temp_list:
            if os.path.exists('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                for item2 in os.listdir('/share/database/reconstruct/tick/%s/%s/%s' % (item, exch, exch)):
                    if len(item2) >= 6 and special_type == item2[0:len(special_type)]:
                        if special_date != '':
                            date_list = [
                                item3.split('_')[-1].split('.')[0]
                                for item3 in os.listdir('/share/database/reconstruct/tick/%s/%s/%s/%s' % (item, exch, exch, item2))
                            ]
                            date_list.sort()
                            if len(date_list) > 0 and date_list[0] <= special_date <= date_list[-1]:
                                ret_list.append(item2)
                        else:
                            ret_list.append(item2)

        ret_list.sort()
        return ret_list

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

    def get_exchange(self):
        """ 交易所过去的合约提取

        Args:
            exch: 交易所简称

        Returns:
            返回的数据类型是 list， 包含该交易所下面所有的合约

        Examples:
            >>> from tickmine.content.info import info
            >>> info.get_exchange()

        """
        temp_list = os.listdir('/share/database/reconstruct/tick')
        exch_list = []
        for item in temp_list:
            for item2 in os.listdir('/share/database/reconstruct/tick/%s' % (item)):
                exch_list.append(item2)

        ret = list(set(exch_list))
        return ret


info = Info()

if __name__ == "__main__":
    print(info.get_instrument('SZSE', '002'))

    # print(info.get_instrument('CZCE', 'TA'))

    # print(info.get_instrument('CZCE', 'TA01', '20220628'))

    #print(info.get_instrument('CZCE', 'TA01-C', '20220628'))

    #print(info.get_instrument('CZCE', 'TA01-P'))

    # print(info.get_instrument('CZCE', 'TA-C'))

    # print(info.get_instrument('CZCE', 'TA-P'))
