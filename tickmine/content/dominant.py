#!/usr/bin/python
# coding=utf-8
import re

class dominantFuture:
    def __init__(self):
        self.SHFE = {}
        self.CZCE = {}
        self.DCE = {}
        self.INE = {}
        self.CFFEX = {}

        self.month1_1 = {'01': ['08', '09', '10', '11']}
        self.month1_2 = {'01': ['09', '10', '11']}
        self.month2 = {'02': []}
        self.month3 = {'03': []}
        self.month4 = {'04': []}
        self.month5 = {'05': ['12', '01', '02', '03']}
        self.month6 = {'06': []}
        self.month7 = {'07': []}
        self.month8 = {'08': []}
        self.month9 = {'09': ['04', '05', '06', '07']}
        self.month10 = {'10': ['05', '06', '07', '08']}
        self.month11 = {'11': []}
        self.month12 = {'12': []}

        self.dominant_compose1 = {'01': ['08', '09', '10', '11'], '05': ['12', '01', '02', '03'], '09': ['04', '05', '06', '07']}
        self.dominant_compose2 = {'01': ['08', '09', '10', '11'], '05': ['12', '01', '02', '03'], '10': ['04', '05', '06', '07']}
        self.dominant_compose3 = {'03': ['11', '12', '01'], '06': ['02', '03', '04'], '09': ['05', '06', '07'], '12': ['08', '09', '10']}
        self.dominant_compose4 = {'03': ['01'], '04': ['02'], '05': ['03'], '06': ['04'], '07': ['05'], '08': ['06'], \
            '09': ['07'], '10': ['08'], '11': ['09'], '12': ['10'], '01': ['11'], '02': ['12']}
        self.dominant_compose5 = {'01': ['09', '10', '11'], '04': ['12', '01', '02'], '10': ['04', '05', '06', '07', '08']}
        self.dominant_compose6 = {'06': ['11', '12', '01', '02', '03', '04'], '12': ['05', '06', '07', '08', '09', '10']}
        self.dominant_compose7 = {'03': ['12'], '04': ['01'], '05': ['02'], '06': ['03'], '07': ['04'], '08': ['05'], \
            '09': ['06'], '10': ['07'], '11': ['08'], '12': ['09'], '01': ['10'], '02': ['11']}

        self.SHFE['cu'] = self.dominant_compose4
        self.SHFE['al'] = self.dominant_compose4
        self.SHFE['zn'] = self.dominant_compose4
        self.SHFE['pb'] = self.dominant_compose4
        self.SHFE['ni'] = self.dominant_compose4
        self.SHFE['sn'] = self.dominant_compose4
        self.SHFE['au'] = self.dominant_compose6
        self.SHFE['ag'] = self.dominant_compose6
        self.SHFE['rb'] = self.dominant_compose2
        self.SHFE['wr'] = self.dominant_compose4
        self.SHFE['hc'] = self.dominant_compose2
        self.SHFE['ss'] = self.dominant_compose4
        self.SHFE['fu'] = self.dominant_compose1
        self.SHFE['bu'] = self.dominant_compose4
        self.SHFE['ru'] = self.dominant_compose1
        self.SHFE['sp'] = self.dominant_compose4

        self.CZCE['WH'] = self.dominant_compose1
        self.CZCE['PM'] = self.dominant_compose1
        self.CZCE['CF'] = self.dominant_compose1
        self.CZCE['SR'] = self.dominant_compose1
        self.CZCE['OI'] = self.dominant_compose1
        self.CZCE['RI'] = self.dominant_compose1
        self.CZCE['RS'] = self.dominant_compose1
        self.CZCE['RM'] = self.dominant_compose1
        self.CZCE['JR'] = self.dominant_compose1
        self.CZCE['LR'] = self.dominant_compose1
        self.CZCE['CY'] = self.dominant_compose1
        self.CZCE['AP'] = self.dominant_compose2
        self.CZCE['CJ'] = self.dominant_compose1
        self.CZCE['TA'] = self.dominant_compose1
        self.CZCE['MA'] = self.dominant_compose1
        self.CZCE['ME'] = self.dominant_compose1
        self.CZCE['FG'] = self.dominant_compose1
        self.CZCE['ZC'] = self.dominant_compose1
        self.CZCE['TC'] = self.dominant_compose1
        self.CZCE['SF'] = self.dominant_compose1
        self.CZCE['SM'] = self.dominant_compose1
        self.CZCE['UR'] = self.dominant_compose1
        self.CZCE['SA'] = self.dominant_compose1
        self.CZCE['PF'] = self.dominant_compose1
        self.CZCE['PK'] = self.dominant_compose5

        self.DCE['c'] = self.dominant_compose1
        self.DCE['cs'] = self.dominant_compose1
        self.DCE['a'] = self.dominant_compose1
        self.DCE['b'] = self.dominant_compose1
        self.DCE['m'] = self.dominant_compose1
        self.DCE['y'] = self.dominant_compose1
        self.DCE['p'] = self.dominant_compose1
        self.DCE['fb'] = self.dominant_compose1
        self.DCE['bb'] = self.dominant_compose1
        self.DCE['jd'] = self.dominant_compose1
        self.DCE['rr'] = self.dominant_compose1
        self.DCE['l'] = self.dominant_compose1
        self.DCE['v'] = self.dominant_compose1
        self.DCE['pp'] = self.dominant_compose1
        self.DCE['j'] = self.dominant_compose1
        self.DCE['jm'] = self.dominant_compose1
        self.DCE['i'] = self.dominant_compose1
        self.DCE['eg'] = self.dominant_compose1
        self.DCE['eb']  = self.dominant_compose1
        self.DCE['pg'] = self.dominant_compose1
        self.DCE['lh'] = self.dominant_compose1

        self.INE['sc'] = self.dominant_compose4
        self.INE['lu'] = self.dominant_compose7
        self.INE['nr'] = self.dominant_compose4
        self.INE['bc'] = self.dominant_compose4

        self.CFFEX['IF'] = self.dominant_compose3
        self.CFFEX['IC'] = self.dominant_compose3
        self.CFFEX['IH'] = self.dominant_compose3
        self.CFFEX['TS'] = self.dominant_compose3
        self.CFFEX['TF'] = self.dominant_compose3
        self.CFFEX['T'] = self.dominant_compose3

    def get_compose(self, exch, ins):
        """ 获取主力合约月份

        Args:
            exch: 交易所简称
            ins: 合约代码

        Returns:
            返回的数据类型是 list， 包含所有的主力合约月份

        Examples:
            >>> from nature_analysis.dominant import dominant
            >>> dominant.get_compose('CZCE', 'MA')
           ['01', '05', '09']
        """
        temp_dict = {}
        if exch == 'SHFE':
            if self.SHFE.__contains__(ins):
                temp_dict = self.SHFE[ins]
        elif exch == 'CZCE':
            if self.CZCE.__contains__(ins):
                temp_dict =  self.CZCE[ins]
        elif exch == 'DCE':
            if self.DCE.__contains__(ins):
                temp_dict =  self.DCE[ins]
        elif exch == 'INE':
            if self.INE.__contains__(ins):
                temp_dict =  self.INE[ins]
        elif exch == 'CFFEX':
            if self.CFFEX.__contains__(ins):
                temp_dict =  self.CFFEX[ins]

        if 'efp' in ins:
            temp_dict =  {}

        return temp_dict

    def get_ins(self, exch, ins, date):
        ins_type = re.split('([0-9]+)', ins)[0]
        month = date[4:6]
        month_compose = self.get_compose(exch, ins_type)

        diminant_month = ''
        for item in month_compose.keys():
            if month in month_compose[item]:
                diminant_month = item
                break

        if date[4:6] > diminant_month:
            _year = str(int(date[:4]) + 1)
        else:
            _year = date[:4]

        if len(re.split('([0-9]+)', ins)[1]) == 3:
            _year = _year[3:4]
        else:
            _year = _year[2:4]

        return ins_type + _year + diminant_month

dominant = dominantFuture()

if __name__=="__main__":
    ret = dominant.get_ins('CZCE', 'TA999', '20210817')
    print(ret)
