from tickmine.api import get_ins, stream_kline, get_kline, get_date
import numpy as np
from datetime import datetime

# stock_exch = ['NASDAQ', 'SEHK']
# possible_splits = [2.0, 3.0, 6.0, 10.0, 15.0]
# for exch in stock_exch:
#     ret_ins = get_ins(exch)
#     for ins in ret_ins:
#         prev_d1_kline = ''
#         print('%s %s' % (exch, ins))
#         for d1_kline in stream_kline(exch, ins, period='1D'):
#             if len(prev_d1_kline) > 0:
#                 price_change = prev_d1_kline.Close[0] / d1_kline.Open[0]
#                 split_flag = False
#                 for item in possible_splits:
#                     if abs(price_change - item) < 0.2:
#                         print('--%s %s %f %f %f--' % (ins, d1_kline.index[0], prev_d1_kline.Close[0], d1_kline.Open[0], price_change))
#                         split_flag = True
#                         break
#                 if split_flag == False and (price_change > 1.8 or price_change < 0.2):
#                     print('++%s %s %f %f %f++' % (ins, d1_kline.index[0], prev_d1_kline.Close[0], d1_kline.Open[0], price_change))

#             prev_d1_kline = d1_kline

# ins_list = get_ins('GATE', special_date='20250819')
# count = 0
# for ins in ins_list:
#     ret = get_kline('GATE', ins, '20250819', '1D')
#     if len(ret) > 0 and ret.Open[0] > 0.1:
#         count = count + 1

# print('total length %d , need count %d' % (len(ins_list), count))

# for exch in ['FXCM']:
#     ins_list = get_ins(exch)
#     for ins in ins_list:
#         ret_date = get_date(exch, ins)
#         if len(ret_date) <= 1:
#             continue
#         dates = []
#         for date_str in ret_date:
#             date = datetime.strptime(date_str, "%Y%m%d")
#             dates.append(date)
#         for i in range(1, len(dates)):
#             delta = dates[i] - dates[i - 1]
#             if delta.days > 15:
#                 print("exch: %s, ins: %s, date1: %s, date2: %s" % (exch, ins, ret_date[i - 1], ret_date[i]))

# import re
# import os
# from datetime import datetime, timedelta

# with open('log.txt', 'r', encoding='utf-8') as file:
#     for line in file:
#         line = line.strip()
#         if not line:
#             continue

#         pattern = r'exch: (\w+), ins: (\w+), date1: (\d{8}), date2: (\d{8})'
#         match = re.match(pattern, line)

#         if match:
#             exch, ins, date1_str, date2_str = match.groups()
#             date1 = datetime.strptime(date1_str, '%Y%m%d')
#             date1_modified = date1 + timedelta(days=15)
#             fix_year_month = '%04d%02d' % (date1_modified.year, date1_modified.month)
#             command = 'python format_ibkr.py %s %s %s' % (fix_year_month, exch, ins)
#             print(command)
#             os.system(command)

#!/usr/bin/env python
# coding=utf-8
import pytest
import sys
import numpy as np
from datetime import datetime, timezone, timedelta
import pandas as pd

from tickmine.api import get_exch, get_ins, get_date, get_rawtick, stream_kline, get_kline


def test_kline():
    one_day_before = datetime.now(timezone(timedelta(hours=8))) + timedelta(days=-1)
    temp_date = '%4d%02d%02d' % (one_day_before.year, one_day_before.month, one_day_before.day)
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch, special_date=temp_date)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        for ins in selected_ins:
            print('%s %s %s' % (exch, ins, temp_date))
            kline = get_kline(exch, ins, temp_date)
            assert (len(kline) > 10), '%s %s %s' % (exch, ins, temp_date)


test_kline()
