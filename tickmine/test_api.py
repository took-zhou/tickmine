#!/usr/bin/env python
# coding=utf-8
import pytest
import sys
import numpy as np
from datetime import datetime, timezone, timedelta
import pandas as pd

from tickmine.api import get_exch, get_ins, get_date, get_rawtick, stream_kline, get_kline
import tickmine


def test_exch():
    ret_exch = get_exch()
    assert (len(ret_exch) == 12)


def test_ins():
    ret_ins = get_ins('CZCE')
    assert (len(ret_ins) > 100)

    ret_ins = get_ins('SHSE')
    assert (len(ret_ins) > 100)

    ret_ins = get_ins('GATE')
    assert (len(ret_ins) > 20)

    ret_ins = get_ins('FXCM')
    assert (len(ret_ins) > 20)


def test_date():
    csv_dir = tickmine.__path__[0] + '/WHITE_DATE.csv'
    temp_df = pd.read_csv(csv_dir)
    known_misalignment = set()
    for index, item in temp_df.iterrows():
        known_misalignment.add('%s %s %s-%s' % (item['exch'], item['ins'], item['date1'], item['date2']))
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=30, replace=False)
        for ins in selected_ins:
            ret_date = get_date(exch, ins)
            if len(ret_date) <= 1:
                continue
            dates = []
            for date_str in ret_date:
                date = datetime.strptime(date_str, "%Y%m%d")
                dates.append(date)
            for i in range(1, len(dates)):
                delta = dates[i] - dates[i - 1]
                if delta.days > 15:
                    message = "%s %s %s-%s" % (exch, ins, ret_date[i - 1], ret_date[i])
                    if message not in known_misalignment:
                        assert False, message


def test_adjust():
    stock_exch = ['NASDAQ', 'SEHK']
    possible_splits = [1.5, 2.0, 3.0, 6.0, 10.0, 15.0, 0.5, 0.333, 0.1]
    for exch in stock_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        for ins in selected_ins:
            prev_d1_kline = ''
            for d1_kline in stream_kline(exch, ins, period='1D'):
                if len(prev_d1_kline) > 0:
                    price_change = prev_d1_kline.Close[0] / d1_kline.Open[0]
                    split_flag = False
                    for item in possible_splits:
                        if abs(price_change - item) < 0.05:
                            split_flag = True
                            break
                    if split_flag == False:
                        assert (0.2 < price_change and price_change < 1.8), "ins: %s, price change: %f" % (ins, price_change)
                prev_d1_kline = d1_kline


def test_rawtick():
    one_day_before = datetime.now(timezone(timedelta(hours=8))) + timedelta(days=-1)
    temp_date = '%4d%02d%02d' % (one_day_before.year, one_day_before.month, one_day_before.day)
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        for ins in selected_ins:
            rawtick = get_rawtick(exch, ins, temp_date)
            assert (len(rawtick) > 10), '%s %s %s' % (exch, ins, temp_date)


def test_kline():
    one_day_before = datetime.now(timezone(timedelta(hours=8))) + timedelta(days=-1)
    temp_date = '%4d%02d%02d' % (one_day_before.year, one_day_before.month, one_day_before.day)
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        for ins in selected_ins:
            kline = get_kline(exch, ins, temp_date)
            assert (len(kline) > 10), '%s %s %s' % (exch, ins, temp_date)


if __name__ == "__main__":
    # python test_api.py -k "test_date"
    pytest_args = sys.argv[1:]
    if not pytest_args:
        pytest_args = ["-v"]

    pytest.main(pytest_args)
