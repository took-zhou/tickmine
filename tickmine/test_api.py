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
        needed_ins = [item for item in ret_ins if len(item) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
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
                if delta.days > 15 and delta.days < 3000:
                    message = "%s %s %s-%s" % (exch, ins, ret_date[i - 1], ret_date[i])
                    if message not in known_misalignment:
                        assert False, message


def test_adjust():
    stock_exch = ['NASDAQ', 'SEHK']
    for exch in stock_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(item) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=len(needed_ins), replace=False)
        date_list = get_date('NASDAQ', 'AAPL')[-2:]
        for ins in selected_ins:
            prev_d1_kline = get_kline(exch, ins, date_list[0], period='1D')
            d1_kline = get_kline(exch, ins, date_list[1], period='1D')
            if len(prev_d1_kline) > 0 and len(d1_kline) > 0:
                price_change = prev_d1_kline.Close[0] / d1_kline.Open[0]
                assert (0.55 < price_change < 1.8), "ins: %s, date: %s, price change: %f" % (ins, d1_kline.index[0], price_change)


def test_rawtick():
    one_day_before = datetime.now(timezone(timedelta(hours=8))) + timedelta(days=-1)
    temp_date = '%4d%02d%02d' % (one_day_before.year, one_day_before.month, one_day_before.day)
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch, special_date=temp_date)
        needed_ins = [item for item in ret_ins if len(item) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if exch in ['CFFEX', 'CZCE', 'DCE', 'GATE', 'GFEX', 'INE', 'NASDAQ', 'SEHK', 'SHFE']:
            assert (len(needed_ins) > 10), '%s %s' % (exch, temp_date)
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        exist_rawtick = False
        for ins in selected_ins:
            rawtick = get_rawtick(exch, ins, temp_date)
            if len(rawtick) > 1:
                exist_rawtick = True
        assert (exist_rawtick == True), '%s %s' % (exch, temp_date)


def test_kline():
    one_day_before = datetime.now(timezone(timedelta(hours=8))) + timedelta(days=-1)
    temp_date = '%4d%02d%02d' % (one_day_before.year, one_day_before.month, one_day_before.day)
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch, special_date=temp_date)
        needed_ins = [item for item in ret_ins if len(item) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
        if exch in ['CFFEX', 'CZCE', 'DCE', 'GATE', 'GFEX', 'INE', 'NASDAQ', 'SEHK', 'SHFE']:
            assert (len(needed_ins) > 10), '%s %s' % (exch, temp_date)
        if len(needed_ins) <= 10:
            continue
        selected_ins = np.random.choice(needed_ins, size=10, replace=False)
        exist_kline = False
        for ins in selected_ins:
            kline = get_kline(exch, ins, temp_date)
            if len(kline) > 1:
                exist_kline = True
        assert (exist_kline == True), '%s %s' % (exch, temp_date)


if __name__ == "__main__":
    # python test_api.py -k "test_date"
    pytest_args = sys.argv[1:]
    if not pytest_args:
        pytest_args = ["-v"]

    pytest.main(pytest_args)
