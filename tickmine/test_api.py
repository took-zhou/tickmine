#!/usr/bin/env python
# coding=utf-8
import pytest
import numpy as np
from datetime import datetime

from tickmine.api import get_exch, get_ins, get_date, get_rawtick


def test_exch():
    ret_exch = get_exch()
    assert (len(ret_exch) == 10)


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
    ret_exch = get_exch()
    selected_exch = np.random.choice(ret_exch, size=3, replace=False)
    for exch in selected_exch:
        ret_ins = get_ins(exch)
        needed_ins = [item for item in ret_ins if len(ret_ins) < 7 or exch in ['GATE', 'NASDAQ', 'SEHK', 'FXCM']]
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
                assert (delta.days < 15), "exch: %s, ins: %s, date1: %s, date2: %s" % (exch, ins, ret_date[i - 1], ret_date[i])


def test_rawtick():
    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA205', '20211227')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA205', '20211227')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('GATE', 'ETH_USDT', '20230104')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('GATE', 'AVAX_USDT', '20240730')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('FXCM', 'AUD_CAD', '20230104')
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('FXCM', 'AUD_CHF', '20240730')
    assert (len(ret_rawtick) > 10)


if __name__ == "__main__":
    pytest.main()
