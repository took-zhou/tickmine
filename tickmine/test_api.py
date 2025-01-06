#!/usr/bin/env python
# coding=utf-8
import pytest

from tickmine.api import *


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
    ret_date = get_date('CZCE', 'TA205')
    assert (len(ret_date) > 100)

    ret_date = get_date('GATE', 'BTC_USDT')
    assert (len(ret_date) > 10)

    ret_date = get_date('FXCM', 'AUD_CAD')
    assert (len(ret_date) > 10)


def test_rawtick():
    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310', time_slice=['09:00:00', '09:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310', time_slice=['21:00:00', '21:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA205', '20211227', time_slice=['09:00:00', '09:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA205', '20211227', time_slice=['21:00:00', '21:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227', time_slice=['09:00:00', '09:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227', time_slice=['21:00:00', '21:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('SHFE', 'al2205', '20211227', time_slice=['00:00:00', '01:00:00'])
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
