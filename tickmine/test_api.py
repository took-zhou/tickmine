#!/usr/bin/env python
# coding=utf-8
import pytest

from tickmine.api import *


def test_exch():
    ret_exch = get_exch()
    assert (len(ret_exch) == 7)


def test_ins():
    ret_ins = get_ins('CZCE')
    assert (len(ret_ins) > 100)

    ret_ins = get_ins('SHSE')
    assert (len(ret_ins) > 100)


def test_date():
    ret_date = get_date('CZCE', 'TA205')
    assert (len(ret_date) > 100)

    # ret_date = get_date('global', 'CL')
    # assert (len(ret_date) > 10)


def test_rawtick():
    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310', time_slice=['09:00:00', '09:30:00'])
    assert (len(ret_rawtick) > 10)

    ret_rawtick = get_rawtick('CZCE', 'TA209', '20220310', time_slice=['21:00:00', '21:30:00'])
    assert (len(ret_rawtick) > 10)

    # ret_rawtick = get_rawtick('global', 'CL', '20220310', time_slice=['09:00:00', '10:00:00'])
    # assert(len(ret_rawtick) > 10)

    # ret_rawtick = get_rawtick('global', 'CL', '20220310', time_slice=['21:00:00', '21:30:00'])
    # assert(len(ret_rawtick) > 10)

    # ret_rawtick = get_rawtick('global', 'CL', '20220310', time_slice=['00:00:00', '00:30:00'])
    # assert(len(ret_rawtick) > 10)

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

    # ret_rawtick = get_rawtick('SHFE', 'al2209', '20220718', time_slice=['21:00:00', '21:30:00'])
    # assert (len(ret_rawtick) > 10)

    # ret_rawtick = get_rawtick('CZCE', 'TA209', '20220718', time_slice=['21:00:00', '01:00:00'])
    # assert (len(ret_rawtick) > 10)


if __name__ == "__main__":
    pytest.main()
