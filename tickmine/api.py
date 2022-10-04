#coding=utf-8
import datetime
import re

import _pickle as cPickle
import pandas as pd
import zerorpc

from tickmine.topology import topology


def _get_night_date(_data):
    ins_time_of_week = pd.to_datetime(_data, format='%Y-%m-%d').dayofweek + 1

    if ins_time_of_week == 1:
        three_day_before = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=-3)
        split = str(three_day_before).split('-')
        night_date = split[0] + split[1] + split[2].split(' ')[0]
    elif 1 < ins_time_of_week <= 5:
        one_day_before = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=-1)
        split = str(one_day_before).split('-')
        night_date = split[0] + split[1] + split[2].split(' ')[0]
    else:
        night_date = ''

    return night_date


def _get_time_slice(_data, _time):
    ret = ['', '']

    if '16:00:00' <= _time[0] <= '24:00:00':
        ret[0] = datetime.datetime.strptime(_get_night_date(_data) + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif '00:00:00' <= _time[0] <= '03:00:00':
        night_date = _get_night_date(_data)
        one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
        split = str(one_day_after).split('-')
        one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
        ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif _time[0] != '':
        ret[0] = datetime.datetime.strptime(_data + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

    if '16:00:00' <= _time[1] <= '24:00:00':
        ret[1] = datetime.datetime.strptime(_get_night_date(_data) + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif '00:00:00' <= _time[1] <= '03:00:00':
        night_date = _get_night_date(_data)
        one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
        split = str(one_day_after).split('-')
        one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
        ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif _time[1] != '':
        ret[1] = datetime.datetime.strptime(_data + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

    return ret


def _get_time_slice_sina(_data, _time):
    ret = ['', '']

    if '00:00:00' <= _time[0] <= '06:00:00':
        one_day_after = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=1)
        split = str(one_day_after).split('-')
        one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
        ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif _time[0] != '':
        ret[0] = datetime.datetime.strptime(_data + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

    if '00:00:00' <= _time[1] <= '06:00:00':
        one_day_after = pd.to_datetime(_data, format='%Y-%m-%d') + datetime.timedelta(days=1)
        split = str(one_day_after).split('-')
        one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
        ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
    elif _time[1] != '':
        ret[1] = datetime.datetime.strptime(_data + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

    return ret


def _get_year(exch, ins):
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


def get_rawtick(exch, ins, day_date, time_slice=[]):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = c.rawtick(exch, ins, day_date)
        c.close()
    else:
        if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_tsaodai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.rawtick(exch, ins, day_date))
        else:
            if _get_year(exch, ins) <= '2022':
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic1_1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic2_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()

        if len(time_slice) == 2:
            if exch == 'global':
                _time_slice = _get_time_slice_sina(day_date, time_slice)
            else:
                _time_slice = _get_time_slice(day_date, time_slice)
            temp = temp.truncate(before=_time_slice[0], after=_time_slice[1])

    return temp


def get_kline(exch, ins, day_date, time_slice=[], period='1T'):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = cPickle.loads(c.kline(exch, ins, day_date, period))
        c.close()
    else:
        if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_tsaodai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.kline(exch, ins, day_date, period))
        else:
            if _get_year(exch, ins) <= '2022':
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic1_1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic2_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()

        if len(time_slice) == 2:
            if exch == 'global':
                _time_slice = _get_time_slice_sina(day_date, time_slice)
            else:
                _time_slice = _get_time_slice(day_date, time_slice)
            temp = temp.truncate(before=_time_slice[0], after=_time_slice[1])

    return temp


def get_level1(exch, ins, day_date, time_slice=[]):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = cPickle.loads(c.level1(exch, ins, day_date))
        c.close()
    else:
        if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_tsaodai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.level1(exch, ins, day_date))
        else:
            if _get_year(exch, ins) <= '2022':
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic1_1']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic2_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()

        if len(time_slice) == 2:
            if exch == 'global':
                _time_slice = _get_time_slice_sina(day_date, time_slice)
            else:
                _time_slice = _get_time_slice(day_date, time_slice)
            temp = temp.truncate(before=_time_slice[0], after=_time_slice[1])

    return temp


def get_mline(exch, ins, day_date):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = c.mline(exch, ins, day_date)
        c.close()
    else:
        if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_tsaodai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.mline(exch, ins, day_date))
        else:
            if _get_year(exch, ins) <= '2022':
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic1_1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic2_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()

    return temp


def get_date(exch, ins):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = c.date(exch, ins)
        c.close()
    else:
        if _get_year(exch, ins) <= '2022':
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic1_2-4']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic2_2']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_tsaodai1_2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b))
            temp.sort()

    return temp


def get_ins(exch, special_type='', special_date=''):
    temp = []
    if exch in ['SHSE', 'SZSE']:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1_2']
        c.connect(client_api)
        temp = c.ins(exch, special_type, special_date)
        c.close()
    else:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_citic1_1']
        c.connect(client_api)
        temp_a = c.ins(exch, special_type, special_date)
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_citic2_2']
        c.connect(client_api)
        temp_b = c.ins(exch, special_type, special_date)
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_tsaodai1_2']
        c.connect(client_api)
        temp_c = c.ins(exch, special_type, special_date)
        c.close()
        temp = temp_a + temp_b + temp_c
        temp = list(set(temp))

    return temp


def get_exch():
    temp = []
    c = zerorpc.Client(timeout=300, heartbeat=None)
    client_api = topology.ip_dict['tickserver_zhongtai1_2']
    c.connect(client_api)
    security_exch = c.exch()
    c.close()

    c = zerorpc.Client(timeout=300, heartbeat=None)
    client_api = topology.ip_dict['tickserver_tsaodai1_2']
    c.connect(client_api)
    future_exch = c.exch()
    c.close()

    temp = security_exch + future_exch
    temp.sort()
    return temp


if __name__ == "__main__":
    import time

    start = time.time()
    ret = get_date('CZCE', 'TA301')
    # for item in ret:
    #     out = get_rawtick('CZCE', 'AP301', item)
    #     # print(out)
    #     if len(out) == 0:
    #         print(item)
    print(ret)

    end = time.time()
    runTime = end - start
    print("run time: ", runTime)
