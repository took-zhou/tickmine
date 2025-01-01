#coding=utf-8
import datetime
import re

import _pickle as cPickle
import pandas as pd
import zerorpc

from tickmine.topology import topology

try:
    c = zerorpc.Client(timeout=3, heartbeat=None)
    client_api = topology.ip_dict['tickserver_citic_self1_2']
    c.connect(client_api)
    future_exch = c.exch()
    c.close()
except:
    print('select external api')
    for item in topology.gradation_list:
        if '192.168.0.102' in item['access_api']:
            topology.ip_dict[item['docker_name']] = item['access_api'].replace('192.168.0.102', 'tsaodai.com')
        if '192.168.0.104' in item['access_api']:
            topology.ip_dict[item['docker_name']] = item['access_api'].replace('192.168.0.104', 'tsaodai.com')


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


def _get_year(exch, ins, day_date):
    resplit = re.findall(r'([0-9]*)([A-Z,a-z]*)', ins)
    begin = 200
    split_date = ''
    for i in range(10):
        split_date = str(begin + i) + resplit[1][0][-3:] + '31'
        if split_date >= day_date:
            break

    return split_date[0:4]


def get_rawtick(exch, ins, day_date, time_slice=[]):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.rawtick(exch, ins, day_date))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1_2-4']
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
    except:
        pass

    return temp


def get_kline(exch, ins, day_date, time_slice=[], period='1T'):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.kline(exch, ins, day_date, period))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1_2-4']
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
    except:
        pass

    return temp


def get_level1(exch, ins, day_date, time_slice=[]):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.level1(exch, ins, day_date))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.level1(exch, ins, day_date))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1_2-4']
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
    except:
        pass

    return temp


def get_mline(exch, ins, day_date):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = cPickle.loads(c.mline(exch, ins, day_date))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1_2']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1_2-4']
                    c.connect(client_api)
                    temp = cPickle.loads(c.mline(exch, ins, day_date))
                    c.close()
                else:
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic2_2']
                    c.connect(client_api)
                    temp = cPickle.loads(c.mline(exch, ins, day_date))
                    c.close()
    except:
        pass

    return temp


def get_date(exch, ins):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate_self1_2']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate1_2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b))
            temp.sort()
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm1_2']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm1_2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b))
            temp.sort()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic1_2-4']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic2_2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic_self1_2']
            c.connect(client_api)
            temp_c = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b + temp_c))
            temp.sort()
    except:
        pass

    return temp


def get_ins(exch, special_type='', special_date=''):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1_2']
            c.connect(client_api)
            temp = c.ins(exch, special_type, special_date)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate_self1_2']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate1_2']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()
            temp = temp_a + temp_b
            temp = list(set(temp))
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm_self1_2']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm1_2']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()
            temp = temp_a + temp_b
            temp = list(set(temp))
        else:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic1_2-4']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic2_2']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic_self1_2']
            c.connect(client_api)
            temp_c = c.ins(exch, special_type, special_date)
            c.close()
            temp = temp_a + temp_b + temp_c
            temp = list(set(temp))
    except:
        pass

    return temp


def get_exch():
    temp = []
    try:
        # c = zerorpc.Client(timeout=300, heartbeat=None)
        # client_api = topology.ip_dict['tickserver_zhongtai1_2']
        # c.connect(client_api)
        # security_exch = c.exch()
        # c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_gate_self1_2']
        c.connect(client_api)
        crypto_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_fxcm1_2']
        c.connect(client_api)
        forex_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_citic_self1_2']
        c.connect(client_api)
        future_exch = c.exch()
        c.close()

        # temp = security_exch + crypto_exch + forex_exch + future_exch
        temp = crypto_exch + forex_exch + future_exch
        temp.sort()
    except:
        pass

    return temp


def get_activity(exch, ins, day_date):
    temp = []
    try:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_summary1_2']
        c.connect(client_api)
        temp = cPickle.loads(c.activity(exch, ins, day_date))
        c.close()

    except:
        pass

    return temp


if __name__ == "__main__":
    # import time

    # start = time.time()
    # ret = get_activity('CFFEX', 'IC', '20150416')
    # ret_ins = ret[ret['Ins'] == 'IC1505']
    # print(ret[ret['Ins'] == 'IC1505'].InsDegree[0])
    # print(ret_ins.InsDegree[0])
    # print(ret_ins.GroupDegree[0])

    # # for item in ret:
    # #     out = get_rawtick('CZCE', 'AP301', item)
    # #     # print(out)
    # #     if len(out) == 0:
    # #         print(item)
    # print(ret)

    # end = time.time()
    # runTime = end - start
    # print("run time: ", runTime)
    # time.sleep(10000)
    print(get_exch())
    # print(get_kline('CZCE', 'AP501', '20241031', period='1D'))
