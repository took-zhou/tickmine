#coding=utf-8
import datetime
import re

import _pickle as cPickle
import pandas as pd
import zerorpc

from tickmine.topology import topology

try:
    c = zerorpc.Client(timeout=3, heartbeat=None)
    client_api = topology.ip_dict['tickserver_citic_self1']
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


def _get_year(exch, ins, day_date):
    resplit = re.findall(r'([0-9]*)([A-Z,a-z]*)', ins)
    begin = 200
    split_date = ''
    for i in range(10):
        split_date = str(begin + i) + resplit[1][0][-3:] + '31'
        if split_date >= day_date:
            break

    return split_date[0:4]


def get_rawtick(exch, ins, day_date):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = cPickle.loads(c.rawtick(exch, ins, day_date))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1']
                    c.connect(client_api)
                    temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                    c.close()
                else:
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic2']
                    c.connect(client_api)
                    temp = cPickle.loads(c.rawtick(exch, ins, day_date))
                    c.close()
    except:
        pass

    return temp


def get_kline(exch, ins, day_date, period='1T'):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = cPickle.loads(c.kline(exch, ins, day_date, period))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1']
                    c.connect(client_api)
                    temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                    c.close()
                else:
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic2']
                    c.connect(client_api)
                    temp = cPickle.loads(c.kline(exch, ins, day_date, period))
                    c.close()
    except:
        pass

    return temp


def get_mline(exch, ins, day_date):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = cPickle.loads(c.mline(exch, ins, day_date))
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_gate1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=None)
                client_api = topology.ip_dict['tickserver_citic_self1']
                c.connect(client_api)
                temp = cPickle.loads(c.mline(exch, ins, day_date))
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic1']
                    c.connect(client_api)
                    temp = cPickle.loads(c.mline(exch, ins, day_date))
                    c.close()
                else:
                    c = zerorpc.Client(timeout=300, heartbeat=None)
                    client_api = topology.ip_dict['tickserver_citic2']
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
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate_self1']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate1']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b))
            temp.sort()
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm1']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic1']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic_self1']
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
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = c.ins(exch, special_type, special_date)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate_self1']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_gate1']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()
            temp = temp_a + temp_b
            temp = list(set(temp))
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_fxcm1']
            c.connect(client_api)
            temp = c.ins(exch, special_type, special_date)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic1']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic2']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=None)
            client_api = topology.ip_dict['tickserver_citic_self1']
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
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_zhongtai1']
        c.connect(client_api)
        security_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_gate_self1']
        c.connect(client_api)
        crypto_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_fxcm1']
        c.connect(client_api)
        forex_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_citic_self1']
        c.connect(client_api)
        future_exch = c.exch()
        c.close()

        temp = security_exch + crypto_exch + forex_exch + future_exch
        temp.sort()
    except:
        pass

    return temp


def get_activity(exch, ins, day_date):
    temp = []
    try:
        c = zerorpc.Client(timeout=300, heartbeat=None)
        client_api = topology.ip_dict['tickserver_summary1']
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
    print(get_date('GATE', 'RWA_USDT'))
    # print(get_kline('CZCE', 'AP501', '20241031', period='1D'))
