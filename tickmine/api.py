#coding=utf-8
import datetime
import re

import _pickle as cPickle
import zerorpc

from tickmine.topology import topology

try:
    c = zerorpc.Client(timeout=3, heartbeat=45)
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


def _get_data(func, exch, ins, day_date, period):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, day_date, period):
                yield cPickle.loads(data)
            c.close()
        elif exch in ['GATE']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=45)
                client_api = topology.ip_dict['tickserver_gate_self1']
                c.connect(client_api)
                for data in c.__getattr__(func)(exch, ins, day_date, period):
                    yield cPickle.loads(data)
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=45)
                client_api = topology.ip_dict['tickserver_gate1']
                c.connect(client_api)
                for data in c.__getattr__(func)(exch, ins, day_date, period):
                    yield cPickle.loads(data)
                c.close()
        elif exch in ['FXCM']:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=45)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                for data in c.__getattr__(func)(exch, ins, day_date, period):
                    yield cPickle.loads(data)
                c.close()
            else:
                c = zerorpc.Client(timeout=300, heartbeat=45)
                client_api = topology.ip_dict['tickserver_fxcm1']
                c.connect(client_api)
                for data in c.__getattr__(func)(exch, ins, day_date, period):
                    yield cPickle.loads(data)
                c.close()
        else:
            if (datetime.datetime.today() - datetime.datetime.strptime(day_date, "%Y%m%d")).days <= 45:
                c = zerorpc.Client(timeout=300, heartbeat=45)
                client_api = topology.ip_dict['tickserver_citic_self1']
                c.connect(client_api)
                for data in c.__getattr__(func)(exch, ins, day_date, period):
                    yield cPickle.loads(data)
                c.close()
            else:
                if _get_year(exch, ins, day_date) <= '2022':
                    c = zerorpc.Client(timeout=300, heartbeat=45)
                    client_api = topology.ip_dict['tickserver_citic1']
                    c.connect(client_api)
                    for data in c.__getattr__(func)(exch, ins, day_date, period):
                        yield cPickle.loads(data)
                    c.close()
                else:
                    c = zerorpc.Client(timeout=300, heartbeat=45)
                    client_api = topology.ip_dict['tickserver_citic2']
                    c.connect(client_api)
                    for data in c.__getattr__(func)(exch, ins, day_date, period):
                        yield cPickle.loads(data)
                    c.close()
    except:
        pass

    return temp


def _stream_data(func, exch, ins, period):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, '', period):
                yield cPickle.loads(data)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_gate1']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, '', period):
                yield cPickle.loads(data)
            c.close()
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_fxcm1']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, '', period):
                yield cPickle.loads(data)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic1']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, '', period):
                yield cPickle.loads(data)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic2']
            c.connect(client_api)
            for data in c.__getattr__(func)(exch, ins, '', period):
                yield cPickle.loads(data)
            c.close()
    except:
        pass

    return temp


def get_rawtick(exch, ins, day_date):
    return next(_get_data('rawtick', exch, ins, day_date, ''))


def stream_rawtick(exch, ins):
    yield from _stream_data('rawtick', exch, ins, '')


def get_kline(exch, ins, day_date, period='1T'):
    return next(_get_data('kline', exch, ins, day_date, period))


def stream_kline(exch, ins, period='1T'):
    yield from _stream_data('kline', exch, ins, period)


def get_mline(exch, ins, day_date):
    return next(_get_data('mline', exch, ins, day_date, ''))


def stream_mline(exch, ins):
    yield from _stream_data('mline', exch, ins, '')


def get_date(exch, ins):
    temp = []
    try:
        if exch in ['SHSE', 'SZSE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_gate_self1']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_gate1']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            temp = list(set(temp_a + temp_b))
            temp.sort()
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_fxcm1']
            c.connect(client_api)
            temp = c.date(exch, ins)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic1']
            c.connect(client_api)
            temp_a = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic2']
            c.connect(client_api)
            temp_b = c.date(exch, ins)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
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
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_zhongtai1']
            c.connect(client_api)
            temp = c.ins(exch, special_type, special_date)
            c.close()
        elif exch in ['GATE']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_gate_self1']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_gate1']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()

            temp = temp_a + temp_b
            temp = list(set(temp))
        elif exch in ['FXCM']:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_fxcm1']
            c.connect(client_api)
            temp = c.ins(exch, special_type, special_date)
            c.close()
        else:
            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic1']
            c.connect(client_api)
            temp_a = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
            client_api = topology.ip_dict['tickserver_citic2']
            c.connect(client_api)
            temp_b = c.ins(exch, special_type, special_date)
            c.close()

            c = zerorpc.Client(timeout=300, heartbeat=45)
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
        c = zerorpc.Client(timeout=300, heartbeat=45)
        client_api = topology.ip_dict['tickserver_zhongtai1']
        c.connect(client_api)
        security_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=45)
        client_api = topology.ip_dict['tickserver_gate_self1']
        c.connect(client_api)
        crypto_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=45)
        client_api = topology.ip_dict['tickserver_fxcm1']
        c.connect(client_api)
        forex_exch = c.exch()
        c.close()

        c = zerorpc.Client(timeout=300, heartbeat=45)
        client_api = topology.ip_dict['tickserver_citic_self1']
        c.connect(client_api)
        future_exch = c.exch()
        c.close()

        temp = security_exch + crypto_exch + forex_exch + future_exch
        temp.sort()
    except:
        pass

    return temp


if __name__ == "__main__":
    for item in stream_rawtick('CZCE', 'AP510'):
        print(item)
