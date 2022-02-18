import zerorpc
import pickle
import datetime

client_api_first = 'tcp://192.168.0.102:8100'
client_api_second = 'tcp://192.168.0.102:8101'
client_api_third = 'tcp://192.168.0.102:8150'

try:
    c = zerorpc.Client(timeout=1, heartbeat=None)
    c.connect(client_api_first)
    temp = c.exch()
    c.close()
except:
    client_api_first = 'tcp://onepiece.cdsslh.com:6007'
    client_api_second = 'tcp://onepiece.cdsslh.com:6008'
    client_api_third = 'tcp://onepiece.cdsslh.com:6009'

def get_rawtick(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)

    if exch == 'global':
        c.connect(client_api_third)
    else:
        if (datetime.datetime.strptime(day_data, "%Y%m%d")-datetime.datetime.today()).days < -45:
            c.connect(client_api_first)
        else:
            c.connect(client_api_second)
    temp = pickle.loads(c.rawtick(exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_kline(exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if (datetime.datetime.strptime(day_data, "%Y%m%d")-datetime.datetime.today()).days < -45:
        c.connect(client_api_first)
    else:
        c.connect(client_api_second)
    temp = pickle.loads(c.kline(exch, ins, day_data, time_slice, period, subject))
    c.close()
    return temp

def get_tradepoint(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if (datetime.datetime.strptime(day_data, "%Y%m%d")-datetime.datetime.today()).days < -45:
        c.connect(client_api_first)
    else:
        c.connect(client_api_second)
    temp = pickle.loads(c.tradepoint( exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_level1(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if (datetime.datetime.strptime(day_data, "%Y%m%d")-datetime.datetime.today()).days < -45:
        c.connect(client_api_first)
    else:
        c.connect(client_api_second)
    temp = pickle.loads(c.level1( exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_date(exch, ins):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_first)
    a_temp = c.date(exch, ins)
    c.connect(client_api_second)
    c.close()

    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_second)
    b_temp = c.date(exch, ins)
    c.close()

    if a_temp[-1] in b_temp:
        ret = a_temp+b_temp[b_temp.index(a_temp[-1])+1:]
    else:
        ret = a_temp

    return ret

def get_ins(exch, special_type = ''):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_first)
    temp = c.ins(exch, special_type)
    c.close()
    return temp

def get_exch():
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_first)
    temp = c.exch()
    c.close()
    return temp
