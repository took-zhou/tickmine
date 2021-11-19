import zerorpc
import os
import pickle
from tickmine.global_config import api_address

def get_rawtick(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client()
    c.connect(api_address)
    temp = pickle.loads(c.rawtick(exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_kline(exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
    c = zerorpc.Client()
    c.connect(api_address)
    temp = pickle.loads(c.kline(exch, ins, day_data, time_slice, period, subject))
    c.close()
    return temp

def get_tradepoint(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client()
    c.connect(api_address)
    temp = pickle.loads(c.tradepoint( exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_data(exch, ins):
    c = zerorpc.Client()
    c.connect(api_address)
    temp = c.date(exch, ins)
    c.close()
    return temp

def get_ins(exch):
    c = zerorpc.Client()
    c.connect(api_address)
    temp = c.ins(exch)
    c.close()
    return temp

def get_exch():
    c = zerorpc.Client()
    c.connect(api_address)
    temp = c.exch()
    c.close()
    return temp

# datas=get_rawtick('CZCE', 'AP110', '20210615', ['09:00:00', '11:00:00'])
# print(datas)

# datas = get_kline('CZCE', 'AP110', '20210615', ['09:00:00', '11:00:00'])
# print(datas)

# datas = get_tradepoint('CZCE', 'AP110', '20210615', ['21:00:00', '23:00:00'])
# print(datas)
