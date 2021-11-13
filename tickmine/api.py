import zerorpc
import os
import pickle

def get_rawtick(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client()
    c.connect("tcp://192.168.0.102:8100")
    return pickle.loads(c.rawtick(exch, ins, day_data, time_slice))

def get_kline(exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
    c = zerorpc.Client()
    c.connect("tcp://192.168.0.102:8100")
    return pickle.loads(c.kline(exch, ins, day_data, time_slice, period, subject))

def get_tradepoint(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client()
    c.connect("tcp://192.168.0.102:8100")
    return pickle.loads(c.tradepoint( exch, ins, day_data, time_slice))

# datas=get_rawtick('CZCE', 'AP110', '20210615', ['09:00:00', '11:00:00'])
# print(datas)

# datas = get_kline('CZCE', 'AP110', '20210615', ['09:00:00', '11:00:00'])
# print(datas)

# datas = get_tradepoint('CZCE', 'AP110', '20210615', ['21:00:00', '23:00:00'])
# print(datas)
