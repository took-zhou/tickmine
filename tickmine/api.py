import zerorpc
import pickle
import os

if "client_api_address" in os.environ:
    client_api_address = os.getenv('client_api_address')
else:
    from tickmine.global_config import client_api_address

def get_rawtick(exch, ins, day_data, time_slice=[], client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = pickle.loads(c.rawtick(exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_kline(exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice', client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = pickle.loads(c.kline(exch, ins, day_data, time_slice, period, subject))
    c.close()
    return temp

def get_tradepoint(exch, ins, day_data, time_slice=[], client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = pickle.loads(c.tradepoint( exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_date(exch, ins, client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = c.date(exch, ins)
    c.close()
    return temp

def get_ins(exch, special_type = '', client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = c.ins(exch, special_type)
    c.close()
    return temp

def get_exch(client_api=client_api_address):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api)
    temp = c.exch()
    c.close()
    return temp
