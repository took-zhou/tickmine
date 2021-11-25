import zerorpc
import pickle
import configparser
import os

cf = configparser.ConfigParser()
cf.read("%s/.tickmine.conf"%(os.environ.get('HOME')))

if cf.has_option('client', 'address'):
    server_address = cf.get('client', 'address')
else:
    from tickmine.global_config import api_address as server_address

def get_rawtick(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = pickle.loads(c.rawtick(exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_kline(exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = pickle.loads(c.kline(exch, ins, day_data, time_slice, period, subject))
    c.close()
    return temp

def get_tradepoint(exch, ins, day_data, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = pickle.loads(c.tradepoint( exch, ins, day_data, time_slice))
    c.close()
    return temp

def get_date(exch, ins):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = c.date(exch, ins)
    c.close()
    return temp

def get_ins(exch, special_type = ''):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = c.ins(exch, special_type)
    c.close()
    return temp

def get_exch():
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(server_address)
    temp = c.exch()
    c.close()
    return temp
