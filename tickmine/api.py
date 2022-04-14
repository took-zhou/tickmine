import zerorpc
import pickle
import datetime
import re

from tickmine.content.dominant import dominant

# 通过nginx反向代理到5个生产服务
client_api_first = 'tcp://192.168.0.102:8100'
client_api_second = 'tcp://192.168.0.102:8101'
client_api_third = 'tcp://192.168.0.102:8150'

# debug模式
# client_api_first = 'tcp://192.168.0.102:8110'
# client_api_second = 'tcp://192.168.0.102:8120'
# client_api_third = 'tcp://192.168.0.102:8130'

client_api_fourth = 'tcp://192.168.0.106:8150'

try:
    c = zerorpc.Client(timeout=1, heartbeat=None)
    c.connect(client_api_first)
    temp = c.exch()
    c.close()
except:
    client_api_first = 'tcp://onepiece.cdsslh.com:6007'
    client_api_second = 'tcp://onepiece.cdsslh.com:6008'
    client_api_third = 'tcp://onepiece.cdsslh.com:6009'
    client_api_fourth = 'tcp://onepiece.cdsslh.com:6010'

def get_rawtick(exch, ins, day_date, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)

    if exch == 'global':
        temp_dates = get_date(exch, ins)
        if day_date in temp_dates:
            c.connect(client_api_third)
        else:
            c.connect(client_api_fourth)
    else:
        if (datetime.datetime.strptime(day_date, "%Y%m%d")-datetime.datetime.today()).days < -45:
            c.connect(client_api_first)
        else:
            c.connect(client_api_second)

    if '999' in ins:
        temp_ins = dominant.get_ins(exch, ins, day_date)
        temp = pickle.loads(c.rawtick(exch, temp_ins, day_date, time_slice))
    else:
        temp = pickle.loads(c.rawtick(exch, ins, day_date, time_slice))

    c.close()
    return temp

def get_kline(exch, ins, day_date, time_slice=[], period = '1T', subject='lastprice'):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if exch == 'global':
        c.connect(client_api_third)
    else:
        if (datetime.datetime.strptime(day_date, "%Y%m%d")-datetime.datetime.today()).days < -45:
            c.connect(client_api_first)
        else:
            c.connect(client_api_second)

    if '999' in ins:
        temp_ins = dominant.get_ins(exch, ins, day_date)
        temp = pickle.loads(c.kline(exch, temp_ins, day_date, time_slice, period, subject))
    else:
        temp = pickle.loads(c.kline(exch, ins, day_date, time_slice, period, subject))

    c.close()
    return temp

def get_level1(exch, ins, day_date, time_slice=[]):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if exch == 'global':
        c.connect(client_api_third)
    else:
        if (datetime.datetime.strptime(day_date, "%Y%m%d")-datetime.datetime.today()).days < -45:
            c.connect(client_api_first)
        else:
            c.connect(client_api_second)

    if '999' in ins:
        temp_ins = dominant.get_ins(exch, ins, day_date)
        temp = pickle.loads(c.level1( exch, temp_ins, day_date, time_slice))
    else:
        temp = pickle.loads(c.level1( exch, ins, day_date, time_slice))
    c.close()
    return temp

def get_mline(exch, ins, day_date):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    if exch == 'global':
        c.connect(client_api_third)
    else:
        if (datetime.datetime.strptime(day_date, "%Y%m%d")-datetime.datetime.today()).days < -45:
            c.connect(client_api_first)
        else:
            c.connect(client_api_second)

    if '999' in ins:
        temp_ins = dominant.get_ins(exch, ins, day_date)
        temp = pickle.loads(c.mline(exch, temp_ins, day_date))
    else:
        temp = pickle.loads(c.mline(exch, ins, day_date))

    c.close()
    return temp

def _get_ins_date(exch, ins):
    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_first)
    a_temp = c.date(exch, ins)
    c.close()

    c = zerorpc.Client(timeout=300, heartbeat=None)
    c.connect(client_api_second)
    b_temp = c.date(exch, ins)
    c.close()

    if a_temp[-1] in b_temp:
        ret = a_temp + b_temp[b_temp.index(a_temp[-1])+1:]
    else:
        ret = a_temp

    return ret

def _get_main_even_date(exch, ins):
    temp = re.split('([0-9]+)', ins)[0]
    ins_list = get_ins(exch, temp)
    month_compose = dominant.get_compose(exch, temp)

    temp_ret = []
    for item in ins_list:
        if item[-2:] in month_compose.keys():
            temp_ret.append(item)

    temp_date = []
    for item in temp_ret:
        date_list = _get_ins_date(exch, item)
        month_list = month_compose[item[-2:]]
        ins_date = [item_date for item_date in date_list if item_date[4:6] in month_list and item_date[3]]
        temp_date = temp_date + ins_date

    ret_date = list(set(temp_date))
    ret_date.sort()
    return ret_date

def get_date(exch, ins):
    if '999' in ins:
        return _get_main_even_date(exch, ins)
    else:
        return _get_ins_date(exch, ins)

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

if __name__=="__main__":
    ret = get_ins('CZCE', 'TA999_C')
    print(ret)
