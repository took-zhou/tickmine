#coding=utf-8
import datetime

import _pickle as cPickle
import pandas as pd
import zerorpc

global client_api


class Api():

    def __init__(self, link):
        self.client_api = link

    def _get_night_date(self, _data):
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

    def _get_time_slice(self, _data, _time):
        ret = ['', '']

        if '16:00:00' <= _time[0] <= '24:00:00':
            ret[0] = datetime.datetime.strptime(self._get_night_date(_data) + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[0] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[0] = datetime.datetime.strptime(one_day_after_str + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[0] != '':
            ret[0] = datetime.datetime.strptime(_data + _time[0], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        if '16:00:00' <= _time[1] <= '24:00:00':
            ret[1] = datetime.datetime.strptime(self._get_night_date(_data) + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif '00:00:00' <= _time[1] <= '03:00:00':
            night_date = self._get_night_date(_data)
            one_day_after = pd.to_datetime(night_date, format='%Y-%m-%d') + datetime.timedelta(days=1)
            split = str(one_day_after).split('-')
            one_day_after_str = split[0] + split[1] + split[2].split(' ')[0]
            ret[1] = datetime.datetime.strptime(one_day_after_str + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
        elif _time[1] != '':
            ret[1] = datetime.datetime.strptime(_data + _time[1], '%Y%m%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")

        return ret

    def _get_time_slice_sina(self, _data, _time):
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

    def get_rawtick(self, exch, ins, day_date, time_slice=[]):
        c = zerorpc.Client(timeout=300, heartbeat=None)
        c.connect(self.client_api)
        temp = cPickle.loads(c.rawtick(exch, ins, day_date))

        if len(time_slice) == 2:
            if exch == 'global':
                _time_slice = self._get_time_slice_sina(day_date, time_slice)
            else:
                _time_slice = self._get_time_slice(day_date, time_slice)
            temp = temp.truncate(before=_time_slice[0], after=_time_slice[1])
        c.close()

        return temp

    def get_date(self, exch, ins):
        temp = []

        c = zerorpc.Client(timeout=300, heartbeat=None)
        c.connect(self.client_api)
        temp = c.date(exch, ins)
        c.close()

        return temp

    def get_ins(self, exch, special_type='', special_date=''):
        temp = []
        c = zerorpc.Client(timeout=300, heartbeat=None)
        c.connect(self.client_api)
        temp = c.ins(exch, special_type, special_date)
        c.close()

        return temp

    def get_exch(self):
        temp = []
        c = zerorpc.Client(timeout=300, heartbeat=None)
        c.connect(self.client_api)
        temp = c.exch()
        c.close()

        return temp


if __name__ == "__main__":
    import time

    from tickmine.api import Api
    api = Api('tcp://192.168.0.102:8110')
    start = time.time()
    ret = api.get_exch()
    print(ret)

    end = time.time()
    runTime = end - start
    print("run time: ", runTime)
