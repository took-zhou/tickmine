import zerorpc
import os

from tickmine.content.raw_tick import rawtick
from tickmine.content.trade_point import tradepoint
from tickmine.content.k_line import kline

class remote(object):
    def rawtick(self, exch, ins, day_data, time_slice=[]):
        return rawtick.get(exch, ins, day_data, time_slice)

    def tradepoint(self, exch, ins, day_data, time_slice=[]):
        return tradepoint.get(exch, ins, day_data, time_slice)

    def kline(self, exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
        return kline.get(exch, ins, day_data, time_slice, period, subject)

s = zerorpc.Server(remote())
s.bind("tcp://192.168.16.7:11332")
s.run()
