import zerorpc
import os

from tickmine.content.raw_tick import rawtick
from tickmine.content.trade_point import tradepoint
from tickmine.content.k_line import kline
from tickmine.content.info import info

class remote(object):
    def rawtick(self, exch, ins, day_data, time_slice=[]):
        return rawtick.get(exch, ins, day_data, time_slice)

    def tradepoint(self, exch, ins, day_data, time_slice=[]):
        return tradepoint.get(exch, ins, day_data, time_slice)

    def kline(self, exch, ins, day_data, time_slice=[], period = '1T', subject='lastprice'):
        return kline.get(exch, ins, day_data, time_slice, period, subject)

    def date(self, exch, ins):
        return info.get_date(exch, ins)

    def ins(self, exch, special_type = ''):
        return info.get_instrument(exch, special_type)

    def exch(self):
        return info.get_exchange()

s = zerorpc.Server(remote())
s.bind("tcp://127.0.0.1:11332")
s.run()
