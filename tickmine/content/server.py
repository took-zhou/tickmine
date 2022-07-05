import socket

import zerorpc
from tickmine.content.info import info
from tickmine.content.k_line import kline
from tickmine.content.level1 import level1
from tickmine.content.m_line import mline
from tickmine.content.raw_tick import rawtick


class remote(object):

    def rawtick(self, exch, ins, day_data):
        return rawtick.get(exch, ins, day_data)

    def kline(self, exch, ins, day_data, period='1T'):
        return kline.get(exch, ins, day_data, period)

    def level1(self, exch, ins, day_data):
        return level1.get(exch, ins, day_data)

    def mline(self, exch, ins, day_data):
        return mline.get(exch, ins, day_data)

    def date(self, exch, ins):
        return info.get_date(exch, ins)

    def ins(self, exch, special_type=''):
        return info.get_instrument(exch, special_type)

    def exch(self):
        return info.get_exchange()


s = zerorpc.Server(remote())
hostname = socket.gethostname()
ip = socket.gethostbyname(hostname)
s.bind("tcp://%s:11332" % (ip))
s.run()
