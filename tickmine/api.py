#coding=utf-8
import datetime
import re
import grpc
import threading
import _pickle as cPickle

from tickmine.topology import topology
from tickmine import tick_pb2
from tickmine import tick_pb2_grpc

channel_options = [
    ("grpc.keepalive_time_ms", 8000),
    ("grpc.keepalive_timeout_ms", 5000),
    ("grpc.http2.max_pings_without_data", 5),
    ("grpc.keepalive_permit_without_calls", 1),
    ('grpc.max_receive_message_length', 100 * 1024 * 1024),
    ('grpc.max_send_message_length', 100 * 1024 * 1024),
]

try:
    with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic_self1'], options=channel_options) as channel:
        stub = tick_pb2_grpc.TickStub(channel)
        temp = [response.info for response in stub.Exch(tick_pb2.Empty())]
except:
    print('select external api')
    for item in topology.gradation_list:
        if '192.168.0.102' in item['access_api']:
            topology.ip_dict[item['docker_name']] = item['access_api'].replace('192.168.0.102', 'tsaodai.com')
        if '192.168.0.104' in item['access_api']:
            topology.ip_dict[item['docker_name']] = item['access_api'].replace('192.168.0.104', 'tsaodai.com')


def _get_year(exch, ins, date):
    resplit = re.findall(r'([0-9]*)([A-Z,a-z]*)', ins)
    begin = 200
    split_date = ''
    for i in range(10):
        split_date = str(begin + i) + resplit[1][0][-3:] + '31'
        if split_date >= date:
            break

    return split_date[0:4]


def _get_data(func, exch, ins, date, period):
    request = tick_pb2.RequestPara1(exch=exch, ins=ins, date=date, period=period)
    if exch in ['SHSE', 'SZSE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_zhongtai1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
    elif exch in ['GATE']:
        if (datetime.datetime.today() - datetime.datetime.strptime(date, "%Y%m%d")).days <= 45:
            with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate_self1'], options=channel_options) as channel:
                stub = tick_pb2_grpc.TickStub(channel)
                return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
        else:
            with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate1'], options=channel_options) as channel:
                stub = tick_pb2_grpc.TickStub(channel)
                return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
    elif exch in ['FXCM']:
        if (datetime.datetime.today() - datetime.datetime.strptime(date, "%Y%m%d")).days <= 45:
            with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
                stub = tick_pb2_grpc.TickStub(channel)
                return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
        else:
            with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
                stub = tick_pb2_grpc.TickStub(channel)
                return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
    else:
        if (datetime.datetime.today() - datetime.datetime.strptime(date, "%Y%m%d")).days <= 45:
            with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic_self1'], options=channel_options) as channel:
                stub = tick_pb2_grpc.TickStub(channel)
                return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
        else:
            if _get_year(exch, ins, date) <= '2022':
                with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic1'], options=channel_options) as channel:
                    stub = tick_pb2_grpc.TickStub(channel)
                    return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]
            else:
                with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic2'], options=channel_options) as channel:
                    stub = tick_pb2_grpc.TickStub(channel)
                    return [cPickle.loads(response.info) for response in stub.__getattribute__(func)(request)][0]


def _stream_data(func, exch, ins, period):
    if exch in ['SHSE', 'SZSE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_zhongtai1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            for response in stub.__getattribute__(func)(tick_pb2.RequestPara1(exch=exch, ins=ins, date="", period=period)):
                yield cPickle.loads(response.info)
    elif exch in ['GATE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            for response in stub.__getattribute__(func)(tick_pb2.RequestPara1(exch=exch, ins=ins, date="", period=period)):
                yield cPickle.loads(response.info)
    elif exch in ['FXCM']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            for response in stub.__getattribute__(func)(tick_pb2.RequestPara1(exch=exch, ins=ins, date="", period=period)):
                yield cPickle.loads(response.info)
    else:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            for response in stub.__getattribute__(func)(tick_pb2.RequestPara1(exch=exch, ins=ins, date="", period=period)):
                yield cPickle.loads(response.info)

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic2'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            for response in stub.__getattribute__(func)(tick_pb2.RequestPara1(exch=exch, ins=ins, date="", period=period)):
                yield cPickle.loads(response.info)


# 有可能是python3.8线程退出异常，打的补丁
def patch_threading_shutdown():
    _origin_shutdown = threading._shutdown

    def _patched_shutdown():
        non_daemon_threads = [t for t in threading.enumerate() if t != threading.current_thread() and not t.daemon]
        if len(non_daemon_threads) > 1:
            _origin_shutdown()

    threading._shutdown = _patched_shutdown


def get_rawtick(exch, ins, date):
    return _get_data('Rawtick', exch, ins, date, '')


def stream_rawtick(exch, ins):
    yield from _stream_data('Rawtick', exch, ins, '')


def get_kline(exch, ins, date, period='1T'):
    return _get_data('Kline', exch, ins, date, period)


def stream_kline(exch, ins, period='1T'):
    yield from _stream_data('Kline', exch, ins, period)


def get_mline(exch, ins, date):
    return _get_data('Mline', exch, ins, date, '')


def stream_mline(exch, ins):
    yield from _stream_data('Mline', exch, ins, '')


def get_date(exch, ins):
    temp = []

    if exch in ['SHSE', 'SZSE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_zhongtai1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]
    elif exch in ['GATE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate_self1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_a = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_b = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]

        temp = list(set(temp_a + temp_b))
        temp.sort()
    elif exch in ['FXCM']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]
    else:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_a = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic2'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_b = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic_self1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_c = [response.info for response in stub.Date(tick_pb2.RequestPara3(exch=exch, ins=ins))]

        temp = list(set(temp_a + temp_b + temp_c))
        temp.sort()

    return temp


def get_ins(exch, special_type='', special_date=''):
    temp = []

    if exch in ['SHSE', 'SZSE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_zhongtai1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]
    elif exch in ['GATE']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate_self1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_a = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_b = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]

        temp = temp_a + temp_b
        temp = list(set(temp))
    elif exch in ['FXCM']:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]
    else:
        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_a = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic2'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_b = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]

        with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic_self1'], options=channel_options) as channel:
            stub = tick_pb2_grpc.TickStub(channel)
            temp_c = [response.info for response in stub.Ins(tick_pb2.RequestPara2(exch=exch, type=special_type, date=special_date))]

        temp = temp_a + temp_b + temp_c
        temp = list(set(temp))

    return temp


def get_exch():
    temp = []
    with grpc.insecure_channel(target=topology.ip_dict['tickserver_zhongtai1'], options=channel_options) as channel:
        stub = tick_pb2_grpc.TickStub(channel)
        security_exch = [response.info for response in stub.Exch(tick_pb2.Empty())]

    with grpc.insecure_channel(target=topology.ip_dict['tickserver_gate_self1'], options=channel_options) as channel:
        stub = tick_pb2_grpc.TickStub(channel)
        crypto_exch = [response.info for response in stub.Exch(tick_pb2.Empty())]

    with grpc.insecure_channel(target=topology.ip_dict['tickserver_fxcm1'], options=channel_options) as channel:
        stub = tick_pb2_grpc.TickStub(channel)
        forex_exch = [response.info for response in stub.Exch(tick_pb2.Empty())]

    with grpc.insecure_channel(target=topology.ip_dict['tickserver_citic_self1'], options=channel_options) as channel:
        stub = tick_pb2_grpc.TickStub(channel)
        future_exch = [response.info for response in stub.Exch(tick_pb2.Empty())]

    temp = security_exch + crypto_exch + forex_exch + future_exch
    temp.sort()

    return temp


patch_threading_shutdown()

if __name__ == "__main__":
    print(get_exch())
    print(get_ins("CZCE"))
    print(get_date("CZCE", 'AP210'))
    print(get_rawtick("CZCE", 'AP210', '20220722'))
    for item in stream_kline("CZCE", "AP210"):
        print(item)
