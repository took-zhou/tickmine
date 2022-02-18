import streamlit as st
import os
import re
from time import *
import plotly.graph_objs as go
import requests
import pandas as pd

from tickmine.global_config import citic_nature_path
from tickmine.global_config import citic_dst_path
from tickmine.global_config import citic_src_path
from tickmine.global_config import tsaodai_src_path

from tickmine.api import get_exch
from tickmine.api import get_ins
from tickmine.api import get_date

from tickmine.api import get_rawtick
from tickmine.api import get_kline
from tickmine.api import get_tradepoint

def reconstruct_tick_page():
    tick_source = ['CITIC', 'TSAODAI']
    source_option = st.sidebar.selectbox('SOURCE', tick_source)
    if source_option == 'CITIC':
        exchs = get_exch()
        exch_option = st.sidebar.selectbox('EXCH', exchs)

        day_night_option = st.sidebar.selectbox('DAY_NIGHT', [exch_option, '%s_night'%exch_option])

        years = os.listdir('%s/%s/%s/'%(citic_src_path, exch_option, day_night_option))
        years.sort()
        year_option = st.sidebar.selectbox('YEAR', years)

        months = os.listdir('%s/%s/%s/%s/'%(citic_src_path, exch_option, day_night_option, year_option))
        months.sort()
        month_option = st.sidebar.selectbox('MONTH', months)

        month_path = '%s/%s/%s/%s/%s/'%(citic_src_path, exch_option, day_night_option, year_option, month_option)
        if os.path.isdir(month_path):
            dates = os.listdir(month_path)
            dates.sort()
            st.write('`DATE`')
            for item in dates:
                st.write(item)
            st.write('`total item:` %d'%(len(item)))

    elif source_option == 'TSAODAI':
        day_night_option = st.sidebar.selectbox('DAY_NIGHT', ['day', 'night'])

        dates = os.listdir('%s/%s'%(tsaodai_src_path, day_night_option))
        dates.sort()
        st.write('`DATES`')
        for item in dates:
            if item.split('.')[-1] == 'gz':
                st.write(item)

def show_tick_page():
    exchs = get_exch()
    exch_option = st.sidebar.selectbox('EXCH', exchs)

    inses = get_ins(exch_option)
    ins_option = st.sidebar.selectbox('INS', inses)

    dates = get_date(exch_option, ins_option)
    data_option = st.sidebar.selectbox('DATA', dates)


    genre = st.sidebar.radio('data type', ('treadepoint', 'raw tick', '1t kline', '1d kline'))

    if genre == 'raw tick':
        begin_time = time()
        rawtick_df = get_rawtick(exch_option, ins_option, data_option)
        end_time = time()
        run_time = end_time-begin_time
        st.write('rawtick, loading time: `%f`'%(run_time))
        st.write(rawtick_df)

        data = [
            go.Scatter(
                x=rawtick_df.index,
                y=rawtick_df['LastPrice'].values,
                name='tradepoint'
            ),
        ]
        st.plotly_chart(data)

    elif genre == '1t kline':
        begin_time = time()
        kline_df = get_kline(exch_option, ins_option, data_option)
        end_time = time()
        run_time = end_time-begin_time
        st.write('1t kline, loading time: `%f`'%(run_time))
        st.write(kline_df)

    elif genre == '1d kline':
        begin_time = time()
        kline_df = get_kline(exch_option, ins_option, data_option, period='1D')
        end_time = time()
        run_time = end_time-begin_time
        st.write('1d kline, loading time: `%f`'%(run_time))
        st.write('from tsaodai:')
        st.write(kline_df)

        params = {
            "start_date": data_option,
            "end_date": data_option,
            "market": exch_option,
            "index_bar": False
        }
        ret = requests.get('http://192.168.0.102:8205/api/get_futures_daily', params=params, timeout=1).json()
        for item in ret:
            if item['symbol'].lower() == ins_option.lower():
                ret = pd.DataFrame([item], index=[item['date']])
                ret.drop(columns=['symbol', 'date', 'turnover', 'settle', 'pre_settle', 'variety'], inplace=True)
                st.write('from aktools')
                st.write(ret)
                break

    elif genre == 'treadepoint':
        begin_time = time()
        tradepoint_df = get_tradepoint(exch_option, ins_option, data_option)
        end_time = time()
        run_time = end_time-begin_time
        st.write('tradepoint, loading time: `%f`'%(run_time))
        st.write(tradepoint_df)

    if exch_option == 'CZCE':
        st.write('http://www.czce.com.cn/cn/jysj/mrhq/H770301index_1.htm')
    elif exch_option == 'DCE':
        st.write('http://www.dce.com.cn/dalianshangpin/xqsj/tjsj26/rtj/rxq/index.html')
    elif exch_option == 'SHFE':
        st.write('http://www.shfe.com.cn/statements/dataview.html?paramid=kx')
    elif exch_option == 'CFFEX':
        st.write('http://www.cffex.com.cn/rtj/')
    elif exch_option == 'INE':
        st.write('http://www.ine.cn/statements/daily/?paramid=kx')

st.set_page_config(page_title='onepiece operation control', layout='wide', page_icon="..")

genre = st.sidebar.selectbox('operation', ['show', 'reconstruct'])
if genre == 'show':
    show_tick_page()
elif genre == 'reconstruct':
    reconstruct_tick_page()
