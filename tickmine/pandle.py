import streamlit as st
import os
import re
from time import *
import plotly.graph_objs as go

from tickmine.global_config import citic_nature_path
from tickmine.global_config import citic_dst_path
from tickmine.global_config import citic_src_path
from tickmine.global_config import tsaodai_src_path
from tickmine.api import get_rawtick
from tickmine.api import get_kline
from tickmine.api import get_tradepoint

def reconstruct_tick_page():
    tick_source = ['CITIC', 'TSAODAI']
    source_option = st.sidebar.selectbox('SOURCE', tick_source)
    if source_option == 'CITIC':
        exchs = ['INE', 'CFFEX', 'SHFE', 'CZCE', 'DCE']
        exch_option = st.sidebar.selectbox('EXCH', exchs)

        day_night_option = st.sidebar.selectbox('DAY_NIGHT', [exch_option, '%s_night'%exch_option])

        years = os.listdir('%s/%s/%s/'%(citic_src_path, exch_option, day_night_option))
        years.sort()
        year_option = st.sidebar.selectbox('YEAR', years)

        months = os.listdir('%s/%s/%s/%s/'%(citic_src_path, exch_option, day_night_option, year_option))
        months.sort()
        st.write('`MONTH`')
        for item in months:
            st.write(item)
        month_option = st.sidebar.selectbox('MONTH', months)

        month_path = '%s/%s/%s/%s/%s/'%(citic_src_path, exch_option, day_night_option, year_option, month_option)
        if os.path.isdir(month_path):
            dates = os.listdir(month_path)
            dates.sort()
            st.write('`DATE`')
            for item in dates:
                st.write(item)

    elif source_option == 'TSAODAI':
        day_night_option = st.sidebar.selectbox('DAY_NIGHT', ['day', 'night'])

        dates = os.listdir('%s/%s'%(tsaodai_src_path, day_night_option))
        dates.sort()
        st.write('`DATES`')
        for item in dates:
            if item.split('.')[-1] == 'gz':
                st.write(item)

def show_tick_page():
    exchs = ['INE', 'CFFEX', 'SHFE', 'CZCE', 'DCE']
    exch_option = st.sidebar.selectbox('EXCH', exchs)

    years = ['2021', '2020', '2019','2018', '2017', '2016', '2015']
    year_option = st.sidebar.selectbox('EXCH', years)

    ins_list = [exch for exch in os.listdir('%s/%s/%s'%(citic_dst_path, exch_option, exch_option))]

    year_ins = []
    for item in ins_list:
        year_number = ''.join(re.findall(r'[0-9]', item))
        if exch_option == 'CZCE':
            if year_option[-1:] == year_number[:1]:
                year_ins.append(item)
        else:
            if year_option[-2:] == year_number[:2]:
                year_ins.append(item)

    ins_types = set()
    for item in year_ins:
        type = ''.join(re.findall(r'[a-zA-Z]', item))
        ins_types.add(type)

    ins_types = list(ins_types)
    ins_types.sort()
    ins_type_option = st.sidebar.selectbox('INS_TYPE', ins_types)

    ins_list = [item for item in year_ins if ins_type_option in item]
    ins_list.sort()
    ins_option = st.sidebar.selectbox('INS', ins_list)

    data_list = [item.split('.')[0].split('_')[-1] for item in os.listdir('%s/%s/%s/%s'%(citic_dst_path, exch_option, exch_option, ins_option))]
    months = set()
    for item in data_list:
        months.add(item[:6])

    months= list(months)
    months.sort()
    month_option = st.sidebar.selectbox('MONTH', months)

    month_data_list = [item for item in data_list if month_option in item]
    month_data_list.sort()
    data_option = st.sidebar.selectbox('DATA', month_data_list)


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
        st.write(kline_df)

    elif genre == 'treadepoint':
        begin_time = time()
        tradepoint_df = get_tradepoint(exch_option, ins_option, data_option)
        end_time = time()
        run_time = end_time-begin_time
        st.write('tradepoint, loading time: `%f`'%(run_time))
        st.write(tradepoint_df)

st.set_page_config(page_title='onepiece operation control', layout='wide', page_icon="..")

genre = st.sidebar.selectbox('operation', ['show', 'reconstruct'])
if genre == 'show':
    show_tick_page()
elif genre == 'reconstruct':
    reconstruct_tick_page()
