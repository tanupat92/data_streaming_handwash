#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dash
import numpy as np
from dash.dependencies import Output, Input
from dash import dcc
from dash import html
import plotly.graph_objs as go

from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
from collections import defaultdict

import asyncio
from threading import Thread

global CONSECUTIVE_NOT_WASH
CONSECUTIVE_NOT_WASH =3

app = dash.Dash(__name__)
app.layout = html.Div(
    [
        html.H1(f"Personnel hand-washing in dialysis ward after {CONSECUTIVE_NOT_WASH} contacts at time: 0", id='title-text'),
        # dcc.Graph(id='live-graph', animate=True),
        dcc.Graph(id='live-graph2', animate=True),
        # dcc.Interval(
        #     id='graph-update',
        #     interval = 1000 # run graph-update every 1 sec
        #     ),
        dcc.Interval(
            id='graph-update2',
            interval = 2000 # run graph-update every 1 sec
            )
        ]
        
)

@app.callback(Output('title-text', 'children'),
              [Input('graph-update2', 'n_intervals')])
def update_title(input_data):
    return f"Personnel hand-washing in dialysis ward after {CONSECUTIVE_NOT_WASH} contacts at time: {int(TIME) * 8/60:.2f} minutes"

@app.callback(Output('live-graph2', 'figure'),
    [Input('graph-update2', 'n_intervals')])
def update_graph2(input_data):
    size = df_raw['no_wash_num'].copy()
    size.loc[size<=3] = 3
    size.loc[size > 50] = 50
    size = size.to_numpy(dtype=float)
    size = np.log(size)
    size *= 8
    df_raw.loc[:, 'no_wash'] = df_raw.loc[:, 'no_wash_num'] >= CONSECUTIVE_NOT_WASH
    color = ["#FF0000" if v else "#808000" for v in df_raw['no_wash']]
    data = go.Scatter(
        x = list(df_raw['x']),
        y = list(df_raw['y']),
        name ='Scatter',
        mode = 'markers+text',
        marker_color=color,
        text=list(df_raw['key']),
        textposition="top center",
        marker=dict(
            size=list(size))
        )
    fig = go.Figure(data=data)
    fig.update_layout(xaxis=dict(range=[-1, 1]),
                      yaxis=dict(range=[-1, 1]),
                      width=800,
                      height=800
                      )
    fig.update_xaxes(showgrid=False, showticklabels=False)
    fig.update_yaxes(showgrid=False, showticklabels=False)
    for station in unique_station:
        x = location.loc[location['station'] == station, 'x']
        y = location.loc[location['station'] == station, 'y']
        x0 = min(x)
        x1 = max(x)
        y0 = min(y)
        y1 = max(y)
        fig.add_shape(type="rect",
            x0=x0, x1=x1, y0=y0, y1=y1)
        if station in [10,11]:
            text = 'sink'
        elif station == 12:
            text = 'counter'
        else:
            text = str(station)
        fig.add_annotation(
            x=x0+0.06,
            y=y0+0.08,
            text=text,
            showarrow=False,
            font_size=12
            )

    return fig

def iden_station(x, y, x_range, y_range):
    combined = x_range.apply(lambda k: within(k, x)) & y_range.apply(lambda k: within(k, y))
    val = combined.loc[combined].index.values
    if len(val)>0:
        return val[0]
    else:
        return -1

def within(k, x):
    if k[0] < k[1] :
        return k[0] <= x and k[1] >=x
    else: 
        return k[0] >= x and k[1] <=x
        
def odb_consumer():
    # prep station
    global location
    location = pd.read_csv("station_0ft.csv")
    global unique_station
    unique_station = np.unique(location['station'])
    x_range = location.groupby("station").apply(lambda x: x.x.unique())
    y_range = location.groupby("station").apply(lambda x: x.y.unique())

    consumer = KafkaConsumer('movement_day2',bootstrap_servers='10.0.0.50:29092',api_version=(2,0,2), 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    print('\nWaiting for INPUT TUPLES, Ctr/Z to stop ...')
    # prep dictionary washing hand time
    global not_wash_dict
    global df_raw
    global TIME
    df_raw = pd.DataFrame(columns=["key", "x", "y", "time", "no_wash", "no_wash_num"])
    wash_dict_agg = defaultdict(lambda:0) 
    wash_dict_time = defaultdict(lambda:0)
    not_wash_dict = defaultdict(lambda:0)
    TIME = 0

    tuples = [] 
    for message in consumer:
        key = list(message.value.keys())[0]
        time = message.value[key]["time"]
        x = message.value[key]["x"]
        y = message.value[key]["y"]
        in_tuple = (key, 
            time, 
            x, 
            y)

        TIME = str(time)
        station = iden_station(x, y, x_range, y_range)
        if station not in [10,11]:
            if wash_dict_agg[key] > 0:
                print(f"Personnel {key} took {wash_dict_agg[key] * 8} seconds for washing hands")
            wash_dict_time[key] = 0
            wash_dict_agg[key] = 0 
            if station != -1 and station != 12:
                not_wash_dict[key] += 1 
                
        not_wash_list = [key for key, val in not_wash_dict.items() if val>=CONSECUTIVE_NOT_WASH]
        if len(not_wash_list) > 0 and key in not_wash_list and station not in [10,11,-1]:
            print(f"Personnel {key} doesn't wash hand and contact patients!!")

        if station in [10,11]:
            not_wash_dict[key] = 0 
            if wash_dict_agg[key] != 0:
                if wash_dict_agg[key]==-1:
                    wash_dict_agg[key]=0
                wash_dict_agg[key] += int(time) - wash_dict_time[key] 
            if wash_dict_time[key] == 0 :
                wash_dict_agg[key] = -1
            wash_dict_time[key] = int(time)
        tuples.append(in_tuple)

        # add location to df_raw
        if key not in list(df_raw['key']):
            df_raw = df_raw.append(
                {'key': key, 'x': x, 'y': y, 'time': wash_dict_agg[key] * 8, 'no_wash' : False, 'no_wash_num': not_wash_dict[key]}, ignore_index = True)

        else:
            df_raw.loc[df_raw['key'] == key, :] = [key, x, y, wash_dict_agg[key] * 8, False, not_wash_dict[key]]


        if int(message.value[key]["time"])>200:
            break

def async_main_wrapper():
    """Not async Wrapper around async_main to run it as target function of Thread"""
    asyncio.run(odb_consumer())

if __name__ == '__main__':

    th = Thread(target=async_main_wrapper)
    th.start()

    app.run_server(debug=True)
    