#!/usr/bin/env python
# coding: utf-8

# imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch
import sys
import os
import chat_numbers

# Set bot
bot = telegram.Bot(token=os.environ.get("ALARM_BOT_TOKEN"))
chat_id_group = chat_numbers.group

# Functions to catch anomaly and send report
# add max and min values to table for alert from average and weighted data
def add_trigger_values(df, metric):
    df['trigger_top'] = (df['adjusted_threshold'] + 1) * df[metric]
    df['trigger_low'] = df[metric] / (df['adjusted_threshold'] + 1) 
    return df



# calculate avg value from month, get weighted values and "normal corridor" for our value to check
def add_thresholds_to_df(df, metric, threshold):
    # get average metrics by month, week, yesterday
    df['avg_metric_mnth'] = df.groupby('hm')[metric].transform(lambda x: x.mean())
    df_week = df[-672:]
    df['avg_metric_week'] = df_week.groupby('hm')[metric].transform(lambda x: x.mean())
    df_day = df[-192:-96]
    df['avg_metric_day'] = df_day.groupby('hm')[metric].transform(lambda x: x.mean())
    
    # set weights and get metric weighted values
    weights = [.4, .3, .3]
    df['avg_metric_weighted'] = weights[0]*df['avg_metric_mnth'] + weights[1]*df['avg_metric_week'] + weights[2]*df['avg_metric_day']
    df_weighted = df[['hm', 'avg_metric_weighted']].dropna()

    # dispatch values on all set to visually check
    dict_weighted = df_weighted.set_index('hm').to_dict()['avg_metric_weighted']
    df['avg_metric_weighted'] = df['hm'].map(dict_weighted)
    df['avg_metric_weighted_smooth'] = df.rolling(window=9, center=True)['avg_metric_weighted'].mean()

    # adjust threshold
    threshold_of_threshold = df.avg_metric_mnth.mean() * .9
    upd_threshhold = 2 * threshold + (df['avg_metric_weighted_smooth'] * (-threshold/threshold_of_threshold))
    df['adjusted_threshold']  = np.where(df['avg_metric_weighted_smooth'] < threshold_of_threshold, upd_threshhold, threshold)
    
    # get trigger margins
    df = add_trigger_values(df, 'avg_metric_weighted_smooth')
    df['anomaly'] = df[(df[metric] < df['trigger_low']) | (df[metric] > df['trigger_top'])]['ts']
        
    return df



# Check anomaly, returning 1 if anomaly, else 0
def check_anomaly(df, metric):
    # check current with day before (because full day data) trigger values (from threshhold)
    current_ts = df['ts'].max()  # last 15 min data
    day_ago_ts = current_ts - pd.DateOffset(days=1)  # same 15 min from day before
    current_value = df[df['ts'] == current_ts][metric].iloc[0] # metric from current 15 min
    
    # get normal corridor (low and top trigger values from table)
    avg_expected_value = df[df['ts'] == day_ago_ts]['avg_metric_weighted'].iloc[0]
    trigger_low_value = df[df['ts'] == day_ago_ts]['trigger_low'].iloc[0]
    trigger_top_value = df[df['ts'] == day_ago_ts]['trigger_top'].iloc[0]

    # check if current value is normal or beyond
    if trigger_low_value < current_value < trigger_top_value:
        is_alert = 0
    else:
        is_alert = 1
    return is_alert, current_value, avg_expected_value



# Plot and send anomaly # maybe better to split into two functions further
def plot_anomaly(df, metric, color, current_value, avg_expected_value):
    start_today = df.date.iloc[-1]
    start_yesterday = start_today - pd.DateOffset(days=1)
    data_today = df[df.ts > start_today]
    data_yesterday = df[(df.ts < start_today) & (df.ts > start_yesterday)]

    sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
    sns.set_style('whitegrid')

    # plot data 
    ax = sns.lineplot( 
            data=data_today,
            x="hm", y=metric, color=color
            )
    plt.plot(data_yesterday.hm, data_yesterday['avg_metric_weighted_smooth'], alpha=.4, color='gray')
    plt.fill_between(data_yesterday.hm, data_yesterday.trigger_low, data_yesterday.trigger_top, alpha=.1, color='gray')

    plt.legend([metric, 'Smooth avg', 'Normal zone'])
    plt.title('plotting anomaly for: '+ str(metric))

    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % 3 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)

    ax.set(xlabel='Time')
    ax.set(ylabel=metric)
    plt.xticks(rotation=90)
    # plt.show() # comment this
    
    # write alert message
    if metric == 'CTR' or metric == 'Average messages by user':    
        msg = f'{metric} alert. Current value is {current_value:.2f}, {current_value/avg_expected_value*100:.2f}% from expected'
    else:
        msg = f'{metric} alert. Current value is {current_value}, {current_value/avg_expected_value*100:.2f}% from expected'
    
    bot.sendMessage(chat_id=chat_id_group, text=msg)
    # bot.sendMessage(chat_id=chat_id_my, text=msg)

    
    # send plot
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = metric + '_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id_group, photo=plot_object)
    # bot.sendPhoto(chat_id=chat_id_my, photo=plot_object)


    
    
# Main programm
# Get data about users of newsfeed
query = '''SELECT
                          toStartOfFifteenMinutes(time) AS ts
                        , toDate(ts) as date
                        , formatDateTime(ts, '%R') AS hm
                        , uniqExact(user_id) AS "Unique users, feed"
                    FROM simulator_20220420.feed_actions
                    

                    WHERE ts > subtractDays(toStartOfDay(toDate(now())), 30)
                    AND ts < toStartOfFifteenMinutes(now())
                    
                    GROUP BY ts, date, hm
                    ORDER BY ts 
                    '''
data = Getch(query).df
# ...and actions of users, how to make it by one query?
query = '''SELECT
                          toStartOfFifteenMinutes(time) AS ts
                        , action AS action
                        , count(user_id) AS Events
                        
                    FROM simulator_20220420.feed_actions
                    
                    WHERE ts > subtractDays(toStartOfDay(toDate(now())), 30)
                    AND ts < toStartOfFifteenMinutes(now())
                    
                    GROUP BY ts, action
                    ORDER BY ts 
                    '''
data_likes_views = Getch(query).df

# messenger data
query = '''SELECT
        toStartOfFifteenMinutes(time) AS ts,
        COUNT(DISTINCT user_id) AS "Unique users, messenger",
        COUNT(reciever_id) as Messages
        FROM simulator_20220420.message_actions

        WHERE ts > subtractDays(toStartOfDay(toDate(now())), 30)
        AND ts < toStartOfFifteenMinutes(now())

        GROUP BY toStartOfFifteenMinutes(time) AS ts
        ORDER BY ts;'''
data_msg = Getch(query).df
data_msg['Average messages by user'] = data_msg['Messages'] / data_msg['Unique users, messenger']

# prepare all data (merges)
# combine unique users and views-likes
data_views = data_likes_views[data_likes_views.action == 'view']
data_likes = data_likes_views[data_likes_views.action == 'like']
data_likes.rename(columns={'Events': 'Likes'}, inplace=True)
data_views.rename(columns={'Events': 'Views'}, inplace=True)

data_all = pd.merge(data, data_views, on='ts')
data_all = pd.merge(data_all, data_likes, on='ts')
data_all['CTR'] = data_all.Likes / data_all.Views

data_all.drop(['action_x', 'action_y'], axis=1, inplace=True)
data_all = pd.merge(data_all, data_msg, on='ts')

# construct list for metrics (with own thresholds and plot colors)                     
check_list = \
             ['Unique users, feed', .27, 'green'], \
             ['Views', .32, '#599AC8'], \
             ['Likes', .36, '#FFAB63'], \
             ['CTR', .13, 'red'], \
             ['Unique users, messenger', .29, 'b'], \
             ['Messages', .29, '#266E5C'], \
             ['Average messages by user', .2, '#309AA8']
# Messages don't have much anomalies last month
# Average messages by user going down!             

# check all metrics for alerts (with own thresholds and plot colors for each other)
for item in check_list:
    metric = item[0] 
    threshold = item[1]
    color = item[2]
    data_all_thresholded = add_thresholds_to_df(data_all, metric, threshold)
    is_alert, current_value, avg_expected_value = check_anomaly(data_all_thresholded, metric)

    # is_alert check to send report. Also there are connected metrics, maybe not to send all of them?
    if is_alert == 1:
        plot_anomaly(data_all_thresholded, metric, color, current_value, avg_expected_value)
    else:
        pass
        # plot_anomaly(data_all_thresholded, metric, color, current_value, avg_expected_value) # for tests, will be pass here in prod
