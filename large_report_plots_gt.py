#!/usr/bin/env python
# coding: utf-8
import pandahouse as ph
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from read_db.CH import Getch 
import telegram
import io
from matplotlib_venn import venn2 
import chat_numbers

def test_report(chat=None):
    # create the bot
    bot = telegram.Bot(token="ALARM_BOT_TOKEN")
    chat_id = chat_numbers.group

    msg = 'Global report for newsfeed and messenger'
    bot.sendMessage(chat_id=chat_id, text=msg)


    # data for WAU
    query = '''SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
           count(DISTINCT user_id) AS "Unique users"
    FROM simulator_20220420.feed_actions
    WHERE time < toStartOfDay(toDate(now()))
    AND time > subtractWeeks(toStartOfDay(toDate(now())), 1)
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY __timestamp;'''
    data = Getch(query).df

    # get views, likes, ctr
    query = '''SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
           action AS action,
           count(user_id) AS "Events"
    FROM simulator_20220420.feed_actions
    WHERE time < toStartOfDay(toDate(now()))
    AND time > subtractDays(toStartOfDay(toDate(now())), 7)
    GROUP BY action,
             toStartOfDay(toDateTime(time))
             ORDER BY __timestamp;'''
    data_likes_views = Getch(query).df

    data_views = data_likes_views[data_likes_views.action == 'view']
    data_likes = data_likes_views[data_likes_views.action == 'like']
    data_likes.rename(columns={'Events': 'Likes'}, inplace=True)
    data_views.rename(columns={'Events': 'Views'}, inplace=True)
    data['Date'] = data['__timestamp'].dt.strftime('%m-%d')
    data_all = pd.merge(data, data_views, on='__timestamp')
    data_all = pd.merge(data_all, data_likes, on='__timestamp')
    data_all['CTR'] = data_all.Likes / data_all.Views

    # plot data for newsfeed
    plt.subplots(1,3, figsize=(20,6))
    plt.suptitle('   Week dashboard for newsfeed', fontsize=14)

    plt.subplot(1, 3, 1)
    sns.lineplot(data=data_all, x='Date', y='Unique users', color='g')
    plt.grid(ls=':', lw=1)    
    plt.ylim(0, data_all['Unique users'].max()*1.1)
    delta_users = data_all['Unique users'].max() - data_all['Unique users'].min()
    if delta_users > 0:   
        users_text = str(delta_users)+ ' new users'
        plt.text(x=5.8, y=data_all['Unique users'].max()*1.04, s=users_text, ha='right')
    plt.fill_between(data_all.Date, data_all['Unique users'], alpha=.04, color='g')
    plt.title('Unique users')

    plt.subplot(1, 3, 2)
    sns.lineplot(data=data_all, x='Date', y='Views')
    sns.lineplot(data=data_all, x='Date', y='Likes')
    plt.grid(ls=':', lw=1)    
    plt.title('Views and likes')
    plt.legend(['Views', 'Likes'])
    plt.ylim(0, data_all.Views.max()*1.1)

    plt.subplot(1, 3, 3)
    sns.lineplot(data=data_all, x='Date', y='CTR', color='r')
    plt.grid(ls=':', lw=1)    
    plt.title('CTR')
    plt.ylim(0, 1)
    # plt.show() # comment this line to send pic

    # send plots
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'plots.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    # Week dashboars for messenger
    ## Unique users in messenger only by days
    ## Number of messages by days
    ## Number of messager per user by days

    # data for WAU, messages in messenger
    query = '''SELECT toStartOfDay(toDateTime(time)) AS __timestamp,
           count(DISTINCT user_id) AS "Unique users",
           COUNT(reciever_id) as messages
    FROM simulator_20220420.message_actions
    WHERE time < toStartOfDay(toDate(now()))
    AND time > subtractWeeks(toStartOfDay(toDate(now())), 1)
    GROUP BY toStartOfDay(toDateTime(time))
    ORDER BY __timestamp;'''
    data_msg = Getch(query).df
    data_msg['Date'] = data_msg['__timestamp'].dt.strftime('%m-%d')
    data_msg['Messages by user'] = data_msg.messages / data_msg['Unique users']

    # vis messenger data
    plt.subplots(1,3, figsize=(20,6))
    plt.suptitle('  Week dashboard for messenger', fontsize=14)

    plt.subplot(1, 3, 1)
    sns.lineplot(data=data_msg, x='Date', y='Unique users', color='b')
    plt.grid(ls=':', lw=1)    
    plt.ylim(0, data_msg['Unique users'].max()*1.1)
    delta_users = data_msg['Unique users'].max() - data_msg['Unique users'].min()
    if delta_users > 0:   
        users_text = str(delta_users)+ ' new users'
        plt.text(x=5.8, y=data_msg['Unique users'].max()*1.04, s=users_text, ha='right')
    plt.fill_between(data_msg.Date, data_msg['Unique users'], alpha=.04, color='b')
    plt.title('Number of unique users')

    plt.subplot(1, 3, 2)
    sns.lineplot(data=data_msg, x='Date', y='messages')
    plt.grid(ls=':', lw=1)    
    plt.title('Number of messages')
    plt.text(x=5.9, y=data_msg['messages'].max()*1.02, s='that\'s weird...', ha='right')
    plt.ylim(0, data_msg.messages.max()*1.1)

    plt.subplot(1, 3, 3)
    sns.lineplot(data=data_msg, x='Date', y='Messages by user', color='orange')
    plt.grid(ls=':', lw=1)    
    plt.title('Average number of messages by user')
    plt.ylim(0, data_msg['Messages by user'].max()*1.1)
    # plt.show() # comment this line to send pic

    # send plots
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'plots_msg.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    # Data for both services
    ## OS distribution # OK
    ## Num of users by services # OK
    ## Actions of users (messages and views and likes by days) # OK
    ## Distribution of users (messaging - both - newsfeed) venn diagram # OK

    # get data for both services
    query = '''SELECT * FROM
            (SELECT 
            toDate(time) as time,
            user_id,
            MAX(gender) as gender,
            MAX(age) as age,
            MAX(city) as city,
            MAX(country) as country,
            MAX(os) as os,
            MAX(source) as source,
            COUNT(action='view') as views,
            COUNT(action='like') as likes
            FROM simulator_20220420.feed_actions
            WHERE time < toStartOfDay(toDate(now()))
            AND time > subtractWeeks(toStartOfDay(toDate(now())), 1)
            GROUP BY user_id, time
            ORDER BY time
            ) AS feed
        FULL JOIN
            (SELECT 
            toDate(time) as time,
            user_id,
            MAX(gender) as gender,
            MAX(age) as age,
            MAX(city) as city,
            MAX(country) as country,
            MAX(os) as os,
            MAX(source) as source,
            COUNT(reciever_id) as messages
            FROM simulator_20220420.message_actions
            WHERE time < toStartOfDay(toDate(now()))
            AND time > subtractWeeks(toStartOfDay(toDate(now())), 1)
            GROUP BY user_id, time
            ORDER BY time
            ) AS messenger

        USING user_id, time, age, city, country, gender, os, source'''
    data = Getch(query).df
    data['gender_label'] = np.where((data.gender == 1), 'male', 'female')
    data['Date'] = data['time'].dt.strftime('%m-%d') 

    # OS distribution
    plt.subplots(1, 2, figsize=(12, 6))
    plt.suptitle('  OS distribution', fontsize=14)

    plt.subplot(1, 2, 2)

    pie_data = data.value_counts('os')
    plt.pie(pie_data, labels=pie_data.index, autopct='%.2f')
    plt.title('Proportion')

    plt.subplot(1, 2, 1)
    # data for os plot
    data_os = pd.DataFrame(data.groupby(['Date','os'])['user_id'].count()).reset_index()
    data_os.rename(columns={'user_id':'Users'}, inplace=True)
    data_os_a = data_os[data_os.os == 'Android']
    data_os_i = data_os[data_os.os == 'iOS']

    # os plot
    plt.plot(data_os_a.Date, data_os_a.Users)
    plt.plot(data_os_i.Date, data_os_i.Users)
    plt.ylim(0, data_os.Users.max()*1.1)
    plt.grid(ls=':', lw=1)    
    plt.title('OS distribution by week')
    plt.ylabel('Users')
    plt.xlabel('Date')
    plt.legend(['Android', 'iOS'], loc=4)
    # plt.show()

    # send plots
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'both_services1.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    # Num of users by services
    # Actions of users (messages and views and likes by days)
    total_users = data.user_id.nunique()
    # print('total users', total_users)

    # data for users by actions, by user
    messaging_and_feed_users_id = data.loc[(data.messages > 0) & (data.views > 0)].groupby('user_id').sum() # OK
    feed_only_users_id = data[data.messages == 0].groupby('user_id').sum() # non
    messaging_only_users_id = data.loc[(data.messages > 0) & (data.views == 0)].groupby('user_id').sum()

    # data for users by actions, by date
    messaging_and_feed_users = data.loc[(data.messages > 0) & (data.views > 0)].groupby('time')['user_id'].count()
    feed_only_users = data[data.messages == 0].groupby('time')['user_id'].count()
    messaging_only_users = data.loc[(data.messages > 0) & (data.views == 0)].groupby('time')['user_id'].count()

    users_by_services = pd.DataFrame({'Newsfeed users only': feed_only_users, 'Messenger AND Newsfeed users': messaging_and_feed_users, 'Messenger user only': messaging_only_users}).reset_index()
    users_by_services['time'] = pd.to_datetime(users_by_services['time'])
    users_by_services['Date'] = users_by_services['time'].dt.strftime('%m-%d') 

    # plot diagrams -  users by services
    plt.subplots(2, 1, figsize=(12, 6))
    plt.suptitle('Users by services, week', fontsize=14)
    color_map = ['#2596be', '#CBD0AE', '#F78A2D']
    alpha = .7

    # num of users - venn diagram
    plt.subplot(1, 2, 2)
    labels = users_by_services.columns[1:4].values
    # venn2(subsets = (len(feed_only_users_id), len(messaging_only_users_id), len(messaging_and_feed_users_id)), 
    #      set_labels = (labels[0], labels[2]),
    #      set_colors=(color_map[0], color_map[2]),alpha=alpha)
    # plt.title('Venn diagram for users by services')
    # temp barplot instead venn
    lbl = ['Unique users last week']
    plt.title('Temporary instead venn diagram for users by services')
    plt.bar(lbl, len(feed_only_users_id), 1, label=labels[0], color=color_map[0], alpha=alpha)
    plt.bar(lbl, len(messaging_and_feed_users_id), 1, bottom=len(feed_only_users_id), label=labels[1], color=color_map[1], alpha=alpha)
    plt.bar(lbl, len(messaging_only_users_id), 1, bottom=len(feed_only_users_id)+len(messaging_and_feed_users_id), label=labels[2], color=color_map[2], alpha=alpha)
    plt.ylabel('Users')
    plt.legend(labels)

    # plot users by actions
    plt.subplot(1, 2, 1)
    plt.stackplot(users_by_services.Date, 
                users_by_services['Newsfeed users only'], 
                users_by_services['Messenger AND Newsfeed users'], 
                users_by_services['Messenger user only'], 
                colors=color_map,
                labels=labels,
                alpha=alpha)

    plt.legend(loc='lower left')
    plt.xlabel('Date')
    plt.ylabel('Users')
    plt.title('Users by services, daily')
    # plt.show() # comment this to export pic

    # send plots
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'both_services2.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    # get data for top_100_users_by_views
    query = '''SELECT user_id AS user_id,
           countIf(action='view') AS views,
           countIf(action='like') AS likes,
           countIf(action='like') / countIf(action='view') AS "CTR",
           count(DISTINCT post_id) AS "unique posts"
    FROM simulator_20220420.feed_actions
    GROUP BY user_id
    ORDER BY views DESC
    LIMIT 100;'''
    top_100_users_by_views = Getch(query).df

    # send top_100_users_by_views
    file_object = io.StringIO()
    top_100_users_by_views.to_csv(file_object)
    file_object.name = 'top_100_users_by_views.csv'
    file_object.seek(0)
    bot.sendDocument(chat_id=chat_id, document=file_object)

    # approve end of report
    bot.sendMessage(chat_id=chat_id, text='End of report')

try:
    test_report()
except Exception as e:
    print(e)
