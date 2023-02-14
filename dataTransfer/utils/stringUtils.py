#!/usr/bin/env python
#-*-coding:utf-8
# author ninesun

import datetime
import time


def str2date(str,date_format="%Y-%m-%d %H:%M:%S"):
    date = datetime.datetime.strptime(str, date_format)
    return date


def date2str(date,date_formate = "%Y-%m-%d %H:%M:%S"):
    str = date.strftime(date_formate)
    return str

"""  by hour """
def date_delta(date,gap,formate = "%Y-%m-%d %H:%M:%S"):
    date = str2date(date)
    pre_date = date + datetime.timedelta(hours=gap)
    # pre_str = date2str(pre_date,formate)  # date形式转化为str
    return pre_date

"""  by minutes """
def date_delta_minutes(date,gap,formate = "%Y-%m-%d %H:%M:%S"):
    date = str2date(date)
    pre_date = date + datetime.timedelta(minutes=gap)
    # pre_str = date2str(pre_date,formate)  # date形式转化为str
    return pre_date

"""  by seconds """
def date_delta_seconds(date,gap,formate = "%Y-%m-%d %H:%M:%S"):
    date = str2date(date)
    pre_date = date + datetime.timedelta(seconds=gap)
    # pre_str = date2str(pre_date,formate)  # date形式转化为str
    return pre_date


"""  by weeks """
def date_delta_weeks(date,gap,formate = "%Y-%m-%d %H:%M:%S"):
    date = str2date(date)
    pre_date = date - datetime.timedelta(weeks=gap)
    # pre_str = date2str(pre_date,formate)  # date形式转化为str
    return pre_date


def str2timestamp(str,timestamp_len=10):
    date_array = time.strptime(str,"%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(date_array))
    if timestamp_len==13:
        timestamp *=1000
    return timestamp


"""  """
def str2FormatStr(str):
    startF = str(str).replace('-', '').replace(' ', '').replace(':', '')
    return startF

""" before by hour """
def date_delta_before(date,gap,formate = "%Y-%m-%d %H:%M:%S"):
    date = str2date(date)
    pre_date = date - datetime.timedelta(hours=gap)
    # pre_str = date2str(pre_date,formate)  # date形式转化为str
    return pre_date