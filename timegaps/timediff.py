# -*- coding: utf-8 -*-


"""
timegaps.timediff -- determine time differences in different granularities
"""

import datetime

def seconds(t1, t2):
    return (t2 - t1).total_seconds()

def hours(t1, t2):
    t1 = datetime.datetime.combine(t1.date(), datetime.time(hour=t1.hour))
    t2 = datetime.datetime.combine(t2.date(), datetime.time(hour=t2.hour))
    return int((t2 - t1).total_seconds() // 3600)

def days(t1, t2):
    t1 = t1.date()
    t2 = t2.date()
    return (t2 - t1).days

def weeks(t1, t2):
    t1 = t1.date() - datetime.timedelta(days=t1.weekday())
    t2 = t2.date() - datetime.timedelta(days=t2.weekday())
    return int((t2 - t1).days // 7)

def months(t1, t2):
    return (t2.year - t1.year) * 12 + (t2.month - t1.month)

def years(t1, t2):
    return t2.year - t1.year

