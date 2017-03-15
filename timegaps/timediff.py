# -*- coding: utf-8 -*-


"""
timegaps.timediff -- determine time differences in different granularities
"""

import datetime

def seconds(t1, t2):
    try:
        return (t2 - t1).total_seconds()
    except AttributeError:
        return t2 - t1

def hours(t1, t2):
    return int(seconds(t1, t2) / 3600)

def days(t1, t2):
    return int(seconds(t1, t2) / 86400)

def weeks(t1, t2):
    return int(seconds(t1, t2) / 604800)

def months(t1, t2):
    return int(seconds(t1, t2) / 2592000)

def years(t1, t2):
    return int(seconds(t1, t2) / 31536000)
