# -*- coding: utf-8 -*-
# Copyright 2014 Jan-Philip Gehrcke. See LICENSE file for details.

import os
import sys
import time
from datetime import datetime, timedelta
from itertools import chain
from random import randint, shuffle
import cProfile, pstats, StringIO

sys.path.insert(0, os.path.abspath('..'))
from timegaps.timegaps import FileSystemEntry
from timegaps.timefilter import TimeFilter


import logging
logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f [%(process)-5d]%(funcName)s# %(message)s',
    datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


def main():
    t0 = time.time()
    now = datetime.now()
    fses = list(fsegen(ref=now, N_per_cat=5*10**4, max_timecount=9))
    shuffle(fses)
    nbr_fses = len(fses)
    n = 8
    rules = {
        "years": n,
        "months": n,
        "weeks": n,
        "days": n,
        "hours": n,
        "recent": n
        }
    sduration = time.time() - t0
    log.info("Setup duration: %.3f s", sduration)
    log.info("Profiling...")
    pr = cProfile.Profile()
    pr.enable()
    a, r = TimeFilter(rules, now).filter(fses)
    pr.disable()
    s = StringIO.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('time')
    ps.print_stats(20)
    print s.getvalue()


class FileSystemEntryMock(FileSystemEntry):
    def __init__(self, moddate):
        self.moddate = moddate

    def __str__(self):
        return "%s(moddate: %s)" % (self.__class__.__name__, self.moddate)

    def __repr__(self):
        return "%s(moddate=%s)" % (self.__class__.__name__, self.moddate)


def nrandint(n, min, max):
    for _ in xrange(n):
        yield randint(min, max)


def fsegen(ref, N_per_cat, max_timecount):

    def td(seconds): return timedelta(seconds=seconds)
    
    N = N_per_cat
    c = max_timecount
    nowminusXyears =   (ref-td(60*60*24*365*i) for i in nrandint(N, 1, c))
    nowminusXmonths =  (ref-td(60*60*24*30 *i) for i in nrandint(N, 1, c))
    nowminusXweeks =   (ref-td(60*60*24*7  *i) for i in nrandint(N, 1, c))
    nowminusXdays =    (ref-td(60*60*24    *i) for i in nrandint(N, 1, c))
    nowminusXhours =   (ref-td(60*60       *i) for i in nrandint(N, 1, c))
    nowminusXseconds = (ref-td(1           *i) for i in nrandint(N, 1, c))
    dates = chain(
        nowminusXyears,
        nowminusXmonths,
        nowminusXweeks,
        nowminusXdays,
        nowminusXhours,
        nowminusXseconds,
        )
    return (FileSystemEntryMock(moddate=d) for d in dates)


if __name__ == "__main__":
    main()
