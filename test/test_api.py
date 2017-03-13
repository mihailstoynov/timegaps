# -*- coding: utf-8 -*-
# Copyright 2014 Jan-Philip Gehrcke. See LICENSE file for details.


"""
Test timegaps.timegaps and timegaps.timefilter API. Most importantly, this
module tests the time categorization logic.
"""


from __future__ import unicode_literals
import os
import sys
import time
from base64 import b64encode
from datetime import date, datetime, timedelta
from itertools import chain, islice, izip, repeat
from random import randint, shuffle
import collections
import tempfile


# Make the same code base run with Python 2 and 3.
if sys.version < '3':
    range = xrange
else:
    pass


# py.test runs tests in order of definition. This is useful for running simple,
# fundamental tests first and more complex tests later.
from py.test import raises, mark


sys.path.insert(0, os.path.abspath('..'))
from timegaps.timegaps import FileSystemEntry, TimegapsError, FilterItem
from timegaps.timefilter import TimeFilter, _Timedelta, TimeFilterError
import timegaps.timediff as timediff

import logging
logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f %(funcName)s# %(message)s',
    datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


WINDOWS = sys.platform == "win32"
SHORTTIME = 0.01


def nrndint(n, imin, imax):
    for _ in range(n):
        yield randint(imin, imax)


def randstring_fssafe():
    return b64encode(os.urandom(6)).replace(b'/', b'!')


def make_moddates(*args):

    def flatten(iterable):
        for el in iterable:
            if isinstance(el, collections.Iterable):
                for sub in flatten(el):
                    yield sub
            else:
                yield el

    num_iterables = sum((isinstance(arg, collections.Iterable)
                         for arg in args))

    if num_iterables == 0:
        raise TypeError("One of the date parts must be a sequence")

    if num_iterables > 1:
        raise TypeError("cannot handle multiple lists of date parts")


    part_iters = (arg if isinstance(arg, collections.Iterable) else repeat(arg)
                  for arg in args)

    return [datetime(*flatten(parts)) for parts in izip(*part_iters)]


def make_fses(*args):
    return [FilterItem(moddate=date) for date in make_moddates(*args)]

def make_dense_fses(ref):
    """Generate a lot of FSEs that cover the timespan of all rules being
    tested. Applying any rule on it should find an item for each
    bucket, so that you can check a rule by just counting the number
    of items found.
    """

    def add_fses(fses, ref, count, delta):
        for _ in range(count):
            ref = ref - delta;
            fses.append(FilterItem(moddate=ref))
        return ref

    fses = []
    # 'recent' items every 30 sec for 70 minutes
    ref = add_fses(fses, ref, 140, timedelta(seconds=30))
    # items every 12 minutes for 3 days
    ref = add_fses(fses, ref, 360, timedelta(minutes=12))
    # items every 1.5 hours for two weeks
    ref = add_fses(fses, ref, 224, timedelta(minutes=90))
    # items every 6 hours for two months
    ref = add_fses(fses, ref, 240, timedelta(hours=6))
    # items every day for 9 months
    ref = add_fses(fses, ref, 270, timedelta(days=1))
    # items every week for ~4 years
    ref = add_fses(fses, ref, 208, timedelta(weeks=1))
    # items every month for ~20 years
    ref = add_fses(fses, ref, 240, timedelta(days=30))

    shuffle(fses)

    return fses


class TestMakeModdates(object):
    """Self-test for the make_moddates helper function"""

    def test_make_moddates_list(self):
        dt = datetime
        assert make_moddates(2016, 1, range(1, 5)) == [dt(2016, 1, 1),
                                                       dt(2016, 1, 2),
                                                       dt(2016, 1, 3),
                                                       dt(2016, 1, 4)]

    def test_make_moddates_list_of_tuples(self):
        dt = datetime
        assert make_moddates(2016, [(1, 12), (2, 13)]) == [dt(2016, 1, 12),
                                                           dt(2016, 2, 13)]

    def test_make_moddates_no_single_values(self):
        with raises(TypeError):
            make_moddates(2016, 1, 1)

    def test_make_moddates_only_one_list(self):
        with raises(TypeError):
            make_moddates(2016, [1, 2], [1, 2, 3, 4])



class TestBasicFSEntry(object):
    """Test basic FileSystemEntry logic.

    Upon creation, an FSE extracts information about the given path with a
    stat() system call via Python's `stat` module. From this data, the FSE
    populates itself with various convenient attributes.

    Flow for each test_method:
        o = TestClass()
        o.setup()
        try:
            o.test_method()
        finally:
            o.teardown()
    """
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_invalid_path(self):
        with raises(OSError):
            FileSystemEntry(path="gibtsgarantiertnichthier")

    def test_dir(self):
        fse = FileSystemEntry(path=".")
        assert fse.type == "dir"
        assert isinstance(fse.moddate, datetime)

    def test_file(self):
        with tempfile.NamedTemporaryFile() as t:
            fse = FileSystemEntry(path=t.name)
            assert fse.type == "file"
            assert isinstance(fse.moddate, datetime)

    def test_custom_moddate(self):
        with tempfile.NamedTemporaryFile() as t:
            fse = FileSystemEntry(path=t.name, moddate=datetime(1977, 7, 7))
            assert fse.type == "file"
            assert isinstance(fse.moddate, datetime)

    def test_custom_moddate_wrongtype(self):
        with tempfile.NamedTemporaryFile() as t:
            with raises(TimegapsError):
                FileSystemEntry(path=t.name, moddate=date(1977, 7, 7))
            with raises(TimegapsError):
                FileSystemEntry(path=t.name, moddate="foo")

    @mark.skipif("WINDOWS")
    def test_symlink(self):
        linkname = "/tmp/%s" % randstring_fssafe()
        try:
            os.symlink("target", linkname)
            fse = FileSystemEntry(path=linkname)
        finally:
            os.unlink(linkname)
        assert fse.type == "symlink"
        assert isinstance(fse.moddate, datetime)


class TestTimeFilterInit(object):
    """Test TimeFilter initialization logic.
    """
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_reftime(self):
        t = datetime.now()
        f = TimeFilter(rules={"days": 1}, reftime=t)
        assert f.reftime == t
        time.sleep(SHORTTIME)
        f = TimeFilter(rules={"days": 1})
        assert f.reftime > t

    def test_invalid_rule_key(self):
        with raises(TimeFilterError):
            TimeFilter(rules={"days": 1, "wrong": 1})

    def test_invalid_rule_value(self):
        with raises(AssertionError):
            TimeFilter(rules={"days": None})

    def test_all_counts_zero(self):
        with raises(TimeFilterError):
            TimeFilter(rules={"days": 0})

    def test_one_count_negative(self):
        with raises(TimeFilterError):
            TimeFilter(rules={"days": -1})

    def test_emtpy_rules_dict(self):
        with raises(TimeFilterError):
            TimeFilter(rules={})

    def test_wrong_rules_type(self):
        with raises(AssertionError):
            TimeFilter(rules=None)

    def test_fillup_rules_default_rules(self):
        f = TimeFilter(rules={"days": 20})
        assert f.rules["days"] == 20
        for c in ("years", "months", "weeks", "hours", "recent"):
            assert f.rules[c] == 0


class TestTimeFilterFilterSig(object):
    """Test TimeFilter.filter method call signature.
    """
    def test_invalid_object(self):
        f = TimeFilter(rules={"days": 1})
        with raises(AttributeError):
            # AttributeError: 'NoneType' object has no attribute 'modtime'
            f.filter([None])

    def test_not_iterable(self):
        f = TimeFilter(rules={"days": 1})
        with raises(TypeError):
            # TypeError: 'NoneType' object is not iterable
            f.filter(None)


class TestTimedelta(object):
    """Test Timedelta logic and arithmetic.
    """
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_wrongtypes(self):
        with raises(Exception):
            _Timedelta(t=None, ref="a")
        with raises(Exception):
            _Timedelta(t=0.0, ref=100.0)

    def test_future(self):
        # Time `t` later than reference.
        with raises(TimeFilterError):
            _Timedelta(t=datetime.now(),
                       ref=datetime.now() - timedelta(hours=10))

    def test_types_math_year(self):
        dt=datetime
        # 1 year is exactly 365 * 24 hours, so calendar dates may
        # differ if a leap year is involved.
        d = _Timedelta(t=dt(2015, 3, 1), ref=dt(2016, 2, 29))
        assert d.years == 1
        assert isinstance(d.years, int)
        assert d.months == 12
        assert isinstance(d.months, int)
        assert d.weeks == 52
        assert isinstance(d.weeks, int)
        assert d.days == 365
        assert isinstance(d.days, int)
        assert d.hours == 365 * 24
        assert isinstance(d.hours, int)

    def test_types_math_hour(self):
        dt=datetime
        d = _Timedelta(t=dt(1915, 2, 24, 9, 35), ref=dt(1915, 2, 24, 10, 35))
        assert d.years == 0
        assert isinstance(d.years, int)
        assert d.months == 0
        assert isinstance(d.months, int)
        assert d.weeks == 0
        assert isinstance(d.weeks, int)
        assert d.days == 0
        assert isinstance(d.days, int)
        assert d.hours == 1
        assert isinstance(d.hours, int)


class TestTimeFilterBasic(object):
    """Test TimeFilter logic and arithmetics with small, well-defined mock
    object lists.
    """

    reftime = datetime(2016, 12, 31, 12, 35)
    # lists of mock items, per category, partly overlapping (e.g. days/weeks,
    # weeks/months)
    fses10 = {
        "recent": [FilterItem(moddate=reftime - timedelta(minutes=n))
                   for n in range(1, 11)],
        "hours": make_fses(2016, 12, 31, range(2, 12), 0),
        "days": make_fses(2016, 12, range(21, 31)),
        "weeks": make_fses(2016,
                           [(12, 24), (12, 17), (12, 10), (12, 3), (11, 26),
                            (11, 19), (11, 12), (11, 5), (10, 29), (10, 22)]),
        "months": make_fses(2016, range(2, 12), 28),
        "years": make_fses(range(2006, 2016), 12, 31)
    }
    # lists of mock items per category, not overlapping. Useful for testing
    # that items don't "spill" into other categories (e.g. a "days" rule
    # shouldn't find any items from "hours" or "weeks")
    fses4 = {
        "recent": [FilterItem(moddate=reftime - timedelta(minutes=n))
                   for n in range(1, 5)],
        "hours": make_fses(2016, 12, 31, range(8, 12), 0),
        "days": make_fses(2016, 12, range(27, 31)),
        "weeks": make_fses(2016, [(12, 24), (12, 17), (12, 10), (12, 3)]),
        "months": make_fses(2016, range(8, 12), 20),
        "years": make_fses(range(2012, 2016), 12, 31)
    }

    yearsago = make_fses(range(2016, 2005, -1), 12, 31)

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_minimal_functionality_and_types(self):
        # Create filter with reftime self.reftime
        f = TimeFilter(rules={"hours": 1}, reftime=self.reftime)
        # Create mock that is 1.5 hours old. Must end up in accepted list,
        # since it's 1 hour old and one item should be kept from the 1-hour-
        # old-category
        fse = FilterItem(moddate=self.reftime-timedelta(hours=1.5))
        a, r = f.filter([fse])
        # http://stackoverflow.com/a/1952655/145400
        assert isinstance(a, collections.Iterable)
        assert isinstance(r, collections.Iterable)
        assert a[0] == fse
        assert len(r) == 0

    def test_requesting_one_retrieves_most_recent(self):
        for category, fses in self.fses10.iteritems():
            f = TimeFilter({category: 1}, self.reftime)
            a, r = f.filter(fses)
            assert len(a) == 1
            def moddates(fses): return map(lambda fse: fse.moddate, fses)
            assert a[0].moddate == max(moddates(fses))
            assert len(r) == 9
            assert a[0] not in r

    def test_requesting_less_than_available_retrieves_most_recent(self):
        for category, fses in self.fses10.iteritems():
            f = TimeFilter({category: 5}, self.reftime)
            a, r = f.filter(fses)
            assert len(a) == 5
            assert len(r) == 5
            def moddates(fses): return map(lambda fse: fse.moddate, fses)
            assert min(moddates(a)) > max(moddates(r))

    def test_requesting_all_available_retrieves_all(self):
        for category, fses in self.fses10.iteritems():
            f = TimeFilter({category: 10}, self.reftime)
            a, r = f.filter(fses)
            assert set(a) == set(fses)
            assert len(r) == 0

    def test_requesting_more_than_available_retrieves_all(self):
        for category, fses in self.fses10.iteritems():
            f = TimeFilter({category: 15}, self.reftime)
            a, r = f.filter(fses)
            assert set(a) == set(fses)
            assert len(r) == 0

    def test_requesting_newer_than_available_retrieves_none(self):
        # excluding the "recent" category which will always accept the newest N
        # items.
        categories = ("hours", "days", "weeks", "months", "years")
        # generate items 6-10 per category, in reverse order to increase
        # the chance of discovering order dependencies in the filter.
        fses10to6 = {cat : sorted(self.fses10[cat],
                                  key=lambda x: x.moddate,
                                  reverse=True)[5:]
                     for cat in categories}

        # now ask for the first 5 items of each category.
        for category, fses in fses10to6.iteritems():
            f = TimeFilter({category: 5}, self.reftime)
            a, r = f.filter(fses)
            assert len(a) == 0
            assert set(r) == set(fses)

    def test_request_less_than_available_distant(self):
        # only distant items are present (at the beginning of the rule period)
        fses = [self.yearsago[10], self.yearsago[9]]
        rules = {"years": 10}
        a, r = TimeFilter(rules, self.reftime).filter(fses)
        assert set(a) == set(fses)
        assert len(r) == 0

    def test_request_less_than_available_close(self):
        # only close items are present (at the end of the rule period)
        fses = [self.yearsago[1], self.yearsago[2]]
        rules = {"years": 10}
        a, r = TimeFilter(rules, self.reftime).filter(fses)
        assert set(a) == set(fses)
        assert len(r) == 0

    def test_all_categories_1acc_1rej(self):
        # test multiple categories -- for simplicity make sure that periods
        # don't overlap
        now = datetime(2015, 12, 31, 12, 30, 45)
        nowminus1year = datetime(2014, 12, 31)
        nowminus2year = datetime(2013, 12, 31)
        nowminus1month = datetime(2015, 11, 30)
        nowminus2month = datetime(2015, 10, 31)
        nowminus1week = datetime(2015, 12, 24)
        nowminus2week = datetime(2015, 12, 17)
        nowminus1day = datetime(2015, 12, 30)
        nowminus2day = datetime(2015, 12, 29)
        nowminus1hour = datetime(2015, 12, 31, 11, 30)
        nowminus2hour = datetime(2015, 12, 31, 10, 30)
        nowminus1second = datetime(2015, 12, 31, 12, 30, 44)
        nowminus2second = datetime(2015, 12, 31, 12, 30, 43)
        adates = (
            nowminus1year,
            nowminus1month,
            nowminus1week,
            nowminus1day,
            nowminus1hour,
            nowminus1second,
            )
        rdates = (
            nowminus2year,
            nowminus2month,
            nowminus2week,
            nowminus2day,
            nowminus2hour,
            nowminus2second,
            )
        afses = [FilterItem(moddate=t) for t in adates]
        rfses = [FilterItem(moddate=t) for t in rdates]
        cats = ("days", "years", "months", "weeks", "hours", "recent")
        rules = {c:1 for c in cats}
        a, r = TimeFilter(rules, now).filter(chain(afses, rfses))
        # All nowminus1* must be accepted, all nowminus2* must be rejected.
        assert set(a) == set(afses)
        assert set(r) == set(rfses)

    def test_requesting_older_categories_than_available_retrieves_none(self):
        categories = ("recent", "hours", "days", "weeks", "months", "years")
        fses = [self.fses4[cat] for cat in categories]

        for n in range(1, len(categories)):
            newer_fses = list(chain.from_iterable(islice(fses, n)))

            rules = {categories[n]: 4}
            a, r = TimeFilter(rules, self.reftime).filter(newer_fses)
            assert len(a) == 0
            assert set(r) == set(newer_fses)

    def test_requesting_newer_categories_than_available_retrieves_none(self):
        categories = ("years", "months", "weeks", "days", "hours", "recent")
        fses = [self.fses4[cat] for cat in categories]

        for n in range(1, len(categories)):
            older_fses = list(chain.from_iterable(islice(fses, n)))

            rules = {categories[n]: 4}
            a, r = TimeFilter(rules, self.reftime).filter(older_fses)
            assert len(a) == 0
            assert set(r) == set(older_fses)


class TestTimeFilterOverlappingRules(object):
    """Test and document behavior of overlapping rules.
    """

    def test_overlapping_rules_dont_accept_additional_items(self):
        # check first rule: 24 hours, overlapping one day
        rules = { "hours": 24 }
        ref_time = datetime(2016, 1, 1)
        moddates = (ref_time - timedelta(hours=i)
                    for i in range(1, 29))
        items = [FilterItem(moddate=d) for d in moddates]
        a, _ = TimeFilter(rules, ref_time).filter(items)
        # expect the first 24 items to be accepted
        assert len(a) == 24
        assert set(a) == set(items[:24])

        # combine with an overlapping "days1" rule
        rules = { "hours":  24, "days": 1 }
        a, _ = TimeFilter(rules, ref_time).filter(items)
        # the result shouldn't change: the most recent 1-day old item is
        # the same as the most recent 24-hour old item
        assert len(a) == 24
        assert set(a) == set(items[:24])

    def test_10_days_2_weeks(self):
        # Further define category 'overlap' behavior. {"days": 10, "weeks": 2}
        # -> week 0 is included in the 10 days, week 1 is only partially
        # included in the 10 days, and week 2 (14 days and older) is not
        # included in the 10 days.
        # Having 15 FSEs, 1 to 15 days in age, the first 10 of them must be
        # accepted according to the 10-day-rule. According to the 2-weeks-rule,
        # the 7th and 14th FSEs must be accepted. The 7th FSE is included in
        # the first 10, so items 1-10 and 14 are the accepted ones.
        now = datetime(2016, 1, 3)
        nowminusXdays = (now - timedelta(days=i)
                         for i in range(1, 16))
        fses = [FilterItem(moddate=d) for d in nowminusXdays]
        rules = {"days": 10, "weeks": 2}
        a, r = TimeFilter(rules, now).filter(fses)
        r = list(r)
        assert len(a) == 11
        # Check if first 11 fses are in accepted list (order can be predicted
        # according to current implementation, but should not be tested, as it
        # is not guaranteed according to the current specification).
        for fse in fses[:10]:
            assert fse in a
        # Check if 14th FSE is accepted.
        assert fses[13] in a
        # Check if FSEs 12, 13, 15 are rejected.
        assert len(r) == 4
        for i in (10, 11, 12, 14):
            assert fses[i] in r


class TestTimeFilterMass(object):
    """Test TimeFilter logic and arithmetics with largish mock object lists.
    """
    now = datetime(2016, 12, 31, 23, 59, 59)
    fses = make_dense_fses(ref=now)

    def setup(self):
        pass

    def teardown(self):
        pass

    def test_singlecat_rules(self):
        n = 8
        ryears = {"years": n}
        rmonths = {"months": n}
        rweeks = {"weeks": n}
        rdays = {"days": n}
        rhours = {"hours": n}
        rrecent = {"recent": n}
        # Run single-category filter on these fses.
        for rules in (ryears, rmonths, rweeks, rdays, rhours, rrecent):
            a, r = TimeFilter(rules, self.now).filter(self.fses)
            assert len(a) == n
            assert len(list(r)) == len(self.fses) - n

    def test_multicat_rules_yield_union_of_singlecat_rules(self):
        N = 10
        MAXCOUNT = 16

        def rndcount(): return randint(1, MAXCOUNT)

        for _ in range(N):
            rules = {
                "years": rndcount(),
                "months": rndcount(),
                "weeks": rndcount(),
                "days": rndcount(),
                "hours": rndcount(),
                "recent": rndcount()
            }

            single_results = set()
            for category, timecount in rules.iteritems():
                single_rule = dict.fromkeys(rules, 0)
                single_rule[category] = timecount
                a, _ = TimeFilter(single_rule, self.now).filter(self.fses)
                assert len(a) == timecount
                single_results.update(a)

            multi_result, _ = TimeFilter(rules, self.now).filter(self.fses)
            assert len(multi_result) == len(single_results)
            assert set(multi_result) == single_results

    def test_1_day(self):
        rules = {"days": 1}
        a, _ = TimeFilter(rules, self.now).filter(self.fses)
        assert len(a) == 1

    def test_1_recent_1_years(self):
        rules = {
            "years": 1,
            "recent": 1
            }
        a, _ = TimeFilter(rules, self.now).filter(self.fses)
        assert len(a) == 2

    def test_realistic_scheme(self):
        rules = {
            "years": 4,
            "months": 12,
            "weeks": 6,
            "days": 10,
            "hours": 48,
            "recent": 5
            }
        a, _ = TimeFilter(rules, self.now).filter(self.fses)
        # 4+12+6+10+48+5 = 85; there is 1 reducing overlap between days and weeks
        # and two more between hours and days -> 82 accepted items are expected.
        assert len(a) == 82
