# -*- coding: utf-8 -*-
# Copyright 2014 Jan-Philip Gehrcke. See LICENSE file for details.


"""
timegaps.timefilter -- generic time categorization logic as used by timegaps.
"""


from __future__ import unicode_literals
import datetime
import logging
from collections import defaultdict
from collections import OrderedDict
import .timediff

log = logging.getLogger("timefilter")


class TimeFilterError(Exception):
    pass


class TimeFilter(object):
    """Represents certain time filtering rules. Allows for filtering objects
    providing a `moddate` attribute.
    """
    # Define valid categories in order from past to future (old -> young).
    valid_categories = ("years", "months", "weeks", "days", "hours", "recent")

    def __init__(self, rules, reftime=None):
        # Define time categories (their labels) and their default filter
        # values. Must be in order from past to future (old -> young).
        time_categories = OrderedDict((c, 0) for c in self.valid_categories)

        # If the reference time is not provided by the user, use current time
        self.reftime = datetime.datetime.now() if reftime is None else reftime
        assert isinstance(self.reftime, datetime.datetime)

        # Give 'em a more descriptive name.
        userrules = rules
        # Validate given rules.
        assert isinstance(userrules, dict), "`rules` parameter must be dict."
        if not len(userrules):
            raise TimeFilterError("Rules dictionary must not be emtpy.")
        greaterzerofound = False
        # `items()` is Py2/3 portable, performance impact on Py2 negligible.
        for label, count in userrules.items():
            assert isinstance(count, int), "`rules` dict values must be int."
            if count > 0:
                greaterzerofound = True
            if count < 0:
                raise TimeFilterError(
                    "'%s' count must be positive integer." % label)
            if not label in time_categories:
                raise TimeFilterError(
                    "Invalid key in rules dictionary: '%s'" % label)
        if not greaterzerofound:
            raise TimeFilterError(
                "Invalid rules dictionary: at least one count > 0 required.")

        # Build up `self.rules` dict. Set rules not given by user to defaults,
        # keep order of `time_categories` dict (order is crucial).
        self.rules = OrderedDict()
        # `items()` is Py2/3 portable, performance impact on Py2 negligible.
        for label, defaultcount in time_categories.items():
            if label in userrules:
                self.rules[label] = userrules[label]
            else:
                self.rules[label] = defaultcount
        log.debug("TimeFilter set up with reftime %s and rules %s",
            self.reftime, self.rules)

    def filter(self, objs):
        """Split list of objects into two lists, `accepted` and `rejected`,
        according to the rules. A treatable object is required to have a
        `modtime` attribute, carrying a Unix timestamp.
        """
        # Upon categorization, items are put into category-timecount buckets,
        # for instance into the 2-year bucket (category: year, timecount: 2).
        # Each bucket may contain multiple items. Therefore, each category
        # (years, months, etc) is represented as a dictionary, whereas the
        # buckets are represented as lists. The timecount for a certain bucket
        # is used as a key for storing the list (value) in the dictionary.
        # For example, `self._years_dict[2]` stores the list representing the
        # 2-year bucket. These dictionaries and their key-value-pairs are
        # created on the fly.
        #
        # There is no timecount distinction in 'recent' category, therefore
        # only one list is used for storing recent items.

        for catlabel in list(self.rules.keys())[:-1]:
            setattr(self, "_%s_dict" % catlabel, defaultdict(list))
        self._recent_items = []

        # ensure we can iterate over objs twice even if it's an iterator
        objs = list(objs)

        # Categorize given objects.
        for obj in objs:
            # Might raise AttributeError if `obj` does not have `moddate`
            # attribute or other exceptions upon `_Timedelta` creation.
            try:
                td = _Timedelta(obj.moddate, self.reftime)
            except _TimedeltaError as e:
                raise TimeFilterError("Cannot categorize %s: %s" % (obj, e))
            # If timecount in youngest category after 'recent' is 0, then this
            # is a recent item.
            if td.hours == 0:
                if self.rules["recent"] > 0:
                    self._recent_items.append(obj)
                continue
            # Iterate through all categories from young to old, w/o 'recent'.
            # Sign. performance impact, don't go with self.rules.keys()[-2::-1]
            for catlabel in ("hours", "days", "weeks", "months", "years"):
                timecount = getattr(td, catlabel)
                if 0 < timecount <= self.rules[catlabel]:
                    # `obj` is X hours/days/weeks/months/years old with X >= 1.
                    # X is requested in current category, e.g. when 3 days are
                    # requested (`self.rules[catlabel]` == 3), and category is
                    # days and X is 2, then X <= 3, so put `obj` into
                    # self._days_dict` with timecount (2) key.
                    #log.debug("Put %s into %s/%s.", obj, catlabel, timecount)
                    getattr(self, "_%s_dict" % catlabel)[timecount].append(obj)

        accepted_objs = set()

        # Sort all category-timecount buckets internally and filter them:
        # Accept the newest element from each bucket.
        # The 'recent' items list needs special treatment. Sort, accept the
        # newest N elements.
        self._recent_items.sort(key=lambda f: f.moddate)
        for recent_item in self._recent_items[-self.rules["recent"]:]:
            accepted_objs.add(recent_item)
            # log.debug(
            #    "Accepted %s: %s/%s",
            #    recent_item, "recent", "?")
        # Iterate through all other categories except for 'recent'.
        # `catdict[timecount]` occurrences are lists with at least one item.
        # The newest item in each of these category-timecount buckets is to
        # be accepted.
        for catlabel in list(self.rules.keys())[:-1]:
            catdict = getattr(self, "_%s_dict" % catlabel)
            for timecount in catdict:
                catdict[timecount].sort(key=lambda f: f.moddate)
                # already_accepted = catdict[timecount][-1] in accepted_objs
                accepted_objs.add(catdict[timecount][-1])
                # log.debug(
                #    "Accepted %s: %s/%s.%s",
                #    catdict[timecount][-1], catlabel, timecount,
                #    "(already accepted)" if already_accepted else "")

        # calculate the difference of objs and accepted_objs using list
        # comprehension instead of set.difference() -- it's deterministic (it
        # keeps the original order) while not necessarily slower:
        # https://gist.github.com/morenopc/10651856.
        rejected_objs = [obj for obj in objs if obj not in accepted_objs]
        return sorted(accepted_objs, key=lambda f: f.moddate), rejected_objs


class _TimedeltaError(TimeFilterError):
    pass


class _Timedelta(object):
    """Represent how many calendar years, months, weeks, days, hours time `t`
    (datetime.datetime) is earlier than reference time `ref`. Differences are
    based on calendar units, for example April 30 would be 1 month earlier than
    May 1, or 7:59 would be 1 hour earlier than 8:00. Differences are considered
    independently (e.g. 1 week usually isn't the same as 7 days) but may overlap
    (e.g. days may be 730, which will often mean that years == 2)
    """
    def __init__(self, t, ref):
        if t > ref:
            raise _TimedeltaError(("Modification time %s not " 
                "earlier than reference time %s.") % (t, ref))
        self.hours = timediff.hours(t, ref)
        self.days = timediff.days(t, ref)
        self.weeks = timediff.weeks(t, ref)
        self.months = timediff.months(t, ref)
        self.years = timediff.years(t, ref)
