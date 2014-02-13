# -*- coding: utf-8 -*-
# Copyright 2014 Jan-Philip Gehrcke. See LICENSE file for details.

from __future__ import unicode_literals
import os
import sys
import logging
from py.test import raises, mark
from clitest import CmdlineInterfaceTest, CmdlineTestError, WrongExitCode


RUNDIRTOP = "./cmdline-test"
TIMEGAPS_NAME = "../../../timegaps.py"
PYTHON_EXE = "python"
WINDOWS = sys.platform == "win32"


class CmdlineInterfaceTestUnix(CmdlineInterfaceTest):
    rundirtop = RUNDIRTOP


class CmdlineInterfaceTestWindows(CmdlineInterfaceTest):
    shellpath = "cmd.exe"
    rundirtop = RUNDIRTOP
    shellscript_ext = ".bat"


CLITest = CmdlineInterfaceTestUnix
if WINDOWS:
    CLITest = CmdlineInterfaceTestWindows


logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f %(funcName)s# %(message)s',
    datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


class Base(object):
    """Implement methods shared by all test classes.
    """

    def setup_method(self, method):
        testname = "%s_%s" % (type(self).__name__, method.__name__)
        print("\n\n%s" % testname)
        self.cmdlinetest = CLITest(testname)

    def teardown_method(self, method):
        self.cmdlinetest.clear()

    def run(self, arguments_unicode, rc=0):
        cmd = "%s %s %s" % (PYTHON_EXE, TIMEGAPS_NAME, arguments_unicode)
        log.info("Test command:\n%s" % cmd)
        self.cmdlinetest.run(cmd_unicode=cmd, expect_rc=rc)
        return self.cmdlinetest


class TestBasic(Base):
    """Test basic functionality.
    """

    def test_too_few_args(self):
        # argparse ArgumentParser.error() makes program exit with code 2.
        t = self.run("", rc=2)
        t.in_stderr("too few arguments")

    def test_valid_rules_missing_item_cmdline(self):
        # TODO: also test missing item / valid rules for stdin mode.
        t = self.run("days5", rc=1)
        t.in_stderr("one item must be provided (if --stdin not set")

    def test_invalid_rulesstring_missing_item(self):
        # Rules are checked first, error must indicate invalid rules.
        t = self.run("bar", rc=1)
        t.in_stderr(["Invalid", "token", "bar"])

    def test_empty_rulesstring(self):
        # Rules are checked first, error must indicate invalid rules.
        t = self.run('""', rc=1)
        t.in_stderr("Token is empty")

    def test_invalid_rulesstring_category(self):
        # Rules are checked first, error must indicate invalid rules.
        t = self.run('peter5', rc=1)
        t.in_stderr(["Time category", "invalid"])

    def test_invalid_rulesstring_wrong_item(self):
        # Rules are checked first, error must indicate invalid rules.
        t = self.run("foo nofile", rc=1)
        t.in_stderr(["Invalid", "token", "foo"])

    def test_invalid_itempath_1(self):
        t = self.run("days5 nofile", rc=1)
        t.in_stderr(["nofile", "Cannot access"])

    def test_invalid_itempath_2(self):
        t = self.run("days5 . nofile", rc=1)
        t.in_stderr(["nofile", "Cannot access"])


