#!/usr/bin/env python
#!-*- coding:utf-8 -*-
"""
最简单的例子。

tasks.py有两个任务，myroot是root节点，ct1是myroot的子节点。先执行myroot任务，后执行ct1任务。
"""
import os
import sys

base_dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath("%s/../" % base_dir))

from pigdag.tasks import tasks
from pigdag.core import *

tk = TasksKeeper(tasks)
tk.check_all()

exr = ExecuterRunner(tk)
exr.start()
