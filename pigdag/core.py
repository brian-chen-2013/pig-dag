#!/usr/bin/env python
#!-*- coding:utf-8 -*-

import os
import time
import subprocess
from Queue import Queue
from threading import Thread

from tasks import tasks, PIG_FILE_DIR
from utils import *

class TasksKeeper:
    """
    保存Tasks，并做各种处理。
    """
    def __init__(self, tasks):
        self.__tasks = tasks

        self.__tasks_dict = {}
        for t in self.__tasks:
            self.__tasks_dict[t['name']] = t

        #记录一个节点的parent节点，可能有多个，也可能没有
        self.__parent_tasks_dict = {}
        for t in self.__tasks:
            self.__parent_tasks_dict[t['name']] = []
            for c in t['parents']:
                if not None is c or len(c) > 0:
                    self.__parent_tasks_dict[t['name']].append(self.get_node(c))

        #把pig文件的路径设置成绝对路径
        self._covert_pig_path()

        #保存所有任务的名字
        self.__all_task_names = [t['name'] for t in self.__tasks]

        #根节点
        self.__roots = [t for t in self.__tasks 
                        if None in t['parents'] or 
                           [] == t['parents'] or 
                           None == t['parents'] ]

    def __repr__(self):
        return "TaskChecker: tasrks = %s" % self.__tasks

    def _covert_pig_path(self):
        """
        task存储的是相对路径，将它转化成绝对路径
        """
        for t in self.__tasks:
            tmp = t['pigfile'][0]
            t['pigfile'] = [os.path.join(PIG_FILE_DIR, tmp)]
        
    def get_all_task_names(self):
        return self.__all_task_names

    def get_roots(self):
        return self.__roots

    def get_node(self, node_name):
        if self.__tasks_dict.has_key(node_name):
            return self.__tasks_dict[node_name]
        return None

    def get_parent_node(self, node_name):
        if self.__parent_tasks_dict.has_key(node_name):
            return self.__parent_tasks_dict[node_name]
        return []

    def _check_cyclic(self):
        print "\n\n\ncheck cyclic in DAG..."
        #three colors algorithm, check cyclic of DAG
        """
        DAG不能有环，否则运行会进入死循环。三色法检查DAG的环结构。
        算法如下：
          初始化时，所有节点得颜色都设置成white。从root节点开始，逐级递归检查子节点。
          检查当前节点时，当前节点应该是白色，然后设置成灰色，如果不是白色，就是有环，抛出异常。
          然后递归检查当前节点的所有子节点。
          检查完当前节点时，当前节点应该是灰色，然后设置成黑色，如果不是灰色，抛出异常。
        """
        def recursion_check(t, colors):
            if colors[t['name']] == 'white':
                colors[t['name']] = 'gray'                
            else:
                raise Exception, "%s node cause cyclic" % t['name']

            childs = [self.get_node(name) for name in t['childs']]
            for child in childs:
                if child:
                    recursion_check(child, colors)

            if colors[t['name']] == 'gray':
                colors[t['name']] = 'black'
            else:
                raise Exception, "%s node cause cyclic" % t['name']

        for t in self.get_roots():
            colors = {}
            for tt in self.__tasks:
                colors[tt['name']] = 'white'
            recursion_check(t, colors)
        #暂时尚未解决的情况是，多个root的DAG共存，root A的某个子节点的子节点，是root B的子节点。
        print "\n\n\ncheck cyclic in DAG ok!"

    def _check_hdfs(self):
        print "\n\ncheck hdfs dirctories..."
        for i in self.get_all_task_names():
            print i
            if is_file_dir_exists(i):
                raise Exception, "Directory %s exists in HDFS, Pig-DAG can not run!" %(i)
        print "\n\n\ncheck hdfs dirctories ok"

    def _check_pigs(self):
        print "\n\ncheck pig files..."
        for i in self.__tasks:
            f = i['pigfile'][0]
            print f
            if not os.path.exists(f):
                raise Exception, "task %s pig file %s doesn't exist! " %(i['name'], f)
        print "\n\n\ncheck pig files ok"

    def check_all(self):
        self._check_cyclic()
        self._check_pigs()
        self._check_hdfs()


class MsgExc:
    def __init__(self, task_name, message):
        self.task_name = task_name
        self.message = message


class Executer(Thread):
    """
    线程类。每个Executer对应一个task。
    执行start()后，定时检测是否可以执行条件是否达成，如果已经达成，就执行task。
    一旦开始run，每隔几秒，检查是否满足执行前提。一旦满足，开始执行，如果出错，将错误信息传到queue，退出。
    如果成功执行，将成功信息传给queue，也退出。
    """
    def __init__(self, task, tk, msgq):
        super(self.__class__, self).__init__()
        self.__task = task
        self.__tk = tk
        self.__msgq = msgq

    def _can_run(self):
        """
        检查任务的执行条件是否具备，也就是说，它的parent是否都执行了。
        """
        parent = self.__tk.get_parent_node(self.__task['name'])
        #如果parent为空，是root节点，立刻执行
        if parent == []:
            #root node
            return True, ''
        #逐个检查parent节点是否满足执行条件
        res = True, ''
        for node in parent:
            s = node['name']
            #检查parent节点运行结果目录是否存在
            if is_file_dir_exists(s):
                #如果目录存在，且有_FAILURE，表明运行出错，立刻返回错误信息
                if is_dir_has_failure(s):
                    return False, 'Parent-Failure'
                #如果目录存在，但没有_SUCCESS，设置False，不报错。
                if not is_dir_has_success(s):
                    res = False, ''
            else:
                #如果目录不存在，设置为False
                res = False, ''
        return res

    def _run_pig_script(self):
        time.sleep(3)
        cmd = subprocess.Popen(['pig', '-x', 'mapreduce', self.__task['pigfile'][0]], 
                               stdout=subprocess.PIPE)
        ret = cmd.wait()
        if 0 == ret:
            return True
        return False

    def run(self):
        while True:
            can_run, reason = self._can_run()
            if can_run:
                #运行条件达成，创建子进程运行pig脚本。
                if self._run_pig_script():
                    self.__msgq.put(MsgExc(self.__task['name'], 'ok:Success'))
                else:
                    self.__msgq.put(MsgExc(self.__task['name'], 'error:Run-Failure'))
                break
            else:
                if reason=='':
                    pass
                else:
                    #父节点运行失败，所以本节点退出，不在等待
                    self.__msgq.put(MsgExc(self.__task['name'], 'error:Parent-Failure'))
                    break
            time.sleep(3)


class ExecuterRunner:
    """
    ExecuterRunner:
    1. 从root tasks开始执行，每个是一个执行体。
    2. 每个执行体，是一个线程对象，它每隔几秒会检查前提条件是否存在，如果条件符合，就开始执行，执行结束，就发个信号到queue里。
    3. 每隔一定时间，从queue获取信号，如果检查到一个任务执行完毕，就生成它的child节点任务，加入到待执行任务。
    4. 等所有得任务都执行完毕，就停止运行。
"""
    def __init__(self, tk):
        self.__tk = tk
        self.__msgq = Queue(100)
        self.__running_exec = []
        self.__first_run = True
        self.__executers = []
        self.__finished_tasks = []
        self.__started_task_names =[]

    def start(self):
        self.run()

    def _is_finished(self):
        for name in self.__tk.get_all_task_names():
            if not name in self.__finished_tasks:
                return False
        return True

    def run(self):
        while True:
            #首次运行
            if self.__first_run:
                root_nodes = self.__tk.get_roots()
                for r in root_nodes:
                    ex = Executer(r, self.__tk, self.__msgq)
                    self.__started_task_names.append(r['name'])
                    ex.start()
                    self.__executers.append(ex)
                self.__first_run = False
                continue
            #如果所有的任务都运行了，则退出循环。
            if self._is_finished():
                print "\n\n\nPig-DAG run successfully, done!"
                break
            #等待消息
            msg = self.__msgq.get()
            if msg.message.split(':')[0] == 'ok':
                self.__finished_tasks.append(msg.task_name)
                #如果节点运行成功，则启动它的所有子节点。
                child_names = self.__tk.get_node(msg.task_name)['childs']
                child_nodes = [self.__tk.get_node(name) for name in child_names]
                for c in child_nodes:
                    #避免多个父节点的节点被重复启动
                    if not c in self.__started_task_names:
                        self.__started_task_names.append(c['name'])
                        ex = Executer(c, self.__tk, self.__msgq)
                        ex.start()
                        self.__running_exec.append(ex)
            else:
                print "Error: %s, %s" %(msg.task_name, msg.message)
                break
