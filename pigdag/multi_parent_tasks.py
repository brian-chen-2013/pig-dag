#!/usr/bin/env python
#!-*- coding:utf-8 -*-

#所有的pig文件都存放在这个目录
PIG_FILE_DIR = '/home/brian/python-test/pig-dag/pigs/'

#task1是root节点，它得parents是空的，childs不是空
task1 = {
    'name':'tt1',
    'parents':[],  #可以有多个parent
    'childs':['tt1_1'], #可以有多个child
    'pigfile':['tt1.pig'],
}

#task2是task1的子节点，它的parents不是空。
task2 = {
    'name':'tt2',
    'parents':[],  #可以有多个parent
    'childs':['tt1_1'], #可以有多个child
    'pigfile':['tt2.pig'],
}

#task3是task1的子节点，它的parents不是空。
task3 = {
    'name':'tt1_1',
    'parents':['tt1','tt2'],  #可以有多个parent
    'childs':[], #可以有多个child
    'pigfile':['tt1_1.pig'],
}

tasks = [task1, task2, task3]
