#!/usr/bin/env python
#!-*- coding:utf-8 -*-

#所有的pig文件都存放在这个目录
PIG_FILE_DIR = '/home/brian/python-test/pig-dag/pigs/'

#task1是root节点，它得parents是空的，childs不是空
task1 = {
    'name':'st1',
    'parents':[],  #可以有多个parent
    'childs':['st1_1', 'st1_2'], #可以有多个child
    'pigfile':['st1.pig'],
}

#task2是task1的子节点，它的parents不是空。
task2 = {
    'name':'st1_1',
    'parents':['st1'],  #可以有多个parent
    'childs':[], #可以有多个child
    'pigfile':['st1_1.pig'],
}

#task3是task1的子节点，它的parents不是空。
task3 = {
    'name':'st1_2',
    'parents':['st1'],  #可以有多个parent
    'childs':[], #可以有多个child
    'pigfile':['st1_2.pig'],
}

tasks = [task1, task2, task3]
