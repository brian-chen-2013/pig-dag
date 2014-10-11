#!/usr/bin/env python
#!-*- coding:utf-8 -*-

#所有的pig文件都存放在这个目录
PIG_FILE_DIR = '/home/brian/python-test/pig-dag/pigs/'

#task1是root节点，它得parents是空的，childs不是空
task1 = {
    'name':'myroot',
    'parents':[],  #可以有多个parent
    'childs':['ct1'], #可以有多个child
    'pigfile':['myroot.pig'],
}

#task2是task1的子节点，它的parents不是空。
task2 = {
    'name':'ct1',
    'parents':['myroot'],  #可以有多个parent
    'childs':[], #可以有多个child
    'pigfile':['ct1.pig'],
}

tasks = [task1, task2]
