#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

import pyrfa
import logging
import logging.handlers
# import socket
from qpython import qconnection

def makeConnection():
    global logger   # 将logger定义为全局变量，便于在makeConnection这个函数之外进行数据操作写日志时调用
    LOG_FILE = 'pyRFA.log'

    #handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024*1024, backupCount=5, encoding='utf-8') # 实例化handler，考虑到特殊字符需要设置encoding为utf-8
    handler = logging.handlers.RotatingFileHandler(LOG_FILE, encoding='utf-8') # 实例化handler，考虑到特殊字符需要设置encoding为utf-8
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)     # 为handler添加formatter

    logger = logging.getLogger('pyRFA') # 获取名为pyRFA的logger
    logger.addHandler(handler)          # 为logger添加handler
    logger.setLevel(logging.INFO)

    logger.info('pyRFAsample is about to run and write log file.')

    # sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sk.settimeout(10)
    try:
        q = qconnection.QConnection(host='localhost', port=5010)  # 连接到本机的5010端口，为在本机跑的TP的端口
        q.open()
        q.is_connected()
        # sk.connect(('127.0.0.1', 5010))
        logger.info('TickerPlant Server port 5010 connected.')
        #　getRealTimeDatafromTREP()   # 调用getRealTimeDatafromTREP获取实时数据
    except Exception:
        logger.info('TickerPlant Server port 5010 NOT connected! Exit pyRFAsample.')
    # sk.close()
    finally:
        q.close()   # Terminate the remote connection

def getRealTimeDatafromTREP():
    p = pyrfa.Pyrfa()
    p.createConfigDb("./pyRFAsample.cfg")   # 指定配置文件
    p.acquireSession("Session1")            # 指定读配置文件哪个Session节的配置
    p.createOMMConsumer()
    p.login()
    p.directoryRequest()
    p.dictionaryRequest()
    #p.setView('DSPLY_NAME,22,25')           # 定义所需查看的字段
    p.setView()                           # 改为需要查看所有的字段
    #p.marketPriceRequest("EUR=")            # RIC List, ask data
    #p.marketPriceRequest("JPY=,EUR=,300570.SZ,600031.SH")   # RIC List, ask data
    p.marketByOrderRequest("0157.HK")
    p.marketByPriceRequest("0100.HK")
    end = False
    while not end:
        try:
            updates = p.dispatchEventQueue(100)
        except KeyboardInterrupt:
            end = True
        if updates:
            print("")
            for u in updates:
                print(u['SERVICE'] + "-" + u['RIC'])    # 打印出“ServiceName - RIC”格式
                for k, v in u.items():   # 可以将k理解为FieldName，v为其值
                    fid = p.getFieldID(k)   # 通过FieldName获取其FID
                    if type(v) is float:    # 如果v的值为float类型
                        print("%20s %g" % (k+' ('+str(fid)+')', v))    # %20s：将k格式化为20位字符长度；%g：浮点数字(根据值的大小采用%e或%f)
                        logger.info("%20s %g" % (k+' ('+str(fid)+')', v))           # 将k, v值写到日志中
                    else:                   # 否则：
                        print("%20s %s" % (k+' ('+str(fid)+')', v))    # %20s：将k格式化为20位字符长度；%s：字符串
                        logger.info("%20s %s" % (k+' ('+str(fid)+')', v))           # 将k, v值写到日志中
                print("")

    p.marketPriceCloseAllRequest()
    #p.marketByOrderCloseAllRequest()
    #p.marketByPriceCloseAllRequest()

def PythonandQConn():
    q = qconnection.QConnection(host='localhost', port=5010)    # 连接到本机的5010端口，为在本机跑的TP的端口
    q.open()
    q.is_connected()
    #data = a('select from AShare_EOD where RIC = `002742, Date = 2017.05.08')   # 发送请求数据的qsql
    #print(data) # 打印所获取的数据

if __name__ == '__main__':
    # PythonandQConn()
    makeConnection()
    getRealTimeDatafromTREP()