#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

'''
    This is the script that retrieves real time data from TREP via PyRFA,
    connects to TickerPlant, invokes the function that had been pre-defined
    in the tp.q script to insert formatted list data into tables in-memory.
'''

import pyrfa
import logging
import logging.handlers
import datetime
import numpy
import threading
import sys
import time

from qpython import qconnection
from qpython.qcollection import qlist
from qpython.qtype import QException, QTIMESPAN_LIST, QSYMBOL_LIST, QFLOAT_LIST, QINT_LIST

class PublisherThread(threading.Thread):

    def __init__(self, pubtrdq, pubtrdp):
        super(PublisherThread, self).__init__()
        self.q = pubtrdq
        self.p = pubtrdp
        self._stopper = threading.Event()

    def stop(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.isSet()

    def run(self):
        while not self.stopped():
            print('^oo^ running...')
            try:
                # publish data to TickerPlant
                print('trying...')
                # 调用TP脚本中的数据插入函数，在owntick示例中为tp.q脚本中的upd函数
                # 参数说明：upd函数，quote表，从get_quote_data()中生成的模拟数据
                #self.q.sync('upd', numpy.string_('quote'), self.get_realtimedata_from_trep())   # 获得实时报价数据
                self.get_realtimedata_from_trep()
                #self.q.sync('upd', numpy.string_('trade'), self.get_tradedata_from_trep())      # 获得实时成交数据
                # 数据插入成功后，则可以连RDB查看表中的数据了，数据分析也可以马上展开。
                time.sleep(1)
            except QException as e:
                print('>*< excepted')
                print(e)
                logger.info(e)
            except:
                self.q.stop()

    def get_tradedata_from_trep(self):
        # 定义所需查看的字段:
        self.p.setView('DSPLY_NAME,TRDTIM_1,TRDPRC_1,TRDVOL_1,OFFCL_CODE')
        self.p.marketPriceRequest("603728.SH,300570.SZ,600031.SH,600995.SH,002783.SZ")
        end = False
        while not end:
            try:
                updates = self.p.dispatchEventQueue(100)
            except KeyboardInterrupt:
                end = True
            if updates:
                print("")
                for u in updates:
                    print(u['SERVICE'] + "-" + u['RIC'])  # 打印出“ServiceName - RIC”格式
                    for k, v in u.items():  # 可以将k理解为FieldName，v为其值
                        fid = self.p.getFieldID(k)  # 通过FieldName获取其FID
                        if type(v) is float:  # 如果v的值为float类型
                            print(
                                "%20s %g" % (k + ' (' + str(fid) + ')', v))  # %20s：将k格式化为20位字符长度；%g：浮点数字(根据值的大小采用%e或%f)
                            logger.info("%20s %g" % (k + ' (' + str(fid) + ')', v))  # 将k, v值写到日志中
                        else:  # 否则：
                            print("%20s %s" % (k + ' (' + str(fid) + ')', v))  # %20s：将k格式化为20位字符长度；%s：字符串
                            logger.info("%20s %s" % (k + ' (' + str(fid) + ')', v))  # 将k, v值写到日志中
                        # tradedata = [qlist()]
                    print("")

    def get_realtimedata_from_trep(self):
        today = numpy.datetime64(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
        # 定义所需查看的字段:
        self.p.setView('DSPLY_NAME,EXCHTIM,BID,ASK,BIDSIZE,ASKSIZE')
        # 改为需要查看所有的字段:
        # self.p.setView()

        '''
        RIC List, ask data:
        Subscription for `MARKET_PRICE`(level 1)
        Subscription for `MARKET_BY_ORDER`(order book)
        Subscription for `MARKET_BY_PRICE`(market depth)
        '''
        # self.p.marketPriceRequest("EUR=")
        self.p.marketPriceRequest("300570.SZ,600031.SH,600995.SH,002783.SZ")
        # self.p.marketPriceRequest("603728.SH")
        # self.p.marketByOrderRequest("600031.SS")
        # self.p.marketByPriceRequest("600995.SS")
        end = False
        while not end:
            try:
                updates = self.p.dispatchEventQueue(100)
            except KeyboardInterrupt:
                end = True
            if updates:
                print("")
                for u in updates:
                    if u['MTYPE'] == 'REFRESH':
                        # 测试过程中发现，如果MTYPE是REFRESH时，相应信息只有ServiceName和RIC字段，除此之外并无其他有用的数据，故而不需要Insert到kdb+中
                        updatetable = False
                    else: # 当MTYPE不为REFRESH时，才需要Insert到kdb+中
                        updatetable = True
                    print(u['SERVICE'] + "-" + u['RIC'])  # 打印出“ServiceName - RIC”格式
                    ric = [u['RIC']]
                    for k, v in u.items():  # 可以将k理解为FieldName，v为其值
                        fid = self.p.getFieldID(k)  # 通过FieldName获取其FID
                        if fid == 22:    # BID域
                            bid = [v]    # BID域的值已为float类型
                        elif fid == 25:  # ASK域
                            ask = [v]    # ASK域的值已为float类型
                        elif fid == 30:  # BIDSIZE域
                            bidsize = [numpy.int(v)]
                        elif fid == 31:  # ASKSIZE域
                            asksize = [numpy.int(v)]
                        elif fid == 1067:    # EXCHTIM域
                            quotetime = [numpy.timedelta64((numpy.datetime64(datetime.datetime.now()) - today), 'ms')]
                        else:
                            pass

                        if type(v) is float:  # 如果v的值为float类型
                            print("%20s %g" % (k + ' (' + str(fid) + ')', v))  # %20s：将k格式化为20位字符长度；%g：浮点数字(根据值的大小采用%e或%f)
                            logger.info("%20s %g" % (k + ' (' + str(fid) + ')', v))  # 将k, v值写到日志中
                        else:  # 否则：
                            print("%20s %s" % (k + ' (' + str(fid) + ')', v))  # %20s：将k格式化为20位字符长度；%s：字符串
                            logger.info("%20s %s" % (k + ' (' + str(fid) + ')', v))  # 将k, v值写到日志中

                    if updatetable:
                        quotedata = [qlist(quotetime, qtype=QTIMESPAN_LIST), qlist(ric, qtype=QSYMBOL_LIST),
                                     qlist(bid, qtype=QFLOAT_LIST), qlist(ask, qtype=QFLOAT_LIST),
                                     qlist(bidsize, qtype=QINT_LIST), qlist(asksize, qtype=QINT_LIST)]
                        print(quotedata)
                        self.q.sync('upd', numpy.string_('quote'), quotedata)
                    else:
                        pass

if __name__ == '__main__':
    global logger  # 将logger定义为全局变量，便于在整个程序架构内进行数据操作写日志时调用
    LOG_FILE = 'pyRFA.log'

    # handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024*1024, backupCount=5, encoding='utf-8') # 实例化handler，考虑到特殊字符需要设置encoding为utf-8
    handler = logging.handlers.RotatingFileHandler(LOG_FILE, encoding='utf-8')  # 实例化handler，考虑到特殊字符需要设置encoding为utf-8
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

    formatter = logging.Formatter(fmt)  # 实例化formatter
    handler.setFormatter(formatter)  # 为handler添加formatter

    logger = logging.getLogger('pyRFA')  # 获取名为pyRFA的logger
    logger.addHandler(handler)  # 为logger添加handler
    logger.setLevel(logging.INFO)

    logger.info('TrepRFAtoTP is about to run and write log file.')

    try:
        # PyRFA连接TREP：
        p = pyrfa.Pyrfa()
        p.createConfigDb("./pyRFAsample.cfg")  # 指定配置文件
        p.acquireSession("Session1")  # 指定读配置文件哪个Session节的配置
        p.createOMMConsumer()
        p.login()
        p.directoryRequest()
        p.dictionaryRequest()

        # 连接TP，在owntick示例中TP运行在本机的8099端口上
        with qconnection.QConnection(host='localhost', port=8099) as q:
            print(q)
            print('IPC Version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
            print('Press <ENTER> to close application')
            logger.info('TickerPlant Server port 8099 connected.')

            t = PublisherThread(q, p)
            t.start()

            sys.stdin.readline()

            t.stop()
            t.join()
    except Exception:
        logger.info('TickerPlant Server port 8099 NOT connected! Exit TrepRFAtoTP.')
    finally:
        # Terminate the connection to TP:
        q.close()
        # Close subscribe to TREP:
        p.marketPriceCloseAllRequest()
        p.marketByOrderCloseAllRequest()
        p.marketByPriceCloseAllRequest()
