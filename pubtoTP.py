#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

'''
    The example depicts how to stream data to the standard kdb+ tickerplant API.
    Before you running it, you must have a TicherPlant and a RDB running with q command. e.g:
    q tp.q
    q rdb.q
    Pls refer to tp.q and rdb.q scripts separately.
'''

import datetime
import numpy
import random
import threading
import sys
import time

from qpython import qconnection
from qpython.qcollection import qlist
from qpython.qtype import QException, QTIMESPAN_LIST, QSYMBOL_LIST, QFLOAT_LIST, QINT_LIST

class PublisherThread(threading.Thread):

    def __init__(self, q):
        super(PublisherThread, self).__init__()
        self.q = q
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
                self.q.sync('upd', numpy.string_('quote'), self.get_quote_data())
                # 数据插入成功后，则可以连RDB查看表中的数据了，数据分析也可以马上展开。
                time.sleep(1)
            except QException as e:
                print('>*< excepted')
                print(e)
            except:
                self.stop()

    def get_quote_data(self):
        # c = random.randint(1, 10)
        c = random.randint(1, 1)
        # today的值为：2017-05-17T00:00:00.000000
        today = numpy.datetime64(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
        # 注意下面生成的模拟数据都是list类型！
        time = [numpy.timedelta64((numpy.datetime64(datetime.datetime.now()) - today), 'ms') for x in range(c)]
        sym = ['qPython' for x in range(c)]
        ask = [random.random() * random.randint(1, 100) for x in range(c)]
        # ask = [numpy.float64(random.random() * random.randint(1, 100)) for x in range(c)]
        asize = [random.randint(501, 1000) for x in range(c)]   # asksize
        bid = [random.random() * random.randint(1, 100) for x in range(c)]
        # bid = [numpy.float64(random.random() * random.randint(1, 100)) for x in range(c)]
        bsize = [random.randint(100, 500) for x in range(c)]    # bidsize
        #simdata = [qlist(time, qtype=QTIMESPAN_LIST), qlist(sym, qtype=QSYMBOL_LIST), qlist(bid, qtype=QFLOAT_LIST), qlist(ask, qtype=QFLOAT_LIST), qlist(bsize, qtype=QINT_LIST), qlist(asize, qtype=QINT_LIST)]
        simdata = [qlist(time, qtype=QTIMESPAN_LIST), qlist(sym, qtype=QSYMBOL_LIST), qlist(bid, qtype=QFLOAT_LIST),
                   qlist(ask, qtype=QFLOAT_LIST), qlist(bsize, qtype=QINT_LIST), qlist(asize, qtype=QINT_LIST)]
        print(simdata)
        return simdata

if __name__ == '__main__':
    # 连接TP，在owntick示例中TP运行在本机的8099端口上
    with qconnection.QConnection(host='localhost', port=8099) as q:
        print(q)
        print('IPC Version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
        print('Press <ENTER> to close application')

        t = PublisherThread(q)
        t.start()

        sys.stdin.readline()

        t.stop()
        t.join()
