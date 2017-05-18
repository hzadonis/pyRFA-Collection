#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

'''
    The example depicts how to subscribe to standard kdb+ tickerplant service:
'''

import numpy
import threading
import sys

from qpython import qconnection
from qpython.qtype import QException
from qpython.qconnection import MessageType
from qpython.qcollection import QTable

class ListenerThread(threading.Thread):

    def __init__(self, q):
        super(ListenerThread, self).__init__()
        self.q = q
        self._stopper = threading.Event()

    def stopit(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.is_set()

    def run(self):
        while not self.stopped():
            print('.')
            try:
                message = self.q.receive(data_only=False, raw=False)
                if message.type != MessageType.ASYNC:
                    print('Unexpected message, expected message of type: ASYNC')

                print('type: %s, message type: %s, data size: %s, is_compressed: %s' %
                      (type(message), message.type, message.size, message.is_compressed))

                if isinstance(message.data, list):
                    if len(message.data) == 3 and message.data[0] == b'upd' and isinstance(message.data[2], QTable):
                        for row in message.data[2]:
                            print(row)
            except QException as e:
                print(e)

if __name__ == '__main__':
    with qconnection.QConnection(host='localhost', port=5010) as q:
        print(q)
        print('IPC Version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))
        print('Press <ENTER> to close application')

        # subscribe to tick
        response = q.sync('.u.sub', numpy.string_('trade'), numpy.string_(''))
        # get table model
        if isinstance(response[1], QTable):
            print('%s table data model: %s' % (response[0], response[1].dtype))

        t = ListenerThread(q)
        t.start()

        sys.stdin.readline()

        t.stopit()
