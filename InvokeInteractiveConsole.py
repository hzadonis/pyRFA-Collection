#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

'''
    The example depicts how to create a simple interactive console for communication
    with a q process:
'''

import qpython
from qpython import qconnection
from qpython.qtype import QException

try:
    input = raw_input

except NameError:
    pass

if __name__ == '__main__':
    print('qPython %s Cython extensions enabled: %s' %
          (qpython.__version__, qpython.__is_cython_enabled__))
    with qconnection.QConnection(host='localhost', port=12888) as q:
        print(q)
        print('IPC Version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))

        while True:
            try:
                x = input('Q)')
            except EOFError:
                print('')
                break

            if x == '\\\\':
                break

            try:
                result = q(x)
                print(type(result))
                print(result)
            except QException as msg:
                print('q error: \%s' % msg)