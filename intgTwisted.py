#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'Adonis hzadonis@gmail.com'

'''
    The example presents how the qPython can be used along with Twisted to
    build asynchronous client:
    The samele code overwrites .u.sub and .z.ts functions on q process.
'''
import struct
import sys
from twisted.internet.protocol import Protocol, ClientFactory

from twisted.internet import reactor
from qpython.qconnection import MessageType, QAuthenticationException
from qpython.qreader import QReader
from qpython.qwriter import QWriter, QWriterException

class IPCProtocol(Protocol):

    class State(object):
        UNKNOWN = -1
        HANDSHAKE = 0
        CONNECTED = 1

    def connectionMade(self):
        self.state = IPCProtocol.State.UNKNOWN
        self.credentials = self.factory.username + ':' + self.factory.password if self.factory.password else ''

        self.transport.write(self.credentials + '\3\0')

        self.message = None

    def dataReceived(self, data):
        if self.state == IPCProtocol.State.CONNECTED:
            try:
                if not self._message:
                    self._message = self._reader.read_header(source = data)
                    self._buffer = ''

                self._buffer += data
                buffer_len = len(self._buffer) if self._buffer else 0

                while self._message and self._message.size <= buffer_len:
                    complete_message = self._buffer[:self._message.size]

                    if buffer_len > self._message.size:
                        self._buffer = self._buffer[self._message.size:]
                        buffer_len = len(self._buffer) if self._buffer else 0
                        self._message = self._reader.read_header(source=self._buffer)
                    else:
                        self._message = None
                        self._buffer = ''
                        buffer_len = 0

                    self.factory.onMessage(self._reader.read(source=complete_message,
                                                             numpy_temporals=True))
            except:
                self.factory.onError(ss.exc_info())
                self._message = None
                self._buffer = ''

        elif self.state == IPCProtocol.State.UNKNOWN:
            # handshake
            if len(data) == 1:
                self.__init(data)
            else:
                self.state = IPCProtocol.State.HANDSHAKE
                self.transport.write(self.credentials + '\0')

        else:
            # protocol version fallback
            if len(data) == 1:
                self._init(data)
            else:
                raise QAuthenticationException('Connection denied.')

    def __init(self, data):
        self.state = IPCProtocol.State.CONNECTED
        self.protocol_version = min(struct.unpack('B', data)[0], 3)
        self._writer = QWriter(stream=None, protocol_version=self.protocol_version)
        self._reader = QReader(stream=None)

        self.factory.clientReady(self)

    def query(self, msg_type, query, *parameters):
        if parameters and len(parameters) > 8:
            raise QWriterException('Too many parameters.')

        if not parameters or len(parameters) == 0:
            self.transport.write(self._writer.write(query, msg_type))
        else:
            self.transport.write(self._writer.write([query] + list(parameters), msg_type))

class IPCClientFactory(ClientFactory):

    protocol = IPCProtocol

    def __init__(self, username, password, connect_success_callback, connect_fail_callback,
                 data_callback, error_callback):
        self.username = username
        self.password = password
        self.client = None

        # register callbacks
        self.connect_success_callback = connect_success_callback
        self.connect_fail_callback = connect_fail_callback
        self.data_callback = data_callback
        self.error_callback = error_callback

    def clientConnectionLost(self, connector, reason):
        print('Lost connection. Reason: %s' % reason)
        # connector.connect()

    def clientConnectionFailed(self, connector, reason):
        if self.connect_fail_callback:
            self.connect_fail_callback(self, reason)

    def clientReady(self, client):
        self.client = client
        if self.connect_success_callback:
            self.connect_success_callback(self)

    def onMessage(self, message):
        if self.data_callback:
            self.data_callback(self, message)

    def onError(self, error):
        if self.error_callback:
            self.error_callback(self, error)

    def query(self, msg_type, query, *parameters):
        if self.client:
            self.client.query(msg_type, query, *parameters)

def onConnectSuccess(source):
    print('Connected, protocol version: %s' % source.client.protocol_version)
    source.query(MessageType.SYNC, '.z.ts:{(handle)(`timestamp$100?10000000000000000000)}')
    source.query(MessageType.SYNC, '.u.sub:{[t;s] handle:: neg .z.w}')
    source.query(MessageType.ASYNC, '.u.sub', 'trade', '')

def onConnectFail(source, reason):
    print('Connection refused: %s' % reason)

def onMessage(source, message):
    print('Received: %s %s' % (message.type, message.data))

def onError(source, error):
    print('Error: %s' % error)

if __name__ == '__main__':
    factory = IPCClientFactory('user', 'pwd', onConnectSuccess, onConnectFail,
                               onMessage, onError)
    reactor.connectTCP('localhost', 12888, factory)
    reactor.run()