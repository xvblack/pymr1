# -*- coding: utf-8 -*-

from __future__ import absolute_import

import errno
import os
import socket
import sys

from . import TTransportBase, TTransportException


class TSocketBase(TTransportBase):
    def _resolveAddr(self):
        if self._unix_socket is not None:
            return [(socket.AF_UNIX, socket.SOCK_STREAM, None, None,
                     self._unix_socket)]
        else:
            return socket.getaddrinfo(
                self.host,
                self.port,
                socket.AF_UNSPEC,
                socket.SOCK_STREAM,
                0,
                socket.AI_PASSIVE | socket.AI_ADDRCONFIG)

    def close(self):
        if self.handle:
            self.handle.close()
            self.handle = None


class TSocket(TSocketBase):
    """Socket implementation of TTransport base."""

    def __init__(self, host='localhost', port=9090, unix_socket=None):
        """Initialize a TSocket

        @param host(str)    The host to connect to.
        @param port(int)    The (TCP) port to connect to.
        @param unix_socket(str)  The filename of a unix socket to connect to.
                                 (host and port will be ignored.)
        """
        self.host = host
        self.port = port
        self.handle = None
        self._unix_socket = unix_socket
        self._timeout = None

    def set_handle(self, h):
        self.handle = h

    def is_open(self):
        return bool(self.handle)

    def set_timeout(self, ms):
        self._timeout = ms / 1000.0 if ms else None

        if self.handle:
            self.handle.settimeout(self._timeout)

    def open(self):
        try:
            res0 = self._resolveAddr()
            for res in res0:
                self.handle = socket.socket(res[0], res[1])
                self.handle.settimeout(self._timeout)
                try:
                    self.handle.connect(res[4])
                except socket.error as e:
                    if res is not res0[-1]:
                        continue
                    else:
                        raise e
                break
        except socket.error as e:
            if self._unix_socket:
                message = 'Could not connect to socket %s' % self._unix_socket
            else:
                message = 'Could not connect to %s:%d' % (self.host, self.port)
            raise TTransportException(type=TTransportException.NOT_OPEN,
                                      message=message)

    def read(self, sz):
        try:
            buff = self.handle.recv(sz)
        except socket.error as e:
            if (e.args[0] == errno.ECONNRESET and
                    (sys.platform == 'darwin' or
                     sys.platform.startswith('freebsd'))):
                # freebsd and Mach don't follow POSIX semantic of recv
                # and fail with ECONNRESET if peer performed shutdown.
                # See corresponding comment and code in TSocket::read()
                # in lib/cpp/src/transport/TSocket.cpp.
                self.close()
                # Trigger the check to raise the END_OF_FILE exception below.
                buff = ''
            else:
                raise
        if len(buff) == 0:
            raise TTransportException(type=TTransportException.END_OF_FILE,
                                      message='TSocket read 0 bytes')
        return buff

    def write(self, buff):
        if not self.handle:
            raise TTransportException(type=TTransportException.NOT_OPEN,
                                      message='Transport not open')
        self.handle.sendall(buff)

    def flush(self):
        pass


class TServerSocket(TSocketBase):
    """Socket implementation of TServerTransport base."""

    def __init__(self, host=None, port=9090, unix_socket=None,
                 socket_family=socket.AF_UNSPEC):
        self.host = host
        self.port = port
        self._unix_socket = unix_socket
        self._socket_family = socket_family
        self.handle = None

    def listen(self):
        res0 = self._resolveAddr()
        socket_family = self._socket_family == socket.AF_UNSPEC and (
            socket.AF_INET6 or self._socket_family)
        for res in res0:
            if res[0] is socket_family or res is res0[-1]:
                break

        # We need remove the old unix socket if the file exists and
        # nobody is listening on it.
        if self._unix_socket:
            tmp = socket.socket(res[0], res[1])
            try:
                tmp.connect(res[4])
            except socket.error as err:
                eno, message = err.args
                if eno == errno.ECONNREFUSED:
                    os.unlink(res[4])

        self.handle = socket.socket(res[0], res[1])
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(self.handle, 'settimeout'):
            self.handle.settimeout(None)
        self.handle.bind(res[4])
        self.handle.listen(128)

    def accept(self):
        client, addr = self.handle.accept()
        result = TSocket()
        result.set_handle(client)
        return result
