from __future__ import absolute_import

import collections
import functools
import inspect
import socket
import time
import warnings

from tornado import ioloop, iostream, gen, stack_context, netutil
from tornado.concurrent import Future
import mock
import greenlet

__all__ = ['Socket', 'green']

version_tuple = (0, 1, '+')


def get_version_string():
    if isinstance(version_tuple[-1], basestring):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

version = get_version_string()
"""Current version of Motor."""


HAS_SSL = True
try:
    import ssl
except ImportError:
    ssl = None
    HAS_SSL = False


callback_type_error = TypeError("callback must be a callable")


def blocking_sock_method(method):
    """Wrap a MotorSocket method to pause the current greenlet and arrange
       for the greenlet to be resumed when non-blocking I/O has completed.
    """
    @functools.wraps(method)
    def _blocking_sock_method(self, *args, **kwargs):
        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main, "Should be on child greenlet"

        timeout_object = None

        if self.timeout:
            def timeout_err():
                # Running on the main greenlet. If a timeout error is thrown,
                # we raise the exception on the child greenlet. Closing the
                # IOStream removes callback() from the IOLoop so it isn't
                # called.
                self.stream.set_close_callback(None)
                self.stream.close()
                child_gr.throw(socket.timeout("timed out"))

            timeout_object = self.stream.io_loop.add_timeout(
                time.time() + self.timeout, timeout_err)

        # This is run by IOLoop on the main greenlet when operation
        # completes; switch back to child to continue processing
        def callback(result=None):
            self.stream.set_close_callback(None)
            if timeout_object:
                self.stream.io_loop.remove_timeout(timeout_object)

            child_gr.switch(result)

        # Run on main greenlet
        def closed():
            if timeout_object:
                self.stream.io_loop.remove_timeout(timeout_object)

            # The child greenlet might have died, e.g.:
            # - An operation raised an error within PyMongo
            # - PyMongo closed the MotorSocket in response
            # - MotorSocket.close() closed the IOStream
            # - IOStream scheduled this closed() function on the loop
            # - PyMongo operation completed (with or without error) and
            #       its greenlet terminated
            # - IOLoop runs this function
            if not child_gr.dead:
                child_gr.throw(socket.error("error"))

        self.stream.set_close_callback(closed)

        try:
            kwargs['callback'] = callback

            # method is MotorSocket.open(), recv(), etc. method() begins a
            # non-blocking operation on an IOStream and arranges for
            # callback() to be executed on the main greenlet once the
            # operation has completed.
            method(self, *args, **kwargs)

            # Pause child greenlet until resumed by main greenlet, which
            # will pass the result of the socket operation (data for recv,
            # number of bytes written for sendall) to us.
            return main.switch()
        except socket.error:
            raise
        except IOError, e:
            # If IOStream raises generic IOError (e.g., if operation
            # attempted on closed IOStream), then substitute socket.error,
            # since socket.error is what PyMongo's built to handle. For
            # example, PyMongo will catch socket.error, close the socket,
            # and raise AutoReconnect.
            raise socket.error(str(e))

    return _blocking_sock_method


class StreamWrapper(object):
    _GLOBAL_DEFAULT_TIMEOUT = socket._GLOBAL_DEFAULT_TIMEOUT

    def close(self):
        sock = self.stream.socket
        try:
            self.stream.close()
        except KeyError:
            # Tornado's _impl (epoll, kqueue, ...) has already removed this
            # file descriptor from its dict.
            pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()


class Socket(StreamWrapper):
    """Replace socket with a class that yields from the current greenlet, if
    we're on a child greenlet, when making blocking calls, and uses Tornado
    IOLoop to schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, *args, **kwargs):
        sock = kwargs.pop('socket', None)

        if sock is None:
            self.timeout = None
            patch.stop()
            try:
                sock = socket.socket(*args, **kwargs)
            finally:
                patch.start()
        self.stream = iostream.IOStream(sock)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        # IOStream calls socket.setblocking(False), which does settimeout(0.0).
        # We must not allow pymongo to set timeout to some other value (a
        # positive number or None) or the socket will start blocking again.
        # Instead, we simulate timeouts by interrupting ourselves with
        # callbacks.
        self.timeout = timeout

    @blocking_sock_method
    def connect(self, address, callback=None):
        """
        :Parameters:
         - `address`: A tuple, (host, port)
        """
        self.stream.connect(address, callback)

    def sendall(self, data):
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        try:
            self.stream.write(data)
        except IOError, e:
            # PyMongo is built to handle socket.error here, not IOError
            raise socket.error(str(e))

        if self.stream.closed():
            # Something went wrong while writing
            raise socket.error("write error")

    send = sendall

    @blocking_sock_method
    def recv(self, num_bytes, callback):
        self.stream.read_bytes(num_bytes, callback)

    def makefile(self, *args, **kwargs):
        return File(self.timeout, self.stream)


class File(StreamWrapper):
    def __init__(self, timeout, io_stream):
        self.timeout = timeout
        self.stream = io_stream

    @blocking_sock_method
    def readline(self, size=None, callback=None):
        # TODO: implement size
        self.stream.read_until('\r\n', callback)


patch = mock.patch.object(socket, 'socket', side_effect=Socket)


def green(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = Future()

        def patched():
            with patch:
                result = fn(*args, **kwargs)
                future.set_result(result)

        greenlet.greenlet(patched).switch()
        return future

    return wrapper
