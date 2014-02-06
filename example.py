from __future__ import absolute_import

import socket
import greenlet
import smtplib
import requests

from tornado import ioloop
from blocks import Socket, green


@green
def helo():
    server = smtplib.SMTP('smtp.gmail.com')
    server.set_debuglevel(1)
    server.ehlo()
    server.quit()
    ioloop.IOLoop.current().stop()

@green
def plain():
    sock = Socket(socket.AF_INET)
    sock.connect(('reddit.com', 80))
    sock.send('GET / HTTP/1.1\r\nHost: reddit.com\r\n\r\n')
    data = sock.recv(100)
    print data
    ioloop.IOLoop.current().stop()

@green
def get():
    r = requests.get('http://reddit.com')
    print r.status_code
    print r.headers
    print r.text
    ioloop.IOLoop.current().stop()


ioloop.IOLoop.current().run_sync(helo)
