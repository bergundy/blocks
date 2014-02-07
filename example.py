from __future__ import absolute_import

import socket
import greenlet
import smtplib

from tornado import ioloop, gen, httpclient
from itertools import islice
from blocks import Socket, green


@green
def helo():
    server = smtplib.SMTP('smtp.gmail.com')
    server.set_debuglevel(1)
    server.ehlo()
    server.quit()


@green
def plain():
    sock = Socket(socket.AF_INET)
    sock.connect(('reddit.com', 80))
    sock.send('GET / HTTP/1.1\r\nHost: reddit.com\r\n\r\n')
    data = sock.recv(100)
    print data


@green
def get():
    try:
        import requests
    except ImportError:
        logging.warn('Failed to import requests, skipping example')
        return
    r = requests.get('http://reddit.com')
    print r.status_code
    print r.headers
    print r.content


@gen.coroutine
def main(num_requests=5):
    client = httpclient.AsyncHTTPClient()
    requests = [client.fetch('http://ogogle.com') for x in xrange(num_requests)]
    results = yield requests + [helo(), get(), plain()]
    for result in islice(results, num_requests):
        print result


if __name__ == '__main__':
    ioloop.IOLoop.current().run_sync(main)
