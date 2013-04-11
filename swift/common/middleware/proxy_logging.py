# Copyright (c) 2010-2011 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Logging middleware for the Swift proxy.

This serves as both the default logging implementation and an example of how
to plug in your own logging format/method.

The logging format implemented below is as follows:

client_ip remote_addr datetime request_method request_path protocol
    status_int referer user_agent auth_token bytes_recvd bytes_sent
    client_etag transaction_id headers request_time source

These values are space-separated, and each is url-encoded, so that they can
be separated with a simple .split()

* remote_addr is the contents of the REMOTE_ADDR environment variable, while
  client_ip is swift's best guess at the end-user IP, extracted variously
  from the X-Forwarded-For header, X-Cluster-Ip header, or the REMOTE_ADDR
  environment variable.

* Values that are missing (e.g. due to a header not being present) or zero
  are generally represented by a single hyphen ('-').

The proxy-logging can be used twice in the proxy server's pipeline when there
is middleware installed that can return custom responses that don't follow the
standard pipeline to the proxy server.

For example, with staticweb, the middleware might intercept a request to
/v1/AUTH_acc/cont/, make a subrequest to the proxy to retrieve
/v1/AUTH_acc/cont/index.html and, in effect, respond to the client's original
request using the 2nd request's body. In this instance the subrequest will be
logged by the rightmost middleware (with a swift.source set) and the outgoing
request (with body overridden) will be logged by leftmost middleware.

Requests that follow the normal pipeline (use the same wsgi environment
throughout) will not be double logged because an environment variable
(swift.proxy_access_log_made) is checked/set when a log is made.

All middleware making subrequests should take care to set swift.source when
needed. With the doubled proxy logs, any consumer/processor of swift's proxy
logs should look at the swift.source field, the rightmost log value, to decide
if this is a middleware subrequest or not. A log processor calculating
bandwidth usage will want to only sum up logs with no swift.source.
"""

import time
from urllib import quote, unquote

from swift.common.swob import Request
from swift.common.utils import (get_logger, get_remote_client,
                                get_valid_utf8_str, config_true_value,
                                InputProxy)


class ProxyLoggingMiddleware(object):
    """
    Middleware that logs Swift proxy requests in the swift log format.
    """

    def __init__(self, app, conf):
        self.app = app
        self.log_hdrs = config_true_value(conf.get(
            'access_log_headers',
            conf.get('log_headers', 'no')))
        # The leading access_* check is in case someone assumes that
        # log_statsd_valid_http_methods behaves like the other log_statsd_*
        # settings.
        self.valid_methods = conf.get(
            'access_log_statsd_valid_http_methods',
            conf.get('log_statsd_valid_http_methods',
                     'GET,HEAD,POST,PUT,DELETE,COPY,OPTIONS'))
        self.valid_methods = [m.strip().upper() for m in
                              self.valid_methods.split(',') if m.strip()]
        access_log_conf = {}
        for key in ('log_facility', 'log_name', 'log_level', 'log_udp_host',
                    'log_udp_port', 'log_statsd_host', 'log_statsd_port',
                    'log_statsd_default_sample_rate',
                    'log_statsd_sample_rate_factor',
                    'log_statsd_metric_prefix'):
            value = conf.get('access_' + key, conf.get(key, None))
            if value:
                access_log_conf[key] = value
        self.access_logger = get_logger(access_log_conf,
                                        log_route='proxy-access')
        self.access_logger.set_statsd_prefix('proxy-server')

    def method_from_req(self, req):
        return req.environ.get('swift.orig_req_method', req.method)

    def req_already_logged(self, req):
        return req.environ.get('swift.proxy_access_log_made')

    def mark_req_logged(self, req):
        req.environ['swift.proxy_access_log_made'] = True

    def log_request(self, req, status_int, bytes_received, bytes_sent,
                    request_time):
        """
        Log a request.

        :param req: swob.Request object for the request
        :param status_int: integer code for the response status
        :param bytes_received: bytes successfully read from the request body
        :param bytes_sent: bytes yielded to the WSGI server
        :param request_time: time taken to satisfy the request, in seconds
        """
        if self.req_already_logged(req):
            return
        req_path = get_valid_utf8_str(req.path)
        the_request = quote(unquote(req_path))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
        logged_headers = None
        if self.log_hdrs:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                                       for k, v in req.headers.items())
        method = self.method_from_req(req)
        self.access_logger.info(' '.join(
            quote(str(x) if x else '-')
            for x in (
                get_remote_client(req),
                req.remote_addr,
                time.strftime('%d/%b/%Y/%H/%M/%S', time.gmtime()),
                method,
                the_request,
                req.environ.get('SERVER_PROTOCOL'),
                status_int,
                req.referer,
                req.user_agent,
                req.headers.get('x-auth-token'),
                bytes_received,
                bytes_sent,
                req.headers.get('etag', None),
                req.environ.get('swift.trans_id'),
                logged_headers,
                '%.4f' % request_time,
                req.environ.get('swift.source'),
            )))
        self.mark_req_logged(req)
        # Log timing and bytes-transfered data to StatsD
        metric_name = self.statsd_metric_name(req, status_int, method)
        # Only log data for valid controllers (or SOS) to keep the metric count
        # down (egregious errors will get logged by the proxy server itself).
        if metric_name:
            self.access_logger.timing(metric_name + '.timing',
                                      request_time * 1000)
            self.access_logger.update_stats(metric_name + '.xfer',
                                            bytes_received + bytes_sent)

    def statsd_metric_name(self, req, status_int, method):
        if req.path.startswith('/v1/'):
            try:
                stat_type = [None, 'account', 'container',
                             'object'][req.path.strip('/').count('/')]
            except IndexError:
                stat_type = 'object'
        else:
            stat_type = req.environ.get('swift.source')
        if stat_type is None:
            return None
        stat_method = method if method in self.valid_methods \
            else 'BAD_METHOD'
        return '.'.join((stat_type, stat_method, str(status_int)))

    def __call__(self, env, start_response):
        start_response_args = [None]
        input_proxy = InputProxy(env['wsgi.input'])
        env['wsgi.input'] = input_proxy
        start_time = time.time()

        def my_start_response(status, headers, exc_info=None):
            start_response_args[0] = (status, list(headers), exc_info)

        def status_int_for_logging(client_disconnect=False, start_status=None):
            # log disconnected clients as '499' status code
            if client_disconnect or input_proxy.client_disconnect:
                return 499
            elif start_status is None:
                return int(start_response_args[0][0].split(' ', 1)[0])
            return start_status

        def iter_response(iterable):
            iterator = iter(iterable)
            try:
                chunk = iterator.next()
                while not chunk:
                    chunk = iterator.next()
            except StopIteration:
                chunk = ''
            for h, v in start_response_args[0][1]:
                if h.lower() in ('content-length', 'transfer-encoding'):
                    break
            else:
                if not chunk:
                    start_response_args[0][1].append(('content-length', '0'))
                elif isinstance(iterable, list):
                    start_response_args[0][1].append(
                        ('content-length', str(sum(len(i) for i in iterable))))
            start_response(*start_response_args[0])
            req = Request(env)

            # Log timing information for time-to-first-byte (GET requests only)
            method = self.method_from_req(req)
            if method == 'GET' and not self.req_already_logged(req):
                status_int = status_int_for_logging()
                metric_name = self.statsd_metric_name(req, status_int, method)
                if metric_name:
                    self.access_logger.timing_since(
                        metric_name + '.first-byte.timing', start_time)

            bytes_sent = 0
            client_disconnect = False
            try:
                while chunk:
                    bytes_sent += len(chunk)
                    yield chunk
                    chunk = iterator.next()
            except GeneratorExit:  # generator was closed before we finished
                client_disconnect = True
                raise
            finally:
                status_int = status_int_for_logging(client_disconnect)
                self.log_request(
                    req, status_int, input_proxy.bytes_received, bytes_sent,
                    time.time() - start_time)

        try:
            iterable = self.app(env, my_start_response)
        except Exception:
            req = Request(env)
            status_int = status_int_for_logging(start_status=500)
            self.log_request(
                req, status_int, input_proxy.bytes_received, 0,
                time.time() - start_time)
            raise
        else:
            return iter_response(iterable)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def proxy_logger(app):
        return ProxyLoggingMiddleware(app, conf)
    return proxy_logger
