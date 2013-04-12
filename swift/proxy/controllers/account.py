#!/usr/bin/python
# --encoding: utf-8--
# Copyright (c) 2010-2012 OpenStack, LLC.
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

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import time
from urllib import unquote

from swift.common.utils import normalize_timestamp, public
from swift.common.constraints import check_metadata, MAX_ACCOUNT_NAME_LENGTH
from swift.common.http import is_success, HTTP_NOT_FOUND
from swift.proxy.controllers.base import Controller, get_account_memcache_key
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed, Request


class AccountController(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        if not self.app.allow_account_management:
            self.allowed_methods.remove('PUT')
            self.allowed_methods.remove('DELETE')

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        # 根据account的值得到目标节点
        partition, nodes = self.app.account_ring.get_nodes(self.account_name)
        # 排序得到的节点
        nodes = self.app.sort_nodes(nodes)
        
        #查询(读取)并返回相应
        # GETorHEAD_base会根据最优的结果(最近的修改时间, 和3个副本最少返回两个的原则)
        resp = self.GETorHEAD_base(
            req, _('Account'), partition, nodes, req.path_info.rstrip('/'),
            len(nodes))
        
        # 如果找不到account尝试自动创建
        if resp.status_int == HTTP_NOT_FOUND and self.app.account_autocreate:
            if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
                resp = HTTPBadRequest(request=req)
                resp.body = 'Account name length of %d longer than %d' % \
                            (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
                return resp
            headers = {'X-Timestamp': normalize_timestamp(time.time()),
                       'X-Trans-Id': self.trans_id,
                       'Connection': 'close'}
            resp = self.make_requests(
                Request.blank('/v1/' + self.account_name),
                self.app.account_ring, partition, 'PUT',
                '/' + self.account_name, [headers] * len(nodes))
            if not is_success(resp.status_int):
                self.app.logger.warning('Could not autocreate account %r' %
                                        self.account_name)
                return resp
            resp = self.GETorHEAD_base(
                req, _('Account'), partition, nodes, req.path_info.rstrip('/'),
                len(nodes))
        return resp

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        # 如果是不允许的方法， 返回错误
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(
                request=req,
                headers={'Allow': ', '.join(self.allowed_methods)})
        
        # 检查来自client的metadata是否正确
        error_response = check_metadata(req, 'account')
        # 如果发生错误则返回 
        if error_response:
            return error_response
        # 检测Account name的长度
        if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
            return resp
        
        # 根据account name在ring中得到节点
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-trans-id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            self.app.memcache.delete(
                get_account_memcache_key(self.account_name))
        
        # 执行请求
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'PUT',
            req.path_info, [headers] * len(accounts))
        
        # 返回结果
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-Trans-Id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            self.app.memcache.delete(
                get_account_memcache_key(self.account_name))
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'POST',
            req.path_info, [headers] * len(accounts))
        if resp.status_int == HTTP_NOT_FOUND and self.app.account_autocreate:
            if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
                resp = HTTPBadRequest(request=req)
                resp.body = 'Account name length of %d longer than %d' % \
                            (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
                return resp
            resp = self.make_requests(
                Request.blank('/v1/' + self.account_name),
                self.app.account_ring, account_partition, 'PUT',
                '/' + self.account_name, [headers] * len(accounts))
            if not is_success(resp.status_int):
                self.app.logger.warning('Could not autocreate account %r' %
                                        self.account_name)
                return resp
        return resp

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(
                request=req,
                headers={'Allow': ', '.join(self.allowed_methods)})
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-Trans-Id': self.trans_id,
                   'Connection': 'close'}
        if self.app.memcache:
            self.app.memcache.delete(
                get_account_memcache_key(self.account_name))
        resp = self.make_requests(
            req, self.app.account_ring, account_partition, 'DELETE',
            req.path_info, [headers] * len(accounts))
        return resp
