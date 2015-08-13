# -*- coding: utf8 -*-
import sys
import uuid

import umsgpack
from mock import MagicMock, patch
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.broker import BrokerClient


class TestBrokerClient(BrokerClient):
    def __init__(self, *args, **kwargs):
        self._debug = False
        self._serializer = umsgpack


class BrokerClientTest(unittest.TestCase):
    def setUp(self):
        self.client = TestBrokerClient()
        self.client.sendMsg = MagicMock()

    def get_id(self):
        return str(uuid.uuid4())

    def test_register(self):
        server_id = self.get_id()
        self.client.register(server_id, 'tcp://address')
        expected_msg = {
            'action': 'register',
            'id': server_id,
            'address': 'tcp://address'}
        self.client.sendMsg.assert_called_with(expected_msg)

    def test_unregister(self):
        server_id = self.get_id()
        self.client.id = server_id
        self.client.unregister()
        expected_msg = {
            'action': 'unregister', 'id': server_id}
        self.client.sendMsg.assert_called_with(expected_msg)

    def test_register_reply(self):
        reply = self.client.register_reply('a message', 'the original request')
        self.assertEqual(reply, 'a message')

    def test_gotMessage(self):
        """Test if {}_reply methods are called"""
        request = {"action": "dummy"}
        reply = [umsgpack.dumps({'action': 'dummy', 'data': 'some_data'})]
        response = self.client.gotMessage(reply, request)
        self.assertEqual(response, None)  # method does not exist

        request = {"action": "register"}
        reply = [umsgpack.dumps({'action': 'register'})]
        self.client.register_reply = MagicMock()
        self.client.gotMessage(reply, request)
        self.client.register_reply.assert_called_once_with(
            {'action': 'register'}, request)
