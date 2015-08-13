# -*- coding: utf8 -*-
import sys
import uuid

import umsgpack
from mock import MagicMock
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.node import NodeClient


class TestNodeClient(NodeClient):
    def __init__(self, *args, **kwargs):
        self._timeout = 5
        self._serializer = umsgpack


class NodeClientTest(unittest.TestCase):
    def setUp(self):
        self.client = TestNodeClient()
        self.client.sendMsg = MagicMock()

    def get_id(self):
        return str(uuid.uuid4())

    def test_on_error(self):
        self.assertRaises(TypeError, self.client._on_error, TypeError)

    def test_get(self):
        self.client.get('message')
        self.client.sendMsg.assert_called_once_with('message')

    def test_set(self):
        self.client.set('message')
        self.client.sendMsg.assert_called_once_with('message')

    def test_remove(self):
        self.client.remove('message')
        self.client.sendMsg.assert_called_once_with('message')

    def test_snapshot(self):
        self.client.snapshot()
        self.client.sendMsg.assert_called_once_with({'action': 'snapshot'})
