# -*- coding: utf8 -*-
import sys
import uuid

from mock import MagicMock, patch
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.node import Node


class TestNode(Node):
    def __init__(self, factory, endpoint, **kwargs):
        self._init_instance(endpoint.address, **kwargs)


class NodeTest(unittest.TestCase):
    def setUp(self):
        self.mock_factory = MagicMock()
        self.mock_endpoint = MagicMock()
        self.mock_node_client = MagicMock()
        self.broker_patcher = patch(
            'brainer.node.node.BrokerClient',
            self.mock_node_client)
        self.broker_patcher.start()
        self.node = TestNode(
            self.mock_factory,
            self.mock_endpoint,
            broker='anaddress',
            cache_class=MagicMock)

    def test_id(self):
        node_id = self.node.id
        self.assertEqual(node_id, self.node.id)

    def test_snapshot(self):
        self.node.snapshot({'message': 'snapshot'})
        self.node._cache.snapshot.assert_called_with()

    def test_connected(self):
        message = {'snapshot': None}
        self.node._connected(message)
        self.node._cache.replay.assert_not_called()

        message = {'snapshot': {'data': {}}}
        self.node._connected(message)
        self.node._cache.replay.assert_called_with(message['snapshot'])

    def test_on_shutdown_not_registered(self):
        self.node._broker = MagicMock()
        self.node.on_shutdown()
        self.node._broker.unregister().assert_not_called()

    def test_on_shutdown_registered(self):
        self.node._broker = MagicMock()
        self.node._is_registered = True
        self.node.on_shutdown()
        self.node._broker.unregister.assert_called_with()

    def test_get(self):
        message = {'key': 'akey'}
        self.node.get(message)
        self.node._cache.get.assert_called_with(message['key'])

    def test_remove(self):
        message = {'key': 'akey'}
        self.node.remove(message)
        self.node._cache.remove.assert_called_with(message['key'])

    def test_set(self):
        message = {'key': 'mykey', 'value': 'myvalue'}
        self.node.set(message)
        self.node._cache.set.assert_called_with(
            message['key'], message['value'])
