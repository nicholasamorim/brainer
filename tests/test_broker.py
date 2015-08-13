# -*- coding: utf8 -*-
import sys
import uuid

from mock import MagicMock, patch
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.broker import Broker
from brainer.lib.exceptions import ZeroNodeError


class TestBroker(Broker):
    def __init__(self, factory, endpoint, *args, **kwargs):
        self._init_instance()


class BrokerTest(unittest.TestCase):
    def setUp(self):
        self.mock_factory = MagicMock()
        self.mock_endpoint = MagicMock()
        self.mock_node_client = MagicMock()
        self.node_patcher = patch(
            'brainer.broker.broker.NodeClient',
            self.mock_node_client)
        self.node_patcher.start()
        self.broker = TestBroker(self.mock_factory, self.mock_endpoint)

    def get_id(self):
        return str(uuid.uuid4())

    def test_register_node(self):
        server_id = self.get_id()
        self.broker.register_node(server_id, 'anaddress')
        self.assertEqual(self.broker._nodes, [server_id])
        self.mock_node_client.create.assert_called_once_with('anaddress')
        self.assertIn(server_id, self.broker._nodes_connections)

    def test_register(self):
        self.broker.reply = MagicMock()
        server_id = self.get_id()
        message = {'id': server_id, 'address': 'anddress'}
        self.broker.register(13123123, message)

        self.broker.snapshot = MagicMock()
        self.assertEqual(self.broker._nodes, [server_id])
        self.mock_node_client.create.assert_called_with('anddress')
        self.assertIn(server_id, self.broker._nodes_connections)
        self.broker.snapshot.assert_not_called()

        server_id2 = self.get_id()
        message = {'id': server_id2, 'address': 'anddress2'}
        self.broker.register(66666, message)
        self.mock_node_client.create.assert_called_with('anddress2')
        self.assertIn(server_id2, self.broker._nodes_connections)
        self.broker.snapshot.assert_called_with(server_id2)

    def test_clean_connection_no_node(self):
        self.assertEqual(
            self.broker.clean_connection('unexistent'),
            None)

    def test_clean_connection_shutdown_is_called(self):
        server_id = self.get_id()
        connection = MagicMock()
        self.broker._nodes_connections[server_id] = connection
        self.broker.clean_connection(server_id)
        connection.shutdown.assert_called_with()

    def test_unregister_node(self):
        server_id = self.get_id()
        connection = MagicMock()
        self.broker._nodes_connections[server_id] = connection
        self.broker.unregister_node(server_id)
        self.assertEqual(self.broker._nodes, [])
        connection.shutdown.assert_called_with()

    def test_get_node_by_key_no_node(self):
        self.assertRaises(ZeroNodeError, self.broker.get_node_by_key, 'what')

    def test_get_node_by_key(self):
        id1, id2 = self.setup_two_nodes()
        connection = self.broker.get_node_by_key('key1')
        self.assertEqual(self.broker._nodes_connections[id2], connection)

        connection = self.broker.get_node_by_key('key2')
        self.assertEqual(self.broker._nodes_connections[id2], connection)

        connection = self.broker.get_node_by_key('key3')
        self.assertEqual(self.broker._nodes_connections[id1], connection)

    def test_snapshot(self):
        self.broker._nodes_connections = {'id1': MagicMock, 'id2': MagicMock()}
        self.broker.snapshot('id1')
        node_to_be_called = self.broker._nodes_connections['id2']
        node_to_be_called.snapshot.assert_called_once_with()

    def setup_two_nodes(self):
        id1, id2 = self.get_id(), self.get_id()
        self.broker._nodes = [id1, id2]
        self.broker._nodes_connections = {id1: MagicMock(), id2: MagicMock()}
        return id1, id2

    def test_get(self):
        id1, id2 = self.setup_two_nodes()
        message = {'key': 'key1'}
        self.broker.get(1231231, message)
        expected_node = self.broker._nodes_connections[id2]
        expected_node.get.assert_called_once_with(message)

        message = {'key': 'key3'}
        self.broker.get(1231231, message)
        expected_node = self.broker._nodes_connections[id1]
        expected_node.get.assert_called_once_with(message)

    def test_batch_wait_all(self):
        id1, id2 = self.setup_two_nodes()
        main_node = self.broker._nodes_connections[id1]
        secondary_node = self.broker._nodes_connections[id2]
        d = self.broker.batch(main_node, True, 'dummy', 'arg1', arg2=2)

        main_node.dummy.assert_called_with('arg1', arg2=2)
        secondary_node.dummy.assert_called_with('arg1', arg2=2)
        self.assertEqual(len(d._deferredList), 2)  # wait all

    def test_batch_dont_wait_all(self):
        id1, id2 = self.setup_two_nodes()
        main_node = self.broker._nodes_connections[id1]
        secondary_node = self.broker._nodes_connections[id2]
        d = self.broker.batch(main_node, False, 'dummy', 'arg1', arg2=2)

        main_node.dummy.assert_called_with('arg1', arg2=2)
        secondary_node.dummy.assert_called_with('arg1', arg2=2)
        self.assertEqual(len(d._deferredList), 1)  # wait all
