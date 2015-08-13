# -*- coding: utf8 -*-
import sys

import umsgpack
from twisted.python import log
from twisted.internet import reactor, defer

from brainer.lib.base import BaseREP
from brainer.lib.hash import ConsistentHash
from brainer.lib.mixins import SerializerMixin
from brainer.lib.exceptions import ZeroNodeError
from brainer.node.client import NodeClient


class Broker(BaseREP, SerializerMixin):
    """This is a broker.
    """
    def __init__(self, factory, endpoint, *args, **kwargs):
        """
        :param factory: A `txzmq.ZmqFactory` object.
        :param endpoint: A `txzmq.ZmqEndpoint` object.
        :param node_manager: A Node Manager. Defaults to `NodeManager`.
        :param serializer: A serializer, defaults to umsgpack.
        :param debug: If True, will log debug messages. Defaults to False.
        """
        self._init_instance(*args, **kwargs)
        log.msg('Broker started!!! Serializer: {}'.format(
            self._serializer.__name__))

        super(Broker, self).__init__(factory, endpoint)

    def _init_instance(self, *args, **kwargs):
        self._serializer = kwargs.pop('serializer', umsgpack)
        self._debug = kwargs.pop('debug', False)
        self._publisher_address = kwargs.get(
            'publisher', 'ipc:///tmp/publisher.sock')

        # A list of node-ids, the index is the node_number
        self._nodes = []
        # Key: Value = node-id: connection obj
        self._nodes_connections = {}
        self._allowed_actions = (
            'register', 'unregister', 'ping',
            'route', 'set', 'get', 'remove')

    def register_node(self, node_id, address):
        """
        :param node_id: The node id sent down by the Node.
        """
        self._nodes.append(node_id)
        node_number = self._nodes.index(node_id)

        node_connection = NodeClient.create(address)
        self._nodes_connections[node_id] = node_connection
        return node_number

    def clean_connection(self, node_id):
        """Shutdown the connection and remove any reference
        to it. Called when the node unregisters.

        :param node_id: The node id sent down by the Node.
        """
        if node_id not in self._nodes_connections:
            return

        connection = self._nodes_connections[node_id]
        connection.shutdown()
        del self._nodes_connections[node_id]

    def unregister_node(self, node_id):
        """Entry-point for unregistering a node.
        It shuts down the connection, removes it from the list of nodes.

        :param node_id: The node id.
        """
        self.clean_connection(node_id)
        if node_id in self._nodes:
            self._nodes.remove(node_id)

    def get_node_by_key(self, key):
        """Gets the right machine based on the ky.

        :param key: The key to be set. It will be used to define which
        node that key should go.
        :returns: A `brainer.node.client.NodeClient` object.
        """
        if not self._nodes:
            raise ZeroNodeError

        hashing = ConsistentHash(len(self._nodes))
        node_number = hashing.get_machine(key)
        node_id = self._nodes[node_number]
        return self._nodes_connections[node_id]

    def gotMessage(self, message_id, *messageParts):
        """Any message received is processed here.

        :param message_id: The message id (Generated by ZeroMQ).
        :param messageParts: The message itself.
        """
        message = self.unpack(messageParts[0])
        if self._debug:
            log.msg("New Message: {}".format(message))

        action = message['action']
        if action not in self._allowed_actions:
            self.reply_error(
                message_id, "FORBBIDEN", "You cannot run this command.")

        method = getattr(self, action, None)
        if method is None:
            self.reply_error(
                message_id, "NOT_IMPLEMENTED", "Command not implemented.")

        try:
            d = defer.maybeDeferred(method, message_id, message)
        except ZeroNodeError:
            self.reply_error(
                message_id, "ZERO_NODES", "There are no nodes registered.")
            return

        d.addErrback(self._on_error, method, message_id)

    def _on_error(self, f, method, message_id):
        """On error, we reply a failure to the customer.

        This is not proper failure handling. Needs to be improved.

        :param f: A `twisted.python.failure.Failure`.
        :param method: The method that the client requested.
        :param message_id: The request message id.
        """
        log.err(f, "Method {} failed.".format(method))
        self.reply_error(message_id, "UNKNOWN_ERROR", "Verify server log")

    def register(self, message_id, message):
        """Registers a node and kick of the process of
        creating connections.

        :param message_id: The message id (generated by ZeroMQ)
        :param message: The message itself.
        """
        server_id, address = message['id'], message['address']
        self.register_node(server_id, address)
        is_first = len(self._nodes_connections) == 1
        if not is_first:
            d = self.snapshot(server_id)
        else:
            d = defer.succeed(None)

        d.addCallback(lambda snapshot: self.reply(
            message_id, {"action": "register", "snapshot": snapshot}))

    def unregister(self, message_id, message):
        """Unregisters a node and kicks off the process of
        cleaning connections.

        :param message_id: The message id (generated by ZeroMQ)
        :param message: The message itself.
        """
        node_id = message['id']
        if self._debug:
            log.msg(
                'Unregister request received for node ID {}'.format(node_id))
        self.unregister_node(node_id)
        self.reply(message_id, {"action": "unregister", "unregistered": True})

    def snapshot(self, requester_id):
        """Requests a snapshot from any other node that is not the requester.

        :param requester_id: A server id to filter out.
        """
        node = None
        for server_id, connection in self._nodes_connections.items():
            if server_id != requester_id:
                node = connection
                break

        return node.snapshot()

    def batch(self, main_node, wait_all, method, *args, **kwargs):
        """Performs an operation in every node. Used for write operations.

        :param main_node: The main node connection picked by the
        hashing method.
        :param wait_all: If True, the deferred will fire only when all
        nodes have replied to the write. If False, it fires immediately
        after the main node replies the write.
        :param method: A method to call in every node connection.
        E.g.: 'set'
        """
        func = getattr(main_node, method)
        main_defer = func(*args, **kwargs)
        dlist = [main_defer]

        for server_id, connection in self._nodes_connections.items():
            if connection == main_node:
                continue

            func = getattr(connection, method)
            d = func(*args, **kwargs)
            if wait_all:
                dlist.append(d)

        d = defer.DeferredList(dlist)
        return d

    def set(self, message_id, message):
        """Sets a key-value pair in the nodes.

        The set is tunable per-query. You can pick speed or consistency.
        This is defined by the parameter 'wait_all' in the message.
        If set to True, we only reply to the customer after all nodes
        get the data. On False, we reply as soon as the main node get
        the data (defined by the hashing).

        It currently defaults to True (consistency). So in the case a
        node goes down, it's harder to lose data, by default.

        :param message_id: The message id (generated by ZeroMQ)
        :param message: The message itself.
        """
        main_node = self.get_node_by_key(message['key'])
        wait_all = message.get('wait_all', True)
        d = self.batch(main_node, wait_all, 'set', message)
        d.addCallback(
            lambda replies: self.reply(message_id, replies[0][1]))

        return d

    def get(self, message_id, message):
        """Get a value based on a key.

        We will consult the right node based on the key (determined
        by hashing). But in case a node goes down, all other nodes
        should have the same data (see `set` documentation).

        :param message_id: The message id (generated by ZeroMQ)
        :param message: The message itself.
        """
        node = self.get_node_by_key(message['key'])
        d = node.get(message)
        d.addCallback(lambda reply: self.reply(message_id, reply))
        return d

    def remove(self, message_id, message):
        """Removes a key.

        :param message_id: The message id (generated by ZeroMQ)
        :param message: The message itself.
        """
        main_node = self.get_node_by_key(message['key'])
        wait_all = message.get('wait_all', True)
        d = self.batch(main_node, wait_all, 'remove', message)
        d.addCallback(
            lambda replies: self.reply(message_id, replies[0][1]))

        return d


def run_broker(host, debug=False):
    log.startLogging(sys.stdout)
    Broker.create(host, debug=debug)
    reactor.run()

if __name__ == '__main__':
    run_broker()
