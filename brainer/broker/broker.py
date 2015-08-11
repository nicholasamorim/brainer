import sys
import umsgpack

from twisted.python import log
from twisted.internet import reactor

from txzmq import ZmqREPConnection, ZmqEndpoint, ZmqFactory

from lib.mixins import SerializerMixin


class NodeManager(object):
    def __init__(self):
        self._nodes = []
        self._nodes_id = {}
        self._last_ping = {}

    def register(self, server_id):
        node_number = len(self._nodes) + 1
        self._nodes.append(node_number)
        self._nodes_id[node_number] = server_id
        return node_number

    def unregister(self, node, server_id=None):
        self._nodes.remove(node)
        del self._nodes_id[node]


class Broker(ZmqREPConnection, SerializerMixin):
    def __init__(self, *args, **kwargs):
        """
        :param node_manager: A Node Manager. Defaults to `NodeManager`.
        :param serializer: A serializer, defaults to umsgpack.
        :param debug: If True, will log debug messages. Defaults to False.
        """
        self._node_manager = kwargs.get('node_manager', NodeManager())
        self._serializer = kwargs.pop('serializer', umsgpack)
        self._debug = kwargs.pop('debug', False)

        super(Broker, self).__init__(*args, **kwargs)

        self._allowed_actions = (
            'register', 'unregister', 'ping',
            'route', 'publish')

    def gotMessage(self, message_id, *messageParts):
        message = self._unpack(messageParts[0])
        if self._debug:
            log.msg("New Message: {}".format(message))

        action = message['action']
        if action not in self._allowed_actions:
            self._reply_error("FORBBIDEN")

        method = getattr(self, action, None)
        if method is None:
            self._reply_error("NOT_IMPLEMENTED")

        method(message_id, message)

    def _reply_error(self, code):
        return self.reply({"success": False, "code": code})

    def reply(self, message_id, data):
        if self._debug:
            log.msg("Message Reply: {}".format(data))

        return super(Broker, self).reply(
            message_id, self._serializer.dumps(data))

    def register(self, message_id, message):
        node_number = self._node_manager.register(message['id'])
        self.reply(message_id, {"action": "register", "node": node_number})

    def unregister(self, message_id, message):
        node, server_id = message['node'], message['id']
        if self._debug:
            log.msg(
                'Unregister request received for node number {}'.format(node))
        self._node_manager.unregister(node, server_id)
        self.reply(message_id, {"action": "unregister", "unregistered": True})

    def ping(self, message_id, *messageParts):
        pass

    def pong(self, message_id, *messageParts):
        pass

    def route(self, message_id, *messageParts):
        pass

    def publish(self, message_id, *messageParts):
        pass


def run_broker():
    log.startLogging(sys.stdout)
    zf = ZmqFactory()
    e = ZmqEndpoint('bind', 'ipc:///tmp/brokersock')
    Broker(zf, e, debug=True)
    reactor.run()

if __name__ == '__main__':
    run_broker()
