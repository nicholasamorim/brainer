import umsgpack

from twisted.python import log
from txzmq import ZmqREPConnection, ZmqFactory, ZmqEndpoint


class BaseREP(ZmqREPConnection):
    _serializer = umsgpack

    def sendMsg(self, message):
        """
        """
        d = super(BaseREP, self).sendMsg(
            self.pack(message))
        d.addCallback(self.gotMessage, message)
        return d

    def reply(self, message_id, data, raw=False):
        if self._debug:
            log.msg("Message Reply: {}".format(data))

        if not raw:
            data = self._serializer.dumps(data)
        return super(BaseREP, self).reply(message_id, data)

    def reply_error(self, code, message=None):
        """Used when replying with an error.
        """
        return self.reply({"success": False, "code": code, "message": message})

    @classmethod
    def create(cls, address, **kwargs):
        """Factory method to create a Broker.

        :param address: An address to bind the Broker.
        E.g.: ipc:///tmp/broker.sock
        :param node_manager: A Node Manager. Defaults to `NodeManager`.
        :param serializer: A serializer, defaults to umsgpack.
        :param debug: If True, will log debug messages. Defaults to False.
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('bind', address)
        return cls(factory, endpoint, **kwargs)
