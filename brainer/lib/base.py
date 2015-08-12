import umsgpack

from twisted.python import log
from txzmq import ZmqREPConnection, ZmqFactory, ZmqEndpoint


class BaseREP(ZmqREPConnection):
    _serializer = umsgpack

    def reply(self, message_id, data, raw=False):
        """Used when replying.

        :param message_id: The request message id. Used internally
        to route the message to the right sender.
        :param data: Data to be sent back.
        :param raw: Defaults to False. If True, we will send the data as-is
        without serializing it.
        """
        if self._debug:
            log.msg("Message Reply: {}".format(data))

        if not raw:
            data = self._serializer.dumps(data)
        return super(BaseREP, self).reply(message_id, data)

    def reply_error(self, message_id, code, message=None):
        """Used when replying with an error. Envelopes the message
        with a status.

        :param message_id: The request message id.
        :param code: A constant code.
        :param message: An optional human-readable message
        explaining what happened.
        """
        return self.reply(
            message_id,
            {"success": False, "code": code, "message": message})

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
