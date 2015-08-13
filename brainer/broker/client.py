import umsgpack
from twisted.python import log
from txzmq import ZmqREQConnection, ZmqEndpoint, ZmqFactory

from brainer.lib.mixins import SerializerMixin


class BrokerClient(ZmqREQConnection, SerializerMixin):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        """
        self._debug = kwargs.pop('debug', False)
        self._serializer = kwargs.pop('serializer', umsgpack)

        super(BrokerClient, self).__init__(*args, **kwargs)

    def sendMsg(self, message):
        """
        """
        d = super(BrokerClient, self).sendMsg(
            self.pack(message))
        d.addCallback(self.gotMessage, message)
        return d

    def register(self, node_id, address):
        """
        """
        self.id = node_id
        message = {'action': 'register', "id": self.id, "address": address}
        if self._debug:
            log.msg('Sending register: {}'.format(message))
        d = self.sendMsg(message)
        return d

    def register_reply(self, message, request):
        """Deals with the reply for the register packet. Pass along
        the reply for any future callback attached to the request.
        """
        return message

    def unregister(self):
        """
        """
        message = {
            "action": "unregister", "id": self.id}
        if self._debug:
            log.msg('Sending unregister: {}'.format(message))

        d = self.sendMsg(message)
        return d

    def gotMessage(self, reply, request):
        """Called when any reply arrives after a request. Routes
        it to correct method. Example: if we receive a reply with
        'register' action, we route it to a method called register_reply.

        :param reply: Non-unpacked reply from server.
        :param request: The original request message.
        """
        reply = self.unpack(reply[0])
        if self._debug:
            log.msg("New Reply: {} ... For request: {}".format(reply, request))

        action = reply['action']
        method = getattr(self, '{}_reply'.format(action), None)
        if method is None:
            log.err('Reply method {} not implemented'.format(action))
            return

        return method(reply, request)

    @classmethod
    def create(cls, host, debug=False):
        """Factory that returns a BrokerClient class.

        :param host: A host.
        :param debug: If True, will log debug messages.
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('connect', host)
        return cls(factory, endpoint, debug=debug)

