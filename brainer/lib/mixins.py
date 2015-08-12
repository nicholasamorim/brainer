# -*- coding: utf8 -*-


class SerializerMixin(object):
    """This is a Mixin that provides pack and unpack methods.

    This class expects the property _serializer to be set in your
    class. The serializer must support `loads` and `dumps` interface,
    like json or umsgpack.

    Usage:

    class MyClass(object, SerializerMixin):
        _serializer = umsgpack

        def send(self, data):
            packed_data = self.pack(data)
            # do something...
    """
    def unpack(self, message):
        """Deserializes a message.

        :param message: Your serialized data.
        """
        return self._serializer.loads(message)

    def pack(self, message):
        """Serializes a message.

        :param message: Your raw data.
        """
        return self._serializer.dumps(message)
