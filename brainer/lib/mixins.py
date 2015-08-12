# -*- coding: utf8 -*-


class SerializerMixin(object):
    """
    """
    def unpack(self, message):
        """
        """
        return self._serializer.loads(message)

    def pack(self, message):
        """
        """
        return self._serializer.dumps(message)
