# -*- coding: utf8 -*-


class SerializerMixin(object):
    """
    """
    def _unpack(self, message):
        """
        """
        return self._serializer.loads(message)

    def _pack(self, message):
        """
        """
        return self._serializer.dumps(message)
