import pyzmq


class Brainer(object):
    """This the Brainer client.
    """
    def __init__(self, host):
        """
        :param host: The host.
        """
        pass

    def connect(self):
        """Connects to Brainer server.
        """
        pass

    def get(self, key):
        """Retrieves the value of a key from Brainer servers.
        If key doesn't exist, returns None.

        :param key: A string key.
        """
        pass

    def set(self, key, value):
        """Binds value to a key on Brainer servers.
        """
        pass

    def remove(self, key):
        pass
