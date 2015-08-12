# -*- coding: utf8 -*-
import sys

import zmq
import umsgpack


class Brainer(object):
    """This the Brainer client.
    """
    def __init__(self, address):
        """
        :param host: The host.
        """
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(address)

    def connect(self):
        """Connects to Brainer server.
        """
        pass

    def _request(self, message):
        self.socket.send(umsgpack.dumps(message))
        return self.socket.recv()

    def get(self, key):
        """Retrieves the value of a key from Brainer servers.
        If key doesn't exist, returns None.

        :param key: A string key.
        """
        data = {"action": "get", "key": key}
        reply = self._request(data)

    def set(self, key, value):
        """Binds value to a key on Brainer servers.
        """
        data = {"action": "set", "key": key, "value": value}
        reply = self._request(data)

    def remove(self, key):
        data = {"action": "remove", "key": key}
        reply = self._request(data)


def main():
    address = sys.argv[1]
    action = sys.argv[2]
    key = sys.argv[3]

    try:
        value = sys.argv[4]
    except IndexError:
        value = None

    allowed_actions = ('get', 'set', 'remove')
    if action not in allowed_actions:
        print 'Invalid action.'
        sys.exit(1)

    client = Brainer(address)
    method = getattr(client, action)
    args = [key]
    if action == 'set':
        args.append(value)

    method(*args)


if __name__ == '__main__':
    main()
