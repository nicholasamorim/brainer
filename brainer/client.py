# -*- coding: utf8 -*-
import sys

import zmq
import umsgpack


class Brainer(object):
    """This the Brainer client.

    Usage:
        >>> client = Brainer('tcp://127.0.0.1:34212)
        >>> client.set('mykey', 'myvalue')
        True
        >>> client.get('mykey')
        'myvalue'
    """
    def __init__(self, address):
        """
        :param host: The host.
        """
        self.address = address
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)

    def connect(self):
        """Connects to Brainer server.
        """
        self.socket.connect(self.address)

    def _request(self, message):
        self.socket.send(umsgpack.dumps(message))
        return umsgpack.loads(self.socket.recv())

    def get(self, key):
        """Retrieves the value of a key from Brainer nodes.
        If key doesn't exist, returns None.

        :param key: A key to retrieve its value.
        :returns: Key value or None if key is not set.
        """
        data = {"action": "get", "key": key}
        reply = self._request(data)
        return reply

    def set(self, key, value, wait_all=True):
        """Binds value to a key on Brainer nodes.

        :param key: A key to pair with the value.
        :param value: The value to be paired with the key.
        :param wait-all: If True, will wait until all nodes has the data.
        """
        data = {
            "action": "set",
            "key": key, "value": value,
            "wait_all": wait_all}
        reply = self._request(data)
        return reply

    def remove(self, key, wait_all=True):
        """Removes a key from the nodes.

        :param key: A key to remove.
        """
        data = {
            "action": "remove",
            "key": key,
            "wait_all": wait_all}
        reply = self._request(data)
        return reply


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
    client.connect()
    method = getattr(client, action)
    args = [key]
    if action == 'set':
        args.append(value)

    print method(*args)


if __name__ == '__main__':
    main()
