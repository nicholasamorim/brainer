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

    def get_node_for_key(self, key):
        """Uses consistent hashing (or try hard...) to get the right node
        for given key

        :param key: A key string.
        """
        pass
