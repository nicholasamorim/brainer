from lib.hash import ConsistentHash


class NodeManager(object):
    """
    """
    def __init__(self, hashing_class=ConsistentHash, num_replicas=3):
        """
        """
        self._nodes = []
        self._nodes_id = {}
        self._last_ping = {}
        self._num_machines = 0
        self._num_replicas = num_replicas
        self._hashing_class = ConsistentHash

    def register(self, server_id):
        """
        """
        node_number = len(self._nodes) + 1
        self._nodes.append(node_number)
        self._nodes_id[node_number] = server_id
        self._num_machines += 1
        return node_number

    def unregister(self, node, server_id=None):
        """
        """
        self._num_machines -= 1
        self._nodes.remove(node)
        del self._nodes_id[node]

    def get_node_for_key(self, key):
        """Uses consistent hashing (or try hard...) to get the right node
        for given key

        :param key: A key string.
        """
        if self._num_machines == 0:
            return  # raise a Failure here.

        hashing = self._hashing_class(self._num_machines, self._num_replicas)
        return hashing.get_machine(key)
