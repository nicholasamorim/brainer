# -*- coding: utf8 -*-
from mock import MagicMock, patch
from twisted.trial import unittest

from brainer.client import Brainer


class BrainerClientTest(unittest.TestCase):
    """Making sure that if someone changes the base implementation (or lack of it)
    it will be accused in the tests.
    """
    def setUp(self):
        self.mock_zmq = MagicMock()
        self.patcher = patch('brainer.client.zmq', self.mock_zmq)
        self.patcher.start()
        self.address = "What's Your Frequency, Kenneth ?"
        self.brainer = Brainer(self.address)

    def tearDown(self):
        self.patcher.stop()

    def test_connect(self):
        self.brainer.connect()
        socket = self.mock_zmq.Context().socket()
        socket.connect.assert_called_once_with(self.address)

    def test_get(self):
        self.assertRaises(TypeError, self.brainer.get)  # no key
        with patch('brainer.client.Brainer._request') as mock_request:
            self.brainer.get('keytest')
            mock_request.assert_called_once_with(
                {"key": "keytest", "action": "get"})

    def test_set(self):
        self.assertRaises(TypeError, self.brainer.set)  # no params
        self.assertRaises(TypeError, self.brainer.set, 'key')  # no value

        with patch('brainer.client.Brainer._request') as mock_request:
            self.brainer.set('keytest', 'valuetest')
            mock_request.assert_called_once_with({
                "key": "keytest", "action": "set",
                "value": "valuetest", "wait_all": True})

            self.brainer.set('keytest2', 'valuetest2', wait_all=False)
            mock_request.assert_called_with({
                "key": "keytest2", "action": "set",
                "value": "valuetest2", "wait_all": False})

    def test_remove(self):
        self.assertRaises(TypeError, self.brainer.remove)  # no key
        with patch('brainer.client.Brainer._request') as mock_request:
            self.brainer.remove('keytest')
            mock_request.assert_called_once_with(
                {"key": "keytest", "action": "remove"})
