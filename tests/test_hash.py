# -*- coding: utf8 -*-
from twisted.trial import unittest


from brainer.lib.hash import ConsistentHash, my_hash


class ConsistentHashTest(unittest.TestCase):
    def test_my_hash(self):
        self.assertEqual(my_hash('key'), 0.989661)
        self.assertEqual(my_hash('key2'), 0.463706)
        self.assertEqual(my_hash('key3'), 0.389136)
        self.assertEqual(my_hash('key4'), 0.308801)
        self.assertEqual(my_hash('key5'), 0.321215)
        self.assertEqual(my_hash('key6'), 0.757958)

    def test_consistent_hash_no_replica(self):
        self.assertRaises(ValueError, ConsistentHash, 1, 0)

    def test_consistent_hash_no_machines(self):
        self.assertRaises(ValueError, ConsistentHash, 0, 2)

    def test_consistent_hash_one_replica(self):
        ch = ConsistentHash(1, 1)

        self.assertEqual(ch.get_machine('key'), 0)
        self.assertEqual(ch.get_machine('key2'), 0)
        self.assertEqual(ch.get_machine('key3'), 0)
        self.assertEqual(ch.get_machine('key4'), 0)
        self.assertEqual(ch.get_machine('key5'), 0)
        self.assertEqual(ch.get_machine('key6'), 0)

    def test_consistent_hash_3_machines_1_replica(self):
        ch = ConsistentHash(3, 1)

        self.assertEqual(ch.get_machine('key'), 1)
        self.assertEqual(ch.get_machine('key2'), 2)
        self.assertEqual(ch.get_machine('key3'), 0)
        self.assertEqual(ch.get_machine('key4'), 0)
        self.assertEqual(ch.get_machine('key5'), 0)
        self.assertEqual(ch.get_machine('key6'), 2)

    def test_consistent_hash_3_machines_3_replicas(self):
        ch = ConsistentHash(3, 3)

        self.assertEqual(ch.get_machine('key'), 1)
        self.assertEqual(ch.get_machine('key2'), 0)
        self.assertEqual(ch.get_machine('key3'), 0)
        self.assertEqual(ch.get_machine('key4'), 1)
        self.assertEqual(ch.get_machine('key5'), 1)
        self.assertEqual(ch.get_machine('key6'), 2)

    def test_consistent_hash_200_machines_20_replicas(self):
        ch = ConsistentHash(200, 20)

        self.assertEqual(ch.get_machine('key'), 93)
        self.assertEqual(ch.get_machine('key2'), 79)
        self.assertEqual(ch.get_machine('key3'), 164)
        self.assertEqual(ch.get_machine('key4'), 141)
        self.assertEqual(ch.get_machine('key5'), 22)
        self.assertEqual(ch.get_machine('key6'), 121)
