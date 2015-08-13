# -*- coding: utf8 -*-
import sys
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.node import Node


class NodeTest(unittest.TestCase):
    pass
