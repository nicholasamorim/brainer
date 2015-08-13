# -*- coding: utf8 -*-
import sys
from twisted.trial import unittest

if 'brainer' not in sys.path:
    sys.path.append('brainer')

from brainer.broker import Broker


class BrokerTest(unittest.TestCase):
    pass
