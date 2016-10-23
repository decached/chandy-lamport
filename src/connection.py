#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 akash <akothaw1@binghamton.edu>

"""

"""
import glob
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from bank import Branch


class Connection:
    def __init__(self, branchId):
        self.branchId = branchId
        transport = TSocket.TSocket(branchId.ip, branchId.port)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Branch.Client(protocol)
        transport.open()


def getBranchCons(branchIds):
    branchCons = []
    for branchId in branchIds:
        branchCons.append(Connection(branchId))
    return branchCons
