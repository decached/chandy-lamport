#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Akash Kothawale <akothaw1@binghamton.edu>

"""

"""

import argparse
import glob
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from bank import Branch
from bank.ttypes import BranchID, TransferMessage, LocalSnapshot, SystemException

from thrift.Thrift import TType
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TJSONProtocol


class Controller:
    def __init__(self, ip, port):
        self.transport = TSocket.TSocket(ip, port)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Branch.Client(protocol)
        self.transport.open()

    def deinit(self):
        self.transport.close()

    def initBranch(self, balance, branches):
        self.client.initBranch(balance, branches)

    def initSnapshot(self, snapshotId):
        pass

    def Marker(self, branchId, snapshotId, messageId):
        pass

    def retrieveSnapshot(self, snapshotId):
        pass


def readBranchDetails(filename):
    branches = []
    with open(filename) as f:
        for line in f.readlines():
            name, ip, port = line.split()
            branches.append(BranchID(name, ip, int(port)))
    return branches


def getBranches(branches, filename):
    branches = readBranchDetails(args.filename)
    branchCtrls = []
    for branch in branches:
        branchCtrls.append(Controller(branch.ip, branch.port))
    return branchCtrls


def initBranches(branches, balance):
    for branch in branches:
        branch.initBranch(balance, branches)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed Bank Application Controller')
    parser.add_argument(dest='balance', help='Initial Balance', type=int)
    parser.add_argument(dest='filename', help='Branch Details')

    args = parser.parse_args()
    branches = getBranches(args.filename)
    initBranches(branches, args.balance)
