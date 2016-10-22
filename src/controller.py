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
import random
import sys
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from bank import Branch
from bank.ttypes import BranchID, TransferMessage, LocalSnapshot, SystemException

from thrift.Thrift import TType
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class Controller:
    def __init__(self, ip, port):
        self.transport = TSocket.TSocket(ip, port)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Branch.Client(protocol)
        self.transport.open()

    def deinit(self):
        self.transport.close()


def readBranchDetails(filename):
    branchIds = []
    with open(filename) as f:
        for line in f.readlines():
            name, ip, port = line.split()
            branchIds.append(BranchID(name, ip, int(port)))
    return branchIds


def getBranches(filename):
    branchIds = readBranchDetails(args.filename)
    branchCtrls = []
    for branchId in branchIds:
        branchCtrls.append(Controller(branchId.ip, branchId.port))
    return branchIds, branchCtrls


def initBranches(balance, branchCtrls, branchIds):
    for branch in branchCtrls:
        branch.client.initBranch(balance, branchIds)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed Bank Application Controller')
    parser.add_argument(dest='balance', help='Initial Balance', type=int)
    parser.add_argument(dest='filename', help='Branch Details')

    args = parser.parse_args()
    branchIds, branchCtrls = getBranches(args.filename)
    initBranches(args.balance, branchCtrls, branchIds)
    snapshotId = 0
    while True:
        time.sleep(3)
        randomBranch = branchCtrls[random.randint(0, len(branchCtrls) - 1)]
        snapshotId += 1
        randomBranch.client.initSnapshot(snapshotId)
