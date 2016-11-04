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
import connection

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from bank.ttypes import (BranchID, TransferMessage, LocalSnapshot, SystemException)

def readBranchIds(filename):
    branchIds = []
    with open(filename) as f:
        for line in f.readlines():
            name, ip, port = line.split()
            branchIds.append(BranchID(name, ip, int(port)))
    return branchIds


def initBranches(balance, branchCons, branchIds):
    perBranchBalance = balance / len(branchIds)

    for branchCon in branchCons[:-1]:
        branchCon.client.initBranch(
            perBranchBalance,
            [branchId for branchId in branchIds if branchId != branchCon.branchId]
        )

    lastBranchCon = branchCons[-1]
    lastBranchCon.client.initBranch(
        balance - (perBranchBalance * (len(branchIds) - 1)),
        [branchId for branchId in branchIds if branchId != lastBranchCon.branchId]
    )



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Distributed Bank Application Controller')
    parser.add_argument(dest='balance', help='Initial Balance', type=int)
    parser.add_argument(dest='filename', help='Branch Details')
    args = parser.parse_args()

    branchIds = readBranchIds(args.filename)
    branchCons = connection.getBranchCons(branchIds)
    initBranches(args.balance, branchCons, branchIds)
    # snapshotId = 0
    # while True:
    #     time.sleep(3)
    #     randomBranch = branchCons[random.randint(0, len(branchCons) - 1)]
    #     snapshotId += 1
    #     randomBranch.client.initSnapshot(snapshotId)
    #     snapshots = []
    #     time.sleep(3)
    #     for branch in branchCons:
    #         snapshot = branch.retrieveSnapshot(snapshotId)
    #         snapshots.append(snapshot)
