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
import socket
import sys
import threading
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from bank import Branch
from bank.ttypes import BranchID, TransferMessage, LocalSnapshot, SystemException
import connection

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

myBranchId = None
myBalance = None
branchIds = []
branchCons = []
myMessageId = 0

states = {}
bLock = threading.Lock()
mLock = threading.Lock()

transfers = {}


class State:
    def __init__(self, snapshotId, balance, startId, channels=[]):
        self.snapshotId = snapshotId
        self.balance = balance
        self.channels = channels


class Balance:
    def __init__(self, value):
        self.value = value

    def decrement(self, percent):
        amount = self.value * percent / 100
        self.value -= amount
        return amount

    def increment(self, amount):
        self.value += amount


def transactioner():
    global myBalance, myBranchId, branchCons, myMessageId
    for i in xrange(10):
        time.sleep(random.randint(1, 5))
        randomBranchCon = branchCons[random.randint(0, len(branchCons) - 1)]
        with bLock:
            amount = myBalance.decrement(percent=random.randint(1, 5))
            randomBranchCon.client.transferMoney(TransferMessage(myBranchId, amount), myMessageId)
            fifo(myBranchId.name, amount)


def fifo(name, amount):
    # FIXME: Implement FIFO
    pass


class BankHandler:
    def initBranch(self, initBalance, allBranches):
        global myBalance, myBranchIds, branchCons, transfers
        myBalance = Balance(initBalance)
        myBranchIds = allBranches
        branchCons = connection.getBranchCons(myBranchIds)
        for branchId in allBranches:
            transfers[branchId.name] = []
        threading.Thread(target=transactioner).start()
        print myBranchId.name, '-> initBranch Successful'

    def transferMoney(self, transferMessage, messageId):
        with bLock:
            myBalance.increment(transferMessage.amount)
            print myBalance.value
        fifo(transferMessage.orig_branchId.name, transferMessage.amount)

    def initSnapshot(self, snapshotId):
        global myBalance, myMessageId

        with bLock and mLock:
            state = State(snapshotId=snapshotId, balance=myBalance, startId=myMessageId)

        for branchCon in branchCons:
            with mLock:
                myMessageId += 1
                branchCon.client.Marker(myBranchId, snapshotId, myMessageId)

        states[str(snapshotId)] = state

    def Marker(self, incomingBranchId, incomingSnapshotId, incomingMessageId):
        state = None
        try:
            state = states[str(incomingSnapshotId)]
            # state.channels[(incomingBranchId.name, myBranchId.name)] = transfers[startMessageId:incomingMessageId]
        except KeyError:
            with bLock and mLock:
                state = State(snapshotId=incomingSnapshotId, balance=myBalance, startId=myMessageId)
                state.channels = []
                # state.channels[(incomingBranchId.name, myBranchId.name)] = []

        states[str(incomingSnapshotId)] = state

    def retrieveSnapshot(self, snapshotId):
        global transfers, myBranchIds

        state = states[str(snapshotId)]
        snapshot = LocalSnapshot(state.snapshotId, state.balance.value, state.channels)

        for branchId in myBranchIds:
            transfers[branchId.name] = []

        return snapshot


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Process some integers.')
        parser.add_argument(dest='name', help='Name')
        parser.add_argument(dest='port', help='Port')
        args = parser.parse_args()

        handler = BankHandler()
        processor = Branch.Processor(handler)
        tsocket = TSocket.TServerSocket('0.0.0.0', args.port)
        transport = TTransport.TBufferedTransportFactory()
        protocol = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadedServer(processor, tsocket, transport, protocol)

        host = socket.gethostname()
        host += '.cs.binghamton.edu' if host.startswith('remote') else ''
        print('Bank server running on ' + host + ':' + args.port)

        myBranchId = BranchID(args.name, host, int(args.port))
        server.serve()

    except Thrift.TException as tx:
        print('%s' % (tx.message))
