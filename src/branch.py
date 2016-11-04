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
myBalance = 0
branchIds = []
branchCons = []

states = {}
cLock = threading.Lock()
bLock = threading.Lock()
mLock = threading.Lock()
cond = threading.Condition()

channels = {}
lastSentMsg = {}
lastSeenMsg = {}


class State:
    def __init__(self, snapshotId, balance, channels={}):
        self.snapshotId = snapshotId
        self.balance = balance
        self.channels = channels


def transactioner():
    global myBalance, myBranchId, branchCons, lastSentMsg

    while True:
        time.sleep(random.randint(1, 1))
        # time.sleep(random.randint(1, 5))
        randomBranchCon = branchCons[random.randint(0, len(branchCons) - 1)]
        with bLock and mLock:
            lastSentMsg[randomBranchCon.branchId.name] += 1
            amount = myBalance * random.randint(1, 5) / 100
            myBalance -= amount
            randomBranchCon.client.transferMoney(TransferMessage(myBranchId, amount), lastSentMsg[randomBranchCon.branchId.name])


class BankHandler:
    def initBranch(self, initBalance, allBranches):
        global myBalance, myBranchIds, branchCons, transfers, lastSeenMsg, lastSentMsg

        myBalance = initBalance
        myBranchIds = allBranches
        branchCons = connection.getBranchCons(myBranchIds)

        for branchId in allBranches:
            lastSeenMsg[branchId.name] = 0
            lastSentMsg[branchId.name] = 0
            channels[branchId.name] = {}

        threading.Thread(target=transactioner).start()

    def transferMoney(self, transferMessage, incomingMsgId):
        global myBalance, lastSeenMsg

        channels[transferMessage.orig_branchId.name][incomingMsgId] = transferMessage.amount
        if not (incomingMsgId % 3) or not (incomingMsgId % 5):
            time.sleep(2)

        with cond:
            while lastSeenMsg[transferMessage.orig_branchId.name] < incomingMsgId - 1:
                cond.wait()

            with bLock:
                myBalance += transferMessage.amount

            print 'Transfer', transferMessage.orig_branchId.name, transferMessage.amount
            del channels[transferMessage.orig_branchId.name][incomingMsgId]
            lastSeenMsg[transferMessage.orig_branchId.name] = incomingMsgId

            cond.notify_all()

    def initSnapshot(self, snapshotId):
        global myBalance, lastSentMsg

        with bLock:
            state = State(snapshotId=snapshotId, balance=myBalance)

        for branchCon in branchCons:
            with mLock:
                lastSentMsg[branchCon.branchId.name] += 1
                branchCon.client.Marker(myBranchId, snapshotId, lastSentMsg[branchCon.branchId.name])

        states[str(snapshotId)] = state

    def Marker(self, incomingBranchId, incomingSnapshotId, incomingMsgId):
        global lastSeenMsg, branchCons
        if not (incomingMsgId % 3) or not (incomingMsgId % 5):
            time.sleep(2)
        with cond:
            while lastSeenMsg[incomingBranchId.name] < incomingMsgId - 1:
                cond.wait()

            lastSeenMsg[incomingBranchId.name] = incomingMsgId

            state = None
            try:
                state = states[str(incomingSnapshotId)]
                state.channels[incomingBranchId.name] = channels[incomingBranchId.name].values()
            except KeyError:
                with bLock:
                    state = State(snapshotId=incomingSnapshotId, balance=myBalance)
                    state.channels[incomingBranchId.name] = []
                    for branchCon in branchCons:
                        with mLock:
                            lastSentMsg[branchCon.branchId.name] += 1
                            branchCon.client.Marker(myBranchId, incomingSnapshotId, lastSentMsg[branchCon.branchId.name])

            print 'Marker', incomingBranchId.name

            states[str(incomingSnapshotId)] = state
            cond.notify_all()

    def retrieveSnapshot(self, snapshotId):
        global branchIds

        state = states[str(snapshotId)]
        snapshot = LocalSnapshot(state.snapshotId, state.balance)

        for branchId in branchIds:
            print 'Snapshot:', branchId.name, state.channels[branchId.name]

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
