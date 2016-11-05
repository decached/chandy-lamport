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
bLock = threading.Lock()
cLock = threading.Lock()
mLock = threading.Lock()
fifo = threading.Condition(cLock)

channels = {}
lastSentMsg = {}
lastSeenMsg = {}


class State:
    def __init__(self, snapshotId, balance, channels={}):
        self.snapshotId = snapshotId
        self.balance = balance
        self.channels = channels


def transactioner():
    """
    Sends money from this branch to all other branches, by randomly selecting
    one branch at a time. Infinitely loops in a Thread.
    """
    global myBalance, myBranchId, branchCons, lastSentMsg

    while True:
        time.sleep(random.randint(1, 5))
        randomBranchCon = branchCons[random.randint(0, len(branchCons) - 1)]
        with bLock and mLock:
            amount = myBalance * random.randint(1, 5) / 100
            myBalance -= amount
            lastSentMsg[randomBranchCon.branchId.name] += 1
            randomBranchCon.client.transferMoney(TransferMessage(myBranchId, amount), lastSentMsg[randomBranchCon.branchId.name])


def sendMarkers(snapshotId):
    """
    Sends Marker messages to all other branches. Will be invoked from
    `initSnapshot` and `Marker`
    """
    global myBranchId, branchCons, lastSentMsg
    with mLock:
        for branchCon in branchCons:
            lastSentMsg[branchCon.branchId.name] += 1
            branchCon.client.Marker(myBranchId, snapshotId, lastSentMsg[branchCon.branchId.name])


class BankHandler:
    def initBranch(self, initBalance, allBranches):
        """
        Set initial balance and initiate thread for random money transfers
        """
        global myBalance, branchIds, branchCons, lastSeenMsg, lastSentMsg

        myBalance = initBalance
        branchIds = allBranches
        branchCons = connection.getBranchCons(allBranches)

        for branchId in allBranches:
            lastSeenMsg[branchId.name] = 0
            lastSentMsg[branchId.name] = 0
            channels[branchId.name] = {"record": False, "amounts": {}}

        threading.Thread(target=transactioner).start()

    def transferMoney(self, transferMessage, incomingMsgId):
        """
        To increment balance as this is the money received from other branches
        """
        global myBalance, lastSeenMsg, channels

        # To simulate a delayed message, uncomment the following lines. It will
        # delay messageIds of multiple 3 or 5
        # if incomingMsgId % 3 == 0 or incomingMsgId % 5 == 0:
        #     time.sleep(3)

        with fifo:
            while lastSeenMsg[transferMessage.orig_branchId.name] < incomingMsgId - 1:
                fifo.wait()

            if channels[transferMessage.orig_branchId.name]["record"]:
                channels[transferMessage.orig_branchId.name]["amounts"][str(incomingMsgId)] = transferMessage.amount

            with bLock:
                myBalance += transferMessage.amount

            lastSeenMsg[transferMessage.orig_branchId.name] = incomingMsgId
            fifo.notify_all()

    def initSnapshot(self, snapshotId):
        """
        Saves local state and sends out Marker messages. Starts recording on
        all incoming channels.

        """
        global myBalance, lastSentMsg, branchIds, channels

        with bLock:
            state = State(snapshotId, myBalance, {
                branchId.name:[] for branchId in branchIds
            })
            states[str(snapshotId)] = state

        with cLock:
            # Start recording on all incoming channels
            for branchId in branchIds:
                channels[branchId.name]["record"] = True
                channels[branchId.name]["amounts"] = {}

        sendMarkers(snapshotId)

    def Marker(self, incomingBranchId, snapshotId, incomingMsgId):
        """
        One receiving 1st Marker message, saves its local state and sends out
        Marker messages to other branches. Sets currently incoming channel to
        [] and starts recording on all other incoming channels.

        One receiving non-1st Marker message(s), saves channels to state, and
        stops recoding current incoming channel.

        """
        global myBalance, lastSeenMsg, branchCons, channels

        # To simulate a delayed message, uncomment the following lines. It will
        # delay messageIds of multiple 3 or 5
        # if incomingMsgId % 3 == 0 or incomingMsgId % 5 == 0:
        #     time.sleep(3)

        with fifo:
            while lastSeenMsg[incomingBranchId.name] < incomingMsgId - 1:
                fifo.wait()

            state = None
            if str(snapshotId) in states:
                state = states[str(snapshotId)]
                state.channels[incomingBranchId.name] = [v for k, v in channels[incomingBranchId.name]["amounts"].iteritems()]
                states[str(snapshotId)] = state
                channels[incomingBranchId.name]["record"] = False
            else:
                with bLock:
                    state = State(snapshotId, myBalance, {
                        branchId.name:[] for branchId in branchIds
                    })
                    # Start recording on all incoming channels
                    for branchId in branchIds:
                        channels[branchId.name]["record"] = True
                        channels[branchId.name]["amounts"] = {}
                    # Mark current incoming channel `empty`
                    state.channels[incomingBranchId.name] = []
                    states[str(snapshotId)] = state

                threading.Thread(target=sendMarkers, args=(snapshotId,)).start()

            lastSeenMsg[incomingBranchId.name] = incomingMsgId
            fifo.notify_all()

    def retrieveSnapshot(self, snapshotId):
        """
        Returns `LocalSnapshot` object.
        """
        global branchIds

        state = states[str(snapshotId)]
        msgs = []
        for channel, amounts in state.channels.iteritems():
            msgs.extend(amounts)

        return LocalSnapshot(snapshotId=state.snapshotId, balance=state.balance, messages=msgs)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Global Snapshot of a Distributed Bank Application')
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
