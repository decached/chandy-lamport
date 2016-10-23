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

branchId = None
balance = None
branchIds = []
branchCons = []
messageId = 0


class Balance:
    def __init__(self, value):
        self.value = value
        self.__lock = threading.Lock()

    def decrement(self, percent):
        with self.__lock:
            amount = 1
            # amount = self.value * percent / 100
            self.value -= amount
            return amount

    def increment(self, amount):
        with self.__lock:
            self.value += amount


def transactioner():
    global branchId, branchCons

    while True:
        time.sleep(random.randint(1, 5))
        amount = balance.decrement(random.randint(1, 5))
        randomBranchCon = branchCons[random.randint(0, len(branchCons) - 1)]
        randomBranchCon.client.transferMoney(TransferMessage(branchId, amount), messageId)


class BankHandler:
    def initBranch(self, initBalance, allBranches):
        global balance, branchIds, branchCons
        balance = Balance(initBalance)
        branchIds = allBranches
        branchCons = connection.getBranchCons(branchIds)
        threading.Thread(target=transactioner).start()

    def transferMoney(self, transferMessage, messageId):
        balance.increment(transferMessage.amount)
        print balance.value

    def initSnapshot(self, snapshotId):
        for branchId in branchIds:
            branchCon = connection.Connection(branchId)
            branchCon.client.Marker(branchId, snapshotId, self.nextMessageId())

    def Marker(self, branchId, snapshotId, messageId):
        print branchId, snapshotId, messageId

    def retrieveSnapshot(self, snapshotId):
        pass

    def nextMessageId(self):
        self.messageId = self.messageId + 1
        return self.messageId


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

        branchId = BranchID(args.name, host, int(args.port))
        server.serve()

    except Thrift.TException as tx:
        print('%s' % (tx.message))
