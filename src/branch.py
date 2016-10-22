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
import socket
import sys
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from bank import Branch
from bank.ttypes import BranchID, TransferMessage, LocalSnapshot, SystemException
from controller import Controller

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

balance = None
lock = threading.Lock()


def setBalance(amount, op="new"):
    with lock:
        if op == "new":
            balance = amount
        elif op == "inc":
            balance += amount
        elif op == "dec":
            balance -= amount


def getBalance():
    with lock:
        return balance


class BankHandler:

    def __init__(self):
        self.branchIds = []
        self.messageId = 0

    def initBranch(self, balance, all_branches):
        setBalance(balance, op="new")
        self.branchIds = all_branches

    def transferMoney(self, transferMessage, messageId):
        setBalance(transferMessage.amount, op="dec")

    def initSnapshot(self, snapshotId):
        for branchId in self.branchIds:
            branchCtrl = Controller(branchId.ip, branchId.port)
            branchCtrl.client.Marker(branchId, snapshotId, self.nextMessageId())

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
        server.serve()

    except Thrift.TException as tx:
        print('%s' % (tx.message))
