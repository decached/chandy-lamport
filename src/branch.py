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

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/lib/gen-py')
sys.path.insert(0, glob.glob('/home/akash/clones/thrift/lib/py/build/lib.*')[0])

from bank import Branch
from bank.ttypes import BranchID, TransferMessage, LocalSnapshot, SystemException

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


class BankHandler():

    def __init__(self):
        self.balance = None
        self.branches = []

    def initBranch(self, balance, all_branches):
        self.balance = balance
        self.branches = all_branches

    def transferMoney(self, message, messageId):
        pass

    def initSnapshot(self, snapshotId):
        pass

    def Marker(self, branchId, snapshotId, messageId):
        pass

    def retrieveSnapshot(self, snapshotId):
        pass


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
