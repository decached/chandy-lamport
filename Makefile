gen-py:
	thrift -r -o lib --gen py lib/bank.thrift
