# MR1.2

A simple map reduce framework

Currently support:

1. Submit job
2. Use share file system

TODO:

1. Add dfs support
	- currenly support a thrift based transport fs
	- not tested distributedly
2. Add multiple node support
3. migrate thread to process based solution
	1. need a process-able processor
	2. or change heavy-load part to multiprocess only
4. check possible race condition

thriftpy is forked from https://github.com/eleme/thriftpy, changed in following parts:
1. allow registering same service with different names
