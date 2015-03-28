# import sys, os
#
# sys.path.append(os.path.join(os.getcwd(), "thriftpy"))

from mr1.container import Container
from mr1.mapred import MapRedMasterTask, mapred_thrift
from mr1.fs import ThriftLocalFileService, fs_thrift
from thriftpy.rpc import client_context
import threading, sys, time
import mr1.utility as utility
from pathlib import Path
import unittest

thrift_conf = {
	"host" : "127.0.0.1",
	"port" : "8081"
}

input_path = str(Path("resources/input/space_numbers.txt").resolve())

mapred_master_conf = {
	"type" : "mapred-master",
	"job_id" : "mapred1",
	"reduce_count" : "1",
	"input_paths" : input_path
}

mapred_master_conf2 = {
	"type" : "mapred-master",
	"job_id" : "mapred2",
	"reduce_count" : "5",
	"input_paths" : input_path
}

resource_node_conf = {
	"type" : "here"
}

conf = {
	"thrift" : thrift_conf,
	"work_dir" : "/tmp/container",
	"resource_node" : resource_node_conf
}

utility.setup_logging()

code = """

def mapper(key, value):
	print key, value
	return key, value

def reducer(key, values):
	return key, len(values)

"""


class TestCodeDist(unittest.TestCase):

	def setUp(self):
		self.container = Container(conf)
		fs = ThriftLocalFileService(self.container, {})
		self.container.start()
		self.container.add_service("thrift-fs", fs_thrift.FileSystem, fs, unique=True)

	def test_run(self):
		self.assertEqual(self.container.get_service("resource_node").endpoint().serialize(), "127.0.0.1:8081/resource_node")
		self.container.run_task(mapred_master_conf, code)
		while True:
			time.sleep(100)

if __name__ == '__main__':
	unittest.main()