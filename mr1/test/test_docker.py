# import sys, os
#
# sys.path.append(os.path.join(os.getcwd(), "thriftpy"))

from mr1.container import Container
from mr1.mapred import MapRedMasterTask, mapred_thrift
from mr1.fs import ThriftLocalFileService, fs_thrift
from thriftpy.rpc import client_context
import threading, sys, time
import mr1.utility as utility
import unittest

thrift_conf = {
	"host" : "127.0.0.1",
	"port" : "8081"
}

resource_node_conf = {
	"type" : "here"
}

conf = {
	"thrift" : thrift_conf,
	"work_dir" : "/tmp/container",
	"resource_node" : resource_node_conf
}

docker_task_conf = {
	"type" : "docker",
	"job_id" : "docker_1",
	"server" : "boot2docker"
}

utility.setup_logging()

class TestCodeDist(unittest.TestCase):

	def setUp(self):
		self.container = Container(conf)
		fs = ThriftLocalFileService(self.container, {})
		self.container.start()
		self.container.add_service("thrift-fs", fs_thrift.FileSystem, fs, unique=True)

	def test_run(self):
		self.assertEqual(self.container.get_service("resource_node").endpoint().serialize(), "127.0.0.1:8081/resource_node")
		self.container.run_task(docker_task_conf, "")
		while True:
			time.sleep(100)

if __name__ == '__main__':
	unittest.main()