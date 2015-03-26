from .mapred_taskbase import MapRedTaskBase, mapred_thrift
from thriftpy.rpc import make_server
from mr1.rpc import ThriftEndPoint
from pathlib import Path
import thriftpy
import time
import json
import mr1.utility as utility
import atomic

class ShuffleOutputInfo:

	def __init__(self, map_id, reduce_id, finished=False, output_info=None):
		self.map_id = map_id
		self.reduce_id = reduce_id
		self.finished = finished
		self.output_info = output_info

	def mark_finished(self):
		assert self.output_info is not None
		self.finished = True

	def to_dict(self):
		info = {
			"map_id" : self.map_id,
			"reduce_id" : self.reduce_id,
			"finished" : self.finished,
			"output_info" : json.dumps(self.output_info)
		}

		return utility.all_string_dict(info)

	@classmethod
	def from_dict(klass, dict):
		map_id = dict["map_id"]
		reduce_id = int(dict["reduce_id"])
		finished = dict["finished"] == "True"
		output_info = json.loads(dict["output_info"])
		return klass(map_id, reduce_id, finished, output_info)


class MapTaskConf:

	def __init__(self, conf):
		self.conf = conf
		self.progress = 0.0
		self.outputs = [ShuffleOutputInfo(conf["job_id"], i) for i in range(int(conf["reduce_count"]))]

	def id(self):
		return self.conf["job_id"]

class ReduceTaskConf:

	def __init__(self, conf):
		self.conf = conf
		self.progress = 0.0
		self.outputs = None

	def id(self):
		return self.conf["job_id"]

class MapRedMasterTask(MapRedTaskBase):

	def __init__(self, container, conf):
		MapRedTaskBase.__init__(self, container, conf)
        
		# add mapred server
		self.working_dir = utility.Directory(conf["work_dir"])
		self.service_id = self.container.add_service("mapred", mapred_thrift.MapRedMaster, self)
		self.logger.debug("mapred master registered")

	def run_task(self, task_conf, zip):
		self.logger.debug("task configuration: %s" % task_conf)

		self.task_conf = task_conf
		self.job_id = task_conf["job_id"]
		self.reduce_count = int(task_conf["reduce_count"])
		self.input_paths = task_conf["input_paths"].split(" ")
		self.zip = zip
		self.start()

	def report_progress(self, map_info, progress):
		map_conf = self.get_map_conf(map_info)
		map_conf.progress = progress

	def register_output(self, map_info, reduce_id, output_info):
		self.logger.debug("Output registered %s %s %s" % (map_info, reduce_id, output_info))
		map_conf = self.get_map_conf(map_info)
		map_conf.outputs[reduce_id].output_info = output_info
		pass

	def report_map_finished(self, map_info):
		map_conf = self.get_map_conf(map_info)
		for reduce_id in range(int(map_conf.conf["reduce_count"])):
			map_conf.outputs[reduce_id].mark_finished()

	def report_reduce_finished(self, reduce_info):
		reduce_id = int(reduce_info["reduce_id"])
		self.reduce_finished += 1

	def get_all_outputs(self, reduce_conf):
		reduce_id = int(reduce_conf["reduce_id"])
		local_conf = self.reduce_confs[reduce_id]
		infos = [info.to_dict() for info in local_conf.outputs]
		self.logger.debug("output_files: %s" % infos[0])
		return infos

	def get_map_conf(self, map_info):
		input_path = map_info["job_id"]
		map_conf = self.map_confs_dict[input_path]
		return map_conf

	def transpose(self, map_confs, reduce_confs):
		for i, reduce_conf in enumerate(reduce_confs):
			reduce_conf.outputs = [map_conf.outputs[i] for map_conf in map_confs]

	def run(self):
		resource_node = self.container.connect_resource_node()
		map_confs = []
		reduce_confs = []

		self.reduce_finished = atomic.AtomicLong(0)

		for i, path in enumerate(self.input_paths):
			map_job_id = "_".join([self.job_id, "map", str(i)])
			map_conf_dict = {
				"type" : "map",
				"job_id" : map_job_id,
				"input" : path,
				"reduce_count" : str(self.reduce_count),
				"master-endpoint" : self.endpoint().serialize()
			}

			map_conf = MapTaskConf(map_conf_dict)
			map_confs.append(map_conf)

		for i in range(self.reduce_count):
			reduce_job_id = "_".join([self.job_id, "reduce", str(i)])

			reduce_conf_dict = {
				"type" : "reduce",
				"job_id" : reduce_job_id,
				"reduce_id" : str(i),
				"master-endpoint" : self.endpoint().serialize()
			}

			reduce_conf = ReduceTaskConf(reduce_conf_dict)
			reduce_confs.append(reduce_conf)

		self.transpose(map_confs, reduce_confs)

		self.map_confs = map_confs
		self.reduce_confs = reduce_confs

		self.logger.debug("reduce_confs: %s" % self.reduce_confs)

		self.logger.debug("%s" % self.reduce_confs)

		map_confs_dict = dict([(map_conf.id(), map_conf) for map_conf in map_confs])
		self.map_confs_dict = map_confs_dict

		for map_conf in self.map_confs:

			container_info = resource_node.allocate_node_container()
			endpoint = ThriftEndPoint.deserialize(container_info)
			container = self.container.connect_remote_container(endpoint)
			container.run_task(map_conf.conf, self.zip)

		for reduce_conf in self.reduce_confs:

			container_info = resource_node.allocate_node_container()
			endpoint = ThriftEndPoint.deserialize(container_info)
			container = self.container.connect_remote_container(endpoint)
			container.run_task(reduce_conf.conf, self.zip)

		while self.reduce_finished != self.reduce_count:
			print self.daemon
			time.sleep(5)

		self.logger.info("mapred %s finished" % self.task_conf["job_id"])


	def get_map_output_locations(self, reduce_id):
		return mapred_thrift.Location("aka", "rin")