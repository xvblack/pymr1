from .mapred_taskbase import MapRedTaskBase, mapred_thrift
from thriftpy.rpc import make_server
from pathlib import Path
import thriftpy
import time
import IPython

class MapOutputInfo:

	def __init__(self):
		self.finished = False

	def mark_finished(self, output_info):
		self.finished = True
		self.output_info = output_info

class MapTaskConf:

	def __init__(self, conf):
		self.conf = conf
		self.progress = 0.0
		self.outputs = [MapOutputInfo() for i in range(int(conf["reduce_count"]))]

	def id(self):
		return self.conf["job_id"]

class ReduceTaskConf:

	def __init__(self, conf):
		self.conf = conf
		self.progress = 0.0

	def id(self):
		return self.conf["job_id"]

class MapRedMasterTask(MapRedTaskBase):

	def __init__(self, container, conf):
		MapRedTaskBase.__init__(self, container, conf)
        
		# add mapred server
		self.working_dir = Path(conf["dir"])
		self.job_id = conf["job_id"]
		self.service_id = self.container.add_service("mapred", mapred_thrift.MapRedMaster, self)
		self.logger.debug("mapred master registered")

	def run_task(self, task_conf, zip):
		self.reduce_count = int(task_conf["reduce_count"])
		self.input_paths = task_conf["input_paths"].split(" ")
		self.zip = zip
		self.start()

	def report_progress(self, map_info, progress):
		map_conf = self.get_map_conf(map_info)
		map_conf.progress = progress

	def register_output(self, map_info, reduce_id, output_info):
		pass

	def mark_finished_output(self, map_info, reduce_id, output_info):
		map_conf = self.get_map_conf(map_info)
		map_conf.outputs[reduce_id].mark_finished(output_info)

	def get_map_conf(self, map_info):
		input_path = map_info["job_id"]
		map_conf = self.map_confs[input_path]
		return map_conf

	def transpose(self, map_confs, reduce_confs):
		for i, reduce_conf in enumerate(reduce_confs):
			reduce_conf.outputs = [map_conf.outputs[i] for map_conf in map_confs]


	def run(self):
		resource_node = self.container.connect_resource_node()
		map_confs = []
		reduce_confs = []

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
				"master-endpoint" : self.endpoint().serialize()
			}

			reduce_conf = ReduceTaskConf(reduce_conf_dict)
			reduce_confs.append(reduce_conf)

		self.transpose(map_confs, reduce_confs)

		self.map_confs = map_confs
		self.reduce_confs = reduce_confs

		self.logger.debug("%s" % self.reduce_confs)

		map_confs = dict([(map_conf.id(), map_conf) for map_conf in map_confs])

		for map_conf in self.map_confs:

			container_info = resource_node.allocate_node_container()
			container = self.container.connect_remote_container(container_info)
			container.run_task(map_conf.conf, self.zip)

		for reduce_conf in self.reduce_confs:

			container_info = resource_node.allocate_node_container()
			container = container.connect_remote_container(container_info)
			container.run_task(reduce_conf.conf, self.zip)

		while True:
			time.sleep(5)


	def get_map_output_locations(self, reduce_id):
		return mapred_thrift.Location("aka", "rin")