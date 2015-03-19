import thriftpy
from mr1.task_base import TaskBase


mapred_thrift = thriftpy.load("resources/mapred.thrift", module_name="mapred_thrift")

class MapRedTaskBase(TaskBase):

	def get_mapred_master(self):
		master_conf = self.conf["master"]