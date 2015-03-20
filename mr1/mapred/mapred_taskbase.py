import thriftpy
from mr1.task_base import TaskBase
from mr1.rpc import ThriftEndPoint


mapred_thrift = thriftpy.load("resources/mapred.thrift", module_name="mapred_thrift")

class MapRedTaskBase(TaskBase):

    def __init__(self, container, conf):
        TaskBase.__init__(self, container, conf)

    def convert_task_conf(self):

    	to_convert = []
    	self.local_conf = {}

        for key in self.task_conf:
            if key.endswith("endpoint"):
	            addr = self.task_conf[key]
	            key = key.split("-")[0]

	            self.local_conf[key] = ThriftEndPoint.deserialize(addr)

    def get_mapred_master(self):
        master_conf = self.local_conf["master"]
       	return self.container.connect_remote_service(master_conf, mapred_thrift.MapRedMaster)