import thriftpy
from mr1.task_base import TaskBase
from mr1.rpc import ThriftEndPoint


mapred_thrift = thriftpy.load("resources/mapred.thrift", module_name="mapred_thrift")

class MapRedTaskBase(TaskBase):

    def __init__(self, container, conf):
        TaskBase.__init__(self, container, conf)

    def get_mapred_master(self):
        master_conf = self.local_conf["master"]
       	return self.container.connect_remote_service(master_conf, mapred_thrift.MapRedMaster)