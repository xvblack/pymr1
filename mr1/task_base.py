import threading, logging
import multiprocessing
import utility
from rpc import ContainerService

ConcurrentBase=threading.Thread
# ConcurrentBase = multiprocessing.Process

class TaskBase(ConcurrentBase, ContainerService):

    def __init__(self, container, conf):
        ConcurrentBase.__init__(self)
        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.daemon = True
        self.container = container
        self.conf = conf

    def setup_workdir(self):
        assert "work_dir" in self.conf

        if not isinstance(self.conf["work_dir"], utility.Directory):
            self.dir = utility.Directory(self.conf["work_dir"])
        else:
            self.dir = self.conf["work_dir"]
        print self.dir

    def convert_task_conf(self):

        to_convert = []
        self.local_conf = {}

        for key in self.task_conf:
            if key.endswith("endpoint"):
                addr = self.task_conf[key]
                key = key.split("-")[0]

                self.local_conf[key] = ThriftEndPoint.deserialize(addr)


    def ping(self):
        return True