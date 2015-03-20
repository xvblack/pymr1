import threading, logging
import utility

ConcurrentBase=threading.Thread

class TaskBase(ConcurrentBase):

    def endpoint(self):
        return self.container.thrift_server.endpoint.get_service(self.service_id)

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