import threading, logging

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

        for key in self.conf:
        	if key.endswith("endpoint"):
        		addr = self.conf[key]

        		del self.conf[key]
        		key = key.split("-")[0]

        		self.conf[key] = ThriftEndPoint.deserialize(addr)