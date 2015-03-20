import thriftpy
from thriftpy.protocol import TBinaryProtocolFactory, TMultiplexingProtocol
from thriftpy.transport import TBufferedTransportFactory, TServerSocket, TSocket
from thriftpy.server import TThreadedServer
from thriftpy.thrift import TClient, TMultiplexingProcessor, TProcessor
from threading import Thread
import mr1.utility as utility
from mr1.rpc import IpEndPoint, ThriftEndPoint
import logging

from mr1.mapred import MapTask, ReduceTask, MapRedMasterTask
import itertools


class MultiplexThriftServer:
    def __init__(self, factory, params):
        self.endpoint = IpEndPoint(params["host"], params["port"])
        socket = factory.SERVER_SOCKET_CLASS(host=params["host"], port=params["port"])
        processor = TMultiplexingProcessor()
        trans_factory = factory.TRANS_FAC_CLASS()
        proto_factory = factory.PROTO_FAC_CLASS()
        server = factory.SERVER_CLASS(processor, 
            socket,
            iprot_factory=proto_factory,
            itrans_factory=trans_factory)
        self.processor = processor
        self.server = server

class MultiplexThriftClient:
    def __init__(self, factory, endpoint, service):
        socket = factory.CLIENT_SOCKET_CLASS(host=endpoint.host, port=endpoint.port)
        trans_factory = factory.TRANS_FAC_CLASS()
        proto_factory = factory.PROTO_FAC_CLASS()
        transport = trans_factory.get_transport(socket)
        protocol = proto_factory.get_protocol(transport)
        multiplex_protocol = TMultiplexingProtocol(protocol, endpoint.service_name)
        transport.open()
        self.client = TClient(service, multiplex_protocol)

class MultiplexThriftFactory:

    SERVER_SOCKET_CLASS = TServerSocket
    CLIENT_SOCKET_CLASS = TSocket
    SERVER_CLASS = TThreadedServer
    PROTO_FAC_CLASS = TBinaryProtocolFactory
    TRANS_FAC_CLASS = TBufferedTransportFactory

    @classmethod
    def make_server(klass, params):
        return MultiplexThriftServer(klass, params)

    @classmethod
    def make_client(klass, endpoint, service):
        return MultiplexThriftClient(klass, endpoint, service)

class LocalResourceNode:

    def __init__(self, container):
        self.container = container

    def allocate_node_container(self):
        return self.container.thrift_server.endpoint

class Container(Thread):

    """
    Container for underlying services from this node

    conf should include configuration for http server and thrift server
    example:
    """

    def __init__(self, conf):
        Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.daemon = True
        self.conf = conf
        self.dir = utility.Directory(conf["work_dir"])
        self.thrift_server = MultiplexThriftFactory.make_server(conf["thrift"])
        self.thrift_server.server.daemon = True
        self.services = {}

    def run(self):
        self.thrift_server.server.serve()

    def add_service(self, service_name, service, handler):
        processor = TProcessor(service, handler)
        for i in itertools.count():
            if "%s_%s" % (service_name, i) not in self.services:
                break
        service_name = "%s_%s" % (service_name, i)
        self.services[service_name] = processor
        self.thrift_server.processor.register_processor(service_name, processor)
        return service_name

    def generate_conf(self, task_conf={}):
        new_work_dir = self.dir.create_dir(prefix=task_conf["job_id"])
        return {"work_dir" : new_work_dir}

    def run_task(self, task_conf, zip):
        self.logger.debug("running job conf\n%s" % utility.format_dict(task_conf))

        assert task_conf["type"] in ["map", "reduce", "mapred-master", "sleep"]

        task_klass = None
        if task_conf["type"] == "map":
            task_klass = MapTask
        elif task_conf["type"] == "reduce":
            task_klass = ReduceTask
        elif task_conf["type"] == "mapred-master":
            task_klass = MapRedMasterTask
        elif task_conf["type"] == "sleep":
            # DEBUG: 
            from mr1.mapred.test import SleepTask
            task_klass = SleepTask

        task = task_klass(self, self.generate_conf(task_conf))
        task.run_task(task_conf, zip)

        pass

    def connect_resource_node(self):
        # TODO: replace with real resource node
        return LocalResourceNode(self)

    def connect_remote_container(self, remote_container):
        # TODO: replace with real remote container
        assert remote_container == self.thrift_server.endpoint
        return self

    def connect_remote_service(self, endpoint, service):
        client = MultiplexThriftFactory.make_client(endpoint, service).client
        return client