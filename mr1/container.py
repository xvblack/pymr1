import thriftpy
from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.transport import TBufferedTransportFactory, TServerSocket, TSocket
from thriftpy.server import TThreadedServer
from thriftpy.thrift import TClient, TMultiplexingProcessor, TProcessor
from threading import Thread
import mr1.utility as utility
from mr1.rpc import IpEndPoint, ThriftEndPoint
import logging

from mr1.mapred import MapTask, ReduceTask, MapRedMasterTask


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
        # sub_protocol = TMultiplexedProtocol(protocol, params["service_name"])
        transport.open()
        self.client = TClient(endpoint.service, protocol)

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
        self.thrift_server = MultiplexThriftFactory.make_server(conf["thrift"])

    def run(self):
        self.thrift_server.server.serve()

    def add_service(self, service_name, service, handler):
        processor = TProcessor(service, handler)
        self.thrift_server.processor.register_processor(processor)
        return service_name

    def run_task(self, task_conf, zip):
        self.logger.debug("running job conf\n%s" % utility.format_dict(task_conf))

        assert task_conf["type"] in ["map", "reduce", "mapred-master"]


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