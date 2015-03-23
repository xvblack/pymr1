from thriftpy.rpc import make_server
import thriftpy

container_thrift = thriftpy.load("resources/container.thrift")

class IpEndPoint:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def get_service(self, service_name):
        return ThriftEndPoint(self.host, self.port, service_name)


class ThriftEndPoint:

    def __init__(self, host, port, service_name):
        self.host = host
        self.port = port
        self.service_name = service_name

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.serialize())

    def serialize(self):
        return "%s:%s/%s" % (self.host, self.port, self.service_name)

    @staticmethod
    def deserialize(s):
        host, remain = s.split(":")
        port, service_name = remain.split("/")
        return ThriftEndPoint(host, int(port), service_name)

class ContainerService:

    def endpoint(self):
        assert hasattr(self, "container")
        assert hasattr(self, "service_id")
        return self.container.thrift_server.endpoint.get_service(self.service_id)

    def register_container(self, service_type, service, unique=False):
        # TEST: is this work?
        # TODO: refactor
        self.service_type = service_type
        self.service_id = self.container.add_service(service_type, service, self, unique)