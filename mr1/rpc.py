from thriftpy.rpc import make_server

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

    def serialize(self):
        return "%s:%s/%s" % (self.host, self.port, self.service_name)

    @staticmethod
    def deserialize(s):
        host, remain = s.split(":")
        port, service_name = remain.split("/")
        return ThriftEndPoint(host, port, service_name)
