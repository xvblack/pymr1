class TMultiplexingProtocol(object):

    """

    Multiplex protocol

    for writing message begin, it prepend the service name to the api
    for other functions, it simply delegate to the original protocol

    """

    def __init__(self, proto, service_name):
        self.service_name = service_name
        self.proto = proto

        # methods = ["skip", "read_message_begin", "read_message_end",
        #  "write_message_end", "read_struct", "write_struct"]
        # for method in methods:
        #     setattr(self, method, getattr(self.proto, method))

    def __getattr__(self, name):
        return getattr(self.proto, name)

    def write_message_begin(self, name, ttype, seqid):
        self.proto.write_message_begin(self.service_name + "+" + name, ttype, seqid)
