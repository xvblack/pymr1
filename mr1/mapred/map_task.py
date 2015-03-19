from .mapred_taskbase import MapRedTaskBase, mapred_thrift

class MapTask(MapRedTaskBase):

    def __init__(self, container, conf):
        TaskBase.__init__(self, container, conf)