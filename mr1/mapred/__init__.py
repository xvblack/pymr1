import thriftpy
import logging


from .mapred_taskbase import mapred_thrift

from .map_task import MapTask
from .reduce_task import ReduceTask
from .master_task import MapRedMasterTask

