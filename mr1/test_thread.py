from mr1.container import Container
from mr1.mapred import MapRedMasterTask, mapred_thrift
from thriftpy.rpc import client_context
import threading, sys, time
import mr1.utility as utility

thrift_conf = {
	"host" : "127.0.0.1",
	"port" : "8081"
}

conf = {
	"thrift" : thrift_conf,
	"work_dir" : "/tmp/container"
}


utility.setup_logging()

sleep_conf = {
	"type" : "sleep",
	"job_id" : "sleep1"
}

from mr1.mapred.test import SleepTask

# try:
container = Container(conf)

s = SleepTask(None, None)
# s.run_task(sleep_conf, "a")
container.run_task(sleep_conf, "a")

while True:
	time.sleep(100)
# except Exception as e:
# 	print "error %s %s" % (type(e), e)
# 	sys.exit(0)