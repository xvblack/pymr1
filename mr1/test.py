from mr1.container import Container
from mr1.mapred import MapRedMasterTask, mapred_thrift
from thriftpy.rpc import client_context
import threading, sys, time
import mr1.utility as utility

thrift_conf = {
	"host" : "127.0.0.1",
	"port" : "8081"
}

mapred_master_conf = {
	"type" : "mapred-master",
	"dir" : "/tmp",
	"job_id" : "mapred1"
}

conf = {
	"thrift" : thrift_conf
}

task_conf = {
	"reduce_count" : "5",
	"input_paths" : "hdfs://a hdfs://b"
}

utility.setup_logging()

# try:
container = Container(conf)
container.start()

mapred_master = MapRedMasterTask(container, mapred_master_conf)

print "inited"

with client_context(mapred_thrift.MapRedMaster, "127.0.0.1", "8081") as c:
	l = c.get_map_output_locations(1)
	c.run_task(task_conf, "a")
	print l

while True:
	time.sleep(100)
# except Exception as e:
# 	print "error %s %s" % (type(e), e)
# 	sys.exit(0)