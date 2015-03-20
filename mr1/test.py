# import sys, os
#
# sys.path.append(os.path.join(os.getcwd(), "thriftpy"))

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
	"job_id" : "mapred1",
	"reduce_count" : "1",
	"input_paths" : "/tmp/a"
}

mapred_master_conf2 = {
	"type" : "mapred-master",
	"job_id" : "mapred2",
	"reduce_count" : "1",
	"input_paths" : "/tmp/a"
}

resource_node_conf = {
	"type" : "local"
}

conf = {
	"thrift" : thrift_conf,
	"work_dir" : "/tmp/container",
	"resource_node" : resource_node_conf
}

utility.setup_logging()



# try:
container = Container(conf)
container.start()

# mapred_master = MapRedMasterTask(container, mapred_master_conf)


print "inited"

# with client_context(mapred_thrift.MapRedMaster, "127.0.0.1", "8081") as c:
# 	l = c.get_map_output_locations(1)
# 	c.run_task(task_conf, "a")
# 	print l
# 	
container.run_task(mapred_master_conf, "a")
# container.run_task(mapred_master_conf2, "a")


while True:
	time.sleep(100)
# except Exception as e:
# 	print "error %s %s" % (type(e), e)
# 	sys.exit(0)