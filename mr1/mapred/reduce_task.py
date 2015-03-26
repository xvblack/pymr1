from multiprocessing import Process
from subprocess import Popen, PIPE
import Queue
from .mapred_taskbase import MapRedTaskBase, mapred_thrift
import mr1.utility as utility
import mr1.fs as fs
from threading import Thread
import time

from .map_task import SplitLineIterator
from .master_task import ShuffleOutputInfo
from .utility import Merger

class SimpleReducer:
    def __init__(self, task, collector):
        self.collector = collector
        self.count = 0

    def reduce(self, key, values):
        self.count += 1
        print "%s#%s#%s" % (self.count, key, values)
        self.collector.collect(key, len(values))

class BinaryFunctionReducer:
    def __init__(self, task, collector):
        self.task = task
        self.collector = collector

        self.environ = {}
        exec self.task.zip in self.environ

        self.reducer = self.environ["reducer"]

    def reduce(self, key, values):
        output_key, output_val = self.reducer(key, values)
        print "%s#%s" % (output_key, output_val)
        self.collector.collect(output_key, output_val)

class NullCollector:
    def __init__(self):
        self.count = 0

    def collect(self, key, value):
        self.count += 1
        print self.count

class InMemoryCollector:

    def __init__(self, task):
        self.task = task
        self.data = []

    def collect(self, key, value):
        self.data.append((key, value))

    def finalize(self):
        output_file = self.task.dir.create_tmp_file(name="reduce_output")
        output_stream = output_file.open("w")

        for k, v in self.data:
            output_stream.write(u"%s\t%s\n" % (k, v))

        output_stream.close()

class ShuffleWatcher(Thread):

    def __init__(self, task):
        Thread.__init__(self)
        self.task = task
        self.queue = Queue.Queue()
        self.finished = set()
        self.all_finished = False
        self.daemon = True
        self.start()

    def run(self):
        while not self.all_finished:
            self.update()
            time.sleep(5)

    def update(self):
        master = self.task.master
        outputs = master.get_all_outputs(self.task.task_conf)
        outputs = [ShuffleOutputInfo.from_dict(output) for output in outputs]

        self.all_finished = True

        for output in outputs:
            if output.map_id in self.finished:
                continue
            elif not output.finished:
                self.all_finished = False
            else:
                self.finished.add(output.map_id)
                self.queue.put(output)

        print "updated"
        print self.finished

    def get(self):
        print "working"        
        while True:
            try:
                return self.queue.get(timeout=10)
            except Queue.Empty:
                pass

class ReduceTask(MapRedTaskBase):


    def prepare(self):
        pass

    def run_task(self, task_conf, zip):
        self.task_conf = task_conf
        self.convert_task_conf()
        self.logger.debug(utility.format_dict(self.conf))
        self.logger.debug(utility.format_dict(self.task_conf))
        self.setup_workdir()
        self.register_container("reduce", mapred_thrift.MapTask)

        self.master = self.get_mapred_master()
        self.start()
        print("run_task finished")

    def run(self):

        # TODO: 

        # Watching for MapTask finished

        watcher = ShuffleWatcher(self)
        input_iters = []

        # Cache files locally

        while not watcher.all_finished:

            output = watcher.get()

            output_info = output.output_info

            data_stream = fs.open(self.container, output_info, "r")
            kv_iter = SplitLineIterator(data_stream, "\t")
            input_iters.append(kv_iter)

        # Shuffle
        
        merger = Merger()
        merged_iter = merger.merge(input_iters)

        # Run reducer

        collector = InMemoryCollector(self)
        reducer = SimpleReducer(self, collector)
        for k, vs in merged_iter:
            reducer.reduce(k, vs)
        collector.finalize()

        self.logger.debug("reducer %s finished" % self.task_conf["job_id"])
        self.master.report_reduce_finished(self.task_conf)
        return