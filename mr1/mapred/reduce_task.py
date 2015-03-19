from multiprocessing import Process
from subprocess import Popen, PIPE

from .mapred_taskbase import MapRedTaskBase

class ReduceTask(MapRedTaskBase):


    def prepare(self):
        pass

    def run(self):

        # get params
        
        reducer_uri = params["reducer"]
        shuffle_files = params["shuffle_files"]

        # Download reducer scripts
        
        reducer_file = self.container.fs.cache_local_from_uri(reducer_uri)

        # Watching for MapTask finished

        watch_queue = queue.Queue()
        count = len(self.shuffle_files)

        for s in self.shuffle_files:
            s.on_finished(watch_queue.put)

        # Cache files locally

        input_streams = []

        while count > 0:
            uid = q.get()
            self.logger.debug("shuffle file %s finished" % uid)

            self.logger.debug("caching file uri: %s" % uid)

            f = self.container.fs.cache_local_from_uri(uri)
            line_iter = iter(f.open("r"))
            input_streams.append(line_iter)

            count = count - 1

        # Shuffle
        
        merger = Merger()
        output_stream = merger.merge(input_streams)

        # Run reducer

        directory = self.container.fs.new_directory_with_prefix("reduce")
        self.logger.debug("reducer output to %s" % directory)

        output_file = directory.touch("reduced.tmp")
        output = output_file.open("w")

        reduce_process = Popen(
            ["python", reducer_file.path], stdin=PIPE, stdout=output)

        for line in output_stream:
            reduce_process.stdin.write(line)

        reduce_process.communicate()

        self.logger.debug("reducer %s finished" % self.jobid)

        id = self.container.fs.addFile(open(str(directory.touch("reduced.tmp")), "r"))
        replica = self.container.fs.getReplica(id)
        self.container.namenode.addReplicaToFile(replica, self.output_uri)

        return