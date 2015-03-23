from .mapred_taskbase import MapRedTaskBase, mapred_thrift
import mr1.utility as utility
import mr1.fs as fs
import StringIO, collections
import time

class LineIterator:

	def __init__(self, stream):
		self.stream = stream

	def __iter__(self):
		return self

	def next(self):
		line = next(self.stream).strip()
		return None, line

class SplitLineIterator:

	def __init__(self, stream, separator=" "):
		self.stream = stream
		self.separator = separator

	def __iter__(self):
		return self

	def next(self):
		line = next(self.stream).strip()
		try:
			i = line.index(self.separator)
		except:
			i = len(line)
		key, val = line[:i], line[i+1:]
		return key, val

class SimplePrintMapper:

	def __init__(self, task, collector):
		self.task = task
		self.collector = collector

	def map(self, key, value):
		self.collector.collect(key, value)

class BinaryFunctionMapper:

	def __init__(self, task, collector):
		self.task = task
		self.collector = collector

		# load codes
		# TODO: allow complex code import
		
		self.environ = {}
		exec self.task.zip in self.environ
		self.mapper = self.environ["mapper"]
	
	def map(self, key, value):
		inter_key, inter_val = self.mapper(key, value)
		self.collector.collect(inter_key, inter_val)

class NullCollector:

	def __init__(self, task, partitioner):
		self.task = task
		self.partitioner = partitioner

	def collect(self, key, value):
		print self.partitioner.part_key(key), key, value

class InMemoryCollector:

	def __init__(self, task, partitioner):
		self.task = task
		self.partitioner = partitioner
		self.data = collections.defaultdict(list)

	def collect(self, key, value):
		part_key = self.partitioner.part_key(key)
		self.data[part_key].append((key, value))

	def finalize(self):
		for key in range(self.task.reduce_count):
			self.data[key].sort()
			output_file = self.task.dir.create_tmp_file(name=("shuffle_file_%s" % key))
			output_stream = output_file.open("w")
			output_stream.write(u"")
			for k, v in self.data[key]:
				output_stream.write(u"%s\t%s\n" % (k, v))
			output_stream.close()
			# output = {"type":"local", "path": str(output_file)}
			file_service = self.task.container.get_service("thrift-fs")
			assert file_service is not None
			output = file_service.add_file(output_file)
			self.task.master.register_output(self.task.task_conf, key, output)

		self.task.master.report_map_finished(self.task.task_conf)

		print self.data


class HashPartitioner:

	def __init__(self, task):
		self.task = task

	def part_key(self, key):
		return hash(key) % self.task.reduce_count

class MapTask(MapRedTaskBase):

    def __init__(self, container, conf):
        MapRedTaskBase.__init__(self, container, conf)

    def run_task(self, task_conf, zip):
    	self.task_conf = task_conf
    	self.reduce_count = int(self.task_conf["reduce_count"])
    	self.convert_task_conf()
    	self.zip = zip
    	self.logger.debug(utility.format_dict(self.conf))
    	self.logger.debug(utility.format_dict(self.task_conf))
    	self.setup_workdir()
    	self.master = self.get_mapred_master()
    	self.start()

    def run(self):

    	# get input stream
    	input_stream = fs.open_uri(self.task_conf["input"], "r")

    	# get line iterator
    	record_stream = SplitLineIterator(input_stream)

    	# setup mapper
    	partitioner = HashPartitioner(self)

    	collector = InMemoryCollector(self, partitioner)
    	# mapper = SimplePrintMapper(self, collector)
    	mapper = BinaryFunctionMapper(self, collector)

    	for key, val in record_stream:
    		mapper.map(key, val)

    	collector.finalize()
    	self.logger.debug("map %s finished" % self.task_conf["job_id"])
    	return
