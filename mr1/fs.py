import __builtin__
from rpc import ThriftEndPoint, ContainerService
from io import TextIOBase
import thriftpy
import pathlib, itertools

fs_thrift = thriftpy.load("resources/fs.thrift")

def open_uri(uri, mode):
	assert uri[0] == "/"
	return __builtin__.open(uri, mode)

class BufferedLineReader:

	BUFFER_SIZE = 4096
	END_LINE = "\n"

	def __init__(self, stream):
		"""
		stream: stream like object, support read
		"""
		self.stream = stream
		self.buffer = None
		self.EOF = False

	def __iter__(self):
		return self

	def next(self):

		output = []
		if self.EOF:
			raise StopIteration()
		while True:
			if self.buffer is None:
				self.buffer = self.stream.read(self.BUFFER_SIZE)
				if len(self.buffer) == 0:
					self.EOF = True
					break
				self.curr_ind = 0
			try:
				next_ind = self.buffer.index(self.END_LINE, self.curr_ind)
				output.append(self.buffer[self.curr_ind: next_ind + 1])
				self.curr_ind = next_ind + 1
				break
			except ValueError:
				if self.curr_ind != len(self.buffer):
					output.append(self.buffer[self.curr_ind:])
				self.buffer = None
				continue
		if len(output) != 0:
			return "".join(output)
		else:
			raise StopIteration()

class ThriftRemoteFile:

	def __init__(self, container, endpoint, file_id):
		self.container = container
		self.endpoint = endpoint
		self.file_id = file_id

	def open(self, mode):
		# TODO: support other file mode?
		assert mode == "r"
		self.remote = self.container.connect_remote_service(self.endpoint, fs_thrift.FileSystem)
		self.handle = self.remote.open(self.file_id)

	def read(self, size):
		result = self.remote.read(self.handle, size)
		self.handle = result.handle
		if result.EOF:
			return ""
		return result.data

	def close(self):
		self.remote.close(self.handle)

class ThriftLocalFileService(ContainerService):

	def __init__(self, container, conf):
		self.container = container
		self.register_container("fs", fs_thrift.FileSystem, unique=True)
		self.paths = {}
		self.state_cache = {}
		self.conf = conf
		self._id = itertools.count()

	def add_file(self, file_path):
		file_id = str(self._id.next())
		self.paths[file_id] = file_path
		file_info = {
			"type" : "thrift-remote",
			"endpoint" : self.endpoint().serialize(),
			"id" : file_id
		}
		return file_info
	
	def open(self, file_id):
		path = self.paths[file_id]
		assert isinstance(path, pathlib.Path)
		f = path.open("r")
		self.state_cache[file_id] = f
		return fs_thrift.FileHandle(file_id, 0)

	def read(self, file_handle, size):
		f = self.state_cache[file_handle.file_id]
		f.seek(file_handle.loc)
		data = f.read(size)
		new_handle = fs_thrift.FileHandle(file_handle.file_id, file_handle.loc + len(data))
		EOF = len(data) == 0
		return fs_thrift.ReadResult(new_handle, EOF, data)

	def close(self, file_handle):
		# TODO: a more efficient implementation
		pass


def open(container, info, mode):
	assert "type" in info

	if info["type"] == "local":
		f = __builtin__.open(info["path"], mode)
	elif info["type"] == "thrift-remote":
		endpoint = ThriftEndPoint.deserialize(info["endpoint"])
		remote_stream = ThriftRemoteFile(container, endpoint, info["id"])
		remote_stream.open("r")
		f = BufferedLineReader(remote_stream)
	else:
		raise Exception("Not support yet")

	return f