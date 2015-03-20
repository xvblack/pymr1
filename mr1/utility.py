import logging
import pprint, pathlib, itertools


def setup_logging():
    logging.basicConfig(level=logging.DEBUG)

printer = pprint.PrettyPrinter(indent=4)

def format_dict(dict):
	return printer.pformat(dict)

def all_string_dict(old_dict):
	new_dict = dict((str(k), str(v)) for (k, v) in old_dict.iteritems())
	return new_dict

class Directory:

	def __init__(self, path):
		if isinstance(path, str):
			self.path = pathlib.Path(path)
		elif isinstance(path, pathlib.Path):
			self.path = path
		else:
			self.path = path.path	
		assert self.path.exists()

	def create_dir(self, name=None, prefix=""):
		# TODO: more efficient way

		if name is None:
			for i in itertools.count():
				file_name = "%s_%s" % (prefix, i)
				path = self.path / file_name
				if not path.exists():
					break

			path.mkdir()
		else:
			path = self.path / name
			assert not path.exists()
			path.mkdir()
		
		return Directory(path)

	def create_tmp_file(self, name=None, prefix=""):

		if name is None:
			for i in itertools.count():
				file_name = "%s_%s" % (prefix, i)
				path = self.path / file_name
				if not path.exists():
					break

			path.touch()
		else:
			path = self.path / name
			assert not path.exists()
			path.touch()
		return path