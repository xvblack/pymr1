import __builtin__

def open_uri(uri, mode):
	assert uri[0] == "/"
	return __builtin__.open(uri, mode)

def open(info, mode):
	assert "type" in info

	if info["type"] == "local":
		f = __builtin__.open(info["path"], mode)
	else:
		raise Exception("Not support yet")

	return f