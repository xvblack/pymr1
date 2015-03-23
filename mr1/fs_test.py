from mr1.fs import BufferedLineReader
from StringIO import StringIO

data = u"abc\ndef\nghi\n"
stream = StringIO(data)

reader = BufferedLineReader(stream)

for line in reader:
	print repr(line)