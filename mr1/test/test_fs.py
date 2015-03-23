from mr1.fs import BufferedLineReader
from StringIO import StringIO
import unittest


class TestFS(unittest.TestCase):

	def test_fs(self):
		data = u"abc\ndef\nghi\n"
		stream = StringIO(data)

		reader = BufferedLineReader(stream)

		self.assertEqual(list(reader), ["abc\n", "def\n", "ghi\n"])

if __name__ == '__main__':
	unittest.main()