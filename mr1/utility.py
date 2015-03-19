import logging
import pprint


def setup_logging():
    logging.basicConfig(level=logging.DEBUG)

printer = pprint.PrettyPrinter(indent=4)

def format_dict(dict):
	return printer.pformat(dict)