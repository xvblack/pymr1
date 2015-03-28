# Container configuration

import sys
import tempfile
import yaml
import time
import os
from mr1.container import Container
from mr1.fs import ThriftLocalFileService, fs_thrift
import mr1.utility as utility
import argparse

example = """
thrift:
    host: 127.0.0.1
    port: 8081

resource_node:
    type: here

work_dir: /tmp/container
"""

def run_conf(conf):
    container = Container(conf)
    fs = ThriftLocalFileService(container, {})
    container.start()
    container.add_service("thrift-fs", fs_thrift.FileSystem, fs, unique=True)
    os.environ["container_endpoint"] = container.get_service("resource_node") \
                                            .endpoint().serialize()
    print os.environ["container_endpoint"]
    while True:
        time.sleep(100)

def main():
    parser = argparse.ArgumentParser("Run container")

    parser.add_argument("--conf_file")
    parser.add_argument("--resource_node", choices=["here", "remote", "local"])
    parser.add_argument("--resource_node_endpoint")
    parser.add_argument("--work_dir")


    confs = vars(parser.parse_args())

    if confs["conf_file"] is not None:
        conf_path = confs["conf_file"]

        conf_file = open(conf_path)
        conf = yaml.load(conf_file)
        conf_file.close()
    else:
        conf = yaml.load(example)

    if confs["resource_node"] is not None:
        if confs["resource_node"] == "remote":
            conf["resource_node"] = {
                "type" : "remote",
                "endpoint" : confs["resource_node_endpoint"]
            }
        elif confs["resource_node"] == "here":
            conf["resource_node"] = {
                "type" : "here"
            }
        elif confs["resource_node"] == "local":
            conf["resource_node"] = {
                "type" : "local"
            }

    if confs["work_dir"] is not None:
        conf["work_dir"] = confs["work_dir"]

    run_conf(conf)

# thrift_conf = {
#   "host" : "127.0.0.1",
#   "port" : "8081"
# }

# resource_node_conf = {
#   "type" : "here"
# }

# conf = {
#   "thrift" : thrift_conf,
#   "work_dir" : "/tmp/container",
#   "resource_node" : resource_node_conf
# }

utility.setup_logging()

if __name__ == '__main__':
    main()