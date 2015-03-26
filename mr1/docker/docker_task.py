from mr1.task_base import TaskBase
import docker
import thriftpy
docker_thrift = thriftpy.load("resources/docker.thrift", module_name="docker_thrift")

class DockerTask(TaskBase):

	def __init__(self, container, conf):
		TaskBase.__init__(self, container, conf)

	def run_task(self, task_conf, zip):
		assert task_conf["server"] == "boot2docker"
		self.register_container("docker", docker_thrift.DockerTask, unique=False)
		# TODO: other docker container
		self.docker_client = docker.Client(**docker.utils.kwargs_from_env())
		print self.docker_client.version()
