from .mapred_taskbase import MapRedTaskBase
import time


class SleepTask(MapRedTaskBase):

	def run_task(self, conf, zip):
		self.start()

	def run(self):
		time.sleep(30)