import Queue as queue
import hashlib
import sys
import threading

# ConcurrentBase = threading.Thread

# class TaskTracker(threading.Thread):

#     def __init__(self, container, job_queue):
#         self.job_queue = job_queue

#     def build_task(self, task_description):
#         assert "type" in task_description

#         if task_description["type"] == "map":
#             task = MapTask(container, task_description)
#         elif task_description["type"] == "reduce":
#             task = ReduceTask(container, task_description)
#         else:
#             raise Exception("Not valid task type %s" % task_description)

#     def run(self):
#         while True:
#             task_description = self.job_queue.get()
#             task = self.build_task(task_description)
#             task.start()