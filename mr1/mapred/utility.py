#
# Utility classes
#


class Merger(object):

    def merge(iters):
        # TODO: implement merger class

class ShuffleWatcher:

    def __init__(self, client, uri):
        self.client = client
        self.uri = uri

        self.logger = logging.getLogger("ShuffleWatcher")
        self.logger.debug("watching: %s" % self.uri)

    def on_finished(self, callback):
        def __inner_func():
            while True:
                if self.client.namenode.isFinished(self.uri):
                    callback(self.uri)
                    break
                else:
                    time.sleep(1)

        thr = threading.Thread(
            target=__inner_func, args=(), kwargs={})
        thr.start()
        return
