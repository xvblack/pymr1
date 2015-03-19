import logging

class HandlerBase:
    def __init__(self, container, params):
        self.container = container
        self.logger = logging.getLogger(self.__class__.__name__)