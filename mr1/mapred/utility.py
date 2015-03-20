#
# Utility classes
#

class SimpleMerger(object):

    def merge(self, iters):
        # TODO: implement a efficient merger
        
        self.all = []
        for iterable in iters:
            for i in iterable:
                self.all.append(i)

        self.all.sort()
        return iter(self.all)
        
Merger = SimpleMerger