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
        values = []
        current_key = None
        for key, value in self.all:
            if key == current_key:
                values.append(value)
            else:
                if current_key != None:
                    yield (current_key, values)
                current_key = key
                values = [value]
        if current_key != None:
            yield (current_key, values)
        return
        
Merger = SimpleMerger