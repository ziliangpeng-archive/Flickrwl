'''
Created on Sep 28, 2010

@author: ziliangdotme
'''
import threading
import thread
import time
import Queue


class ThreadPool(object):
    """Multi-threading supports."""


    def __init__(self, num_threads, func, queue_size=1024):
        assert callable(func)
        self.func = func
        self.queue = Queue.Queue(queue_size)
        self._workers = []
        self.add_worker(int(num_threads))


    def add_data(self, *args, **kwargs):
        self.queue.put((args, kwargs))


    def add_worker(self, num=1):
        for _ in range(num):
            self._workers.append(Worker(self))


    def kill_worker(self, num=1, wait=False):
        dead = []
        for _ in range(num):
            if self.workers:
                w = self.workers.pop()
                w.shutdown()
                dead.append(w)
            else:
                break
        if wait:
            for w in dead:
                w.join()


    def wait(self):
        self.queue.join()


    def shutdown(self, wait=False):
        '''Shut everything down.'''
        if wait:
            self.wait()
        for worker in self._workers:
            worker.shutdown()
            

class Worker(threading.Thread):

    _finished = threading.Event()

    def __init__(self, pool):
        super(Worker, self).__init__()
        self.pool = pool
        self.func = pool.func
        self.start()


    def shutdown(self, wait=False):
        self._finished.set()
        if wait:
            self.join()


    def is_shutdown(self):
        return self._finished.is_set()


    def run(self):
        queue = self.pool.queue
        
        while not self.is_shutdown():
            try:
                params = queue.get(True, 1)
            except Queue.Empty:
                continue
            args = params[0]
            kwargs = params[1]
            self.func(*args, **kwargs)
            queue.task_done()
        
