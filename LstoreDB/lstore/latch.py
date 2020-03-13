from threading import BoundedSemaphore
import threading
import time

class Latch:
    def __init__(self):
        self.__sem__ = BoundedSemaphore(1)
        self.__access__ = BoundedSemaphore(1)
        self.__lock__ = BoundedSemaphore(1)
        self.count = 0

    def acquire(self): #reader acquire
        self.__lock__.acquire()
        self.__lock__.release()
        self.__access__.acquire()
        self.count = self.count + 1
        if self.count == 1:
            self.__sem__.acquire()
        self.__access__.release()
        
    def release(self): #reader release
        self.__access__.acquire()
        self.count = self.count - 1
        if self.count == 0:
            self.__sem__.release()
        self.__access__.release()

    def latch(self): #writer acquire
        self.__lock__.acquire()
        self.__sem__.acquire()

    def unlatch(self): #writer release
        self.__sem__.release()
        self.__lock__.release()

"""
latch = Latch()

class R:
    def __init__(self, count):
        self.count = count
    def reader(self):
            while 1:
                print(self.count, "attempting read")
                latch.acquire()
                print(self.count, "done reading")
                latch.release()
                time.sleep(1)
class W:
    def __init__(self):
        pass
    def writer(self):
        while 1:
            print("attempting latch")
            latch.latch()
            print("latched")
            time.sleep(5)
            latch.unlatch()
            print("unlatched")


w = W()
threads = []
threads.append(threading.Thread(target=w.writer, args = ()))
readers = []
for i in range(5):
    readers.append(R(i))
for r in readers:
    threads.append(threading.Thread(target=r.reader, args = ()))

for thread in threads:
    thread.start()
"""
