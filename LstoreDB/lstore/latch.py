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
