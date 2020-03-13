
# the pseudo lockerManager class to test if transaction.py actually works
class FakeLockManager:
    def __init__(self):
        pass
        
    def getLock(self, table_name, address, actionName):
        print ("LOCK ACQUIRED for " + table_name + " address " + str(address) + " action " + str(actionName) + "\n")
        return True

    # def releaseLock(self, table_name = "table", address = None, actionName = "action"):
    #     print ("LOCK RELEASED for " + table_name + " address " + str(address) + " action " + str(actionName) + "\n")
    #     return True

    def releaseLock(self):
        print("LOCKS RELEASED\n")
        return True