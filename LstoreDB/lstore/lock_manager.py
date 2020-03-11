import threading
from config import *



class LockManager:
    def __init__(self):
        self.locks = {}
        pass
    
    #def getlock(self, type_l, level, index):
    def getlock(self, tabOrAddr):
        thread_id = threading.get_ident()

        target = self.locks.get(thread_id)
        if target:
            pass #TODO stuff
        else:
            self.locks[thread_id] = {} #insert into locks


        #check if thread already has needed lock
        #go from top level down and acquire locks
        #check: if acquired IS on higher level, cannot get X or IX on lower level 

class Lock:
    #def __init__(self, type_l, level, index):
    def __init__(self, table, page_range, page, row):
        self.table = table
        self.page_range = page_range
        self.page = page
        self.row = row

class lockNode:

    def __init__(self, parent, nodeID):
        self.nodeID = nodeID
        self.parent = parent
        self.child = {}
        self.locks = [0,0,0,0,0]
        # locks[0] will never be accessed
    
    def add_lock(self, lock):
        self.locks[lock] += 1

    def remove_lock(self, lock):
        self.locks[lock] -= 1
    
    def compare_locks(self, lock):
        """
             | 1 2 3 4
            _|_________
            1| y y n n
            2| y y n n
            3| n n y n
            4| n n n n
        """
        # if CurrentLock has S and NewLock wants S
        if(self.locks[S] >= 0 and lock == S):
            return 1
        # if the sum of the CurrentLock and NewLock
        # is less than or equal to 4
        for type in range(1,2):
            sum = lock
            if(self.locks[type] > 0):
                sum += type
            if(sum <= 4):
                return 1
        # Else, a NewLock cannot be added
        return 0       

class lockTree:
    """
    In this structure, say we want a lock in a record in 
    the 'Grades' table, PageRange = 1, Page 58, Row = 275
    The path would be a list  containing:
    ['Grades', 1, 58, 275]
    """
    
    def __init__(self):
        self.root = lockNode(None, 'root')

    # this continues the recursive process to find or create the 
    # correct lockNode
    def traverse(self, node, path, opperation, lock_change):
        if not len(path):
            if(opperation == 'add'):
                if(node.compare_locks(lock_change)):
                    node.add_lock(lock_change)
                    return 1
                else:
                    return 0
            if(opperation == 'remove'):
                node.remove_lock(lock_change)
                return 1
        try:
            node.child[path[0]]
        except (IndexError, KeyError):
            new_node = lockNode(node, path.pop(0))
            node.child[new_node.nodeID] = new_node
            if(self.traverse(new_node, path, opperation, lock_change)):
                return 1
            return 0
        else:
            if(self.traverse(node.child[path.pop(0)], path, opperation, lock_change)):
                return 1
            return 0

    # this will start the add or remove lock process
    def change_lock(self, path, opperation, lock_change):
        #
        try:
            self.root.child[path[0]]
        except (IndexError, KeyError):
            new_node = lockNode(self.root, path.pop(0))
            self.root.child[new_node.nodeID] = new_node
            if(self.traverse(new_node, path, opperation, lock_change)):
                return 1
            return 0
        else:
            if(self.traverse(self.root.child[path.pop(0)], path, opperation, lock_change)): 
                return 1
            return 0

    # This will print the nodes of the tree in 
    # depth-first-search fashion.
    def debug_traverse(self, node):
        print("nodeID: ", node.nodeID)
        print("parent: ", node.parent.nodeID)
        print("locks: ", node.locks)
        print("---------")
        for key in node.child:
            try:
                node.child[key]
            except (IndexError, KeyError):
                return
            else:
                print(node.nodeID)
                print(u'\u2193')
                self.debug_traverse(node.child[key])

    def debug_print(self):
        print("root")
        print("---------")
        for key in self.root.child:
            try:
                self.root.child[key]
            except (IndexError, KeyError):
                pass
            else:
                print('root')
                print(u'\u2193')
                self.debug_traverse(self.root.child[key])
        print("END OF TREE")
        print("")



tree = lockTree()
tree.debug_print()
print(" ")
path1 = ["Grades", 1, 58, 275]
path2 = ["Grades", 0, 68, 175]
path3 = ["Grades", 0, 68, 177]
path4 = ["Grades", 1, 55, 275]
add = "add"
remove= "remove"
tree.change_lock(path1, add, 3)
path1 = ["Grades", 1, 58, 275]
tree.change_lock(path2, add, 1)
path2 = ["Grades", 0, 68, 175]
tree.change_lock(path3, add, 4)
path3 = ["Grades", 0, 68, 177]
tree.change_lock(path3, add, 4)
path3 = ["Grades", 0, 68, 177]
tree.change_lock(path1, add, 2)
path1 = ["Grades", 1, 58, 275]
tree.debug_print()

tree.change_lock(path1, remove, 2)
path1 = ["Grades", 1, 58, 275]
tree.change_lock(path2, remove, 1)
path2 = ["Grades", 0, 68, 175]
tree.debug_print()