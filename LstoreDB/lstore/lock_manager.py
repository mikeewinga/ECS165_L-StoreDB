import threading
from config import *
#import config

class LockManager:
    def __init__(self):
        self.lock_dict = {}
        self.locks = {}
        self.lock_tree = lockTree()
        pass
    
    # path is [table, page_range, page, row]
    def getlock(self, path, lock_type):
        thread_id = threading.get_ident()
        target = self.lock_dict.get(thread_id)

        new_lock = Lock(lock_type, path.table, path.page_range, path.page, path.row)

        if target:
            if (self.lock_tree.change_lock(path, "add", new_lock)): # if lock added - returns 1
                self.locks[path] = new_lock
                self.lock_dict[thread_id] = self.locks
                return 1
            else:
                return 0
        else:
            self.lock_tree.change_lock(path, "add", new_lock)
            #self.locks[thread_id] = {} #insert into locks


            self.locks[path] = new_lock
            self.lock_dict[thread_id] = self.locks
            return 1

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
        self.total_locks = 0
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
            1| y y y n
            2| y y n n
            3| y n y n
            4| n n n n
        """
        # if the CurrentLock is empty
        if(self.total_locks == 0):
            return 1
        # if CurrentLock has S and NewLock wants S
        if(self.locks[S] >= 0 and lock == S):
            return 1
        # if the sum of the CurrentLock and NewLock
        # is less than or equal to 4
        for type in range(1,3):
            sum = lock
            if(self.locks[type] > 0):
                sum += type
                if(sum < 5):
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
                    node.total_locks += 1
                    return 1
            if(opperation == 'remove'):
                node.remove_lock(lock_change)
                node.total_locks -= 1
                return 1
            return 0
        lock_changed = 0
        try:
            node.child[path[0]]
        #if the child node doens't exist, create it and traverse
        except (IndexError, KeyError):
            new_node = lockNode(node, path.pop(0))
            node.child[new_node.nodeID] = new_node
            lock_changed = self.traverse(new_node, path, opperation, lock_change)
            # if all locks are removed from its children and if 
            # this node has no locks, delete this node
            if(new_node.total_locks == 0):
                del node.child[new_node.nodeID]
            # if its child added a lock, increase/decrease its total lock counter
            if(lock_changed):
                if(opperation == 'add'):
                    node.total_locks += 1
                    return 1
                else: 
                    node.total_locks -= 1
                    return 1
            return 0
        # if the child node exists, traverse it
        else:
            next_node = path.pop(0)
            lock_changed = self.traverse(node.child[next_node], path, opperation, lock_change)
            # if all locks are removed from its children and if 
            # this node has no locks, delete this node
            if(node.child[next_node].total_locks == 0):
                del node.child[next_node]
            # if its child added a lock, increase/decrease its total lock counter
            if(lock_changed):
                if(opperation == 'add'):
                    node.total_locks += 1
                    return 1
                else: 
                    node.total_locks -= 1
                    return 1
            return 0

    # this will start the add or remove lock process
    def change_lock(self, path, opperation, lock_change):
        lock_changed = 0
        try:
            self.root.child[path[0]]
        except (IndexError, KeyError):
            new_node = lockNode(self.root, path.pop(0))
            self.root.child[new_node.nodeID] = new_node
            lock_changed = self.traverse(new_node, path, opperation, lock_change)
            if(lock_changed):
                return 1
            return 0
        else:
            next_node = path.pop(0)
            lock_changed = self.traverse(self.root.child[next_node], path, opperation, lock_change)
            if(lock_changed):
                return 1
            return 0

    # This will print the nodes of the tree in 
    # depth-first-search fashion.
    def debug_traverse(self, node):
        print("nodeID: ", node.nodeID)
        print("parent: ", node.parent.nodeID)
        print("total locks: ", node.total_locks)
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

"""
# TEST SCRIPT FOR LOCKTREE
tree = lockTree()

print(" ")
path1 = ["Grades", 1, 58, 275]
path2 = ["Grades", 0, 68, 175]
add = "add"
remove= "remove"

tree.change_lock(path1, add, 2)
path1 = ["Grades", 1, 58, 275]
tree.change_lock(path1, add, 1)
path1 = ["Grades", 1, 58, 275]
tree.debug_print()

# tree.change_lock(path1, add, 3)
# path1 = ["Grades", 1, 58, 275]
# tree.debug_print()

tree.change_lock(path1, remove, 2)
path1 = ["Grades", 1, 58, 275]
tree.debug_print()

tree.change_lock(path1, remove, 1)
path1 = ["Grades", 1, 58, 275]
tree.debug_print()
"""
