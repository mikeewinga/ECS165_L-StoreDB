import threading
import copy
from lstore.config import *
#import config

class Lock:
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
        # reader lock exists
        if self.locks[X] > 0:
            return 0
        # if CurrentLock has S and NewLock wants S
        if(self.locks[S] >= 0 and lock == S):
            return 1
        # if the sum of the CurrentLock and NewLock
        # is less than or equal to 4
        for type in range(3,0,-1):
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
        path = list(path)
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
            if(node.child[new_node.nodeID].total_locks == 0):
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
        path = list(path)
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

class LockManager:
    def __init__(self):
        self.lock_dict = {}
        self.locks = {}
        self.lock_tree = lockTree()
        pass
    
    # path is [table, page_range, page, row]
    def add_lock(self, query_opp, table_name, address):
        thread_id = threading.get_ident()
        target = None
        target = self.lock_dict.get(thread_id)
        if target == None:
            target = {}
            self.lock_dict[thread_id] = target

        #page_range = str(address.pagerange)
        #page = str(address.page)
        #row = str(address.row)
        path_T = (table_name)
        path_PR = (table_name, address.pagerange)
        path_P = (table_name, address.pagerange, address.page)
        path_R = (table_name, address.pagerange, address.page, address.row)

        new_lock = None
        lock_complete = 0

        if query_opp == 1:
            new_lock = Lock(IX, IX, IX, X)
        else:
            new_lock = Lock(IS, IS, IS, S)

        lock = target.get(path_R)
        if lock:
            if new_lock.row > lock.row:
                lock_complete = self.lock_tree.change_lock(path_R, 'remove', lock.row)
                lock_complete = self.lock_tree.change_lock(path_P, 'remove', lock.page)
                lock_complete = self.lock_tree.change_lock(path_PR, 'remove', lock.page_range)
                lock_complete = self.lock_tree.change_lock(path_T, 'remove', lock.table)
                lock_complete = self.lock_tree.change_lock(path_T, 'add', new_lock.table)
                lock_complete = self.lock_tree.change_lock(path_PR, 'add', new_lock.page_range)
                lock_complete = self.lock_tree.change_lock(path_P, 'add', new_lock.page)
                lock_complete = self.lock_tree.change_lock(path_R, 'add', new_lock.row)
                target[path_R] = new_lock
            return 1
        else:
            lock_complete = self.lock_tree.change_lock(path_T, 'add', new_lock.table)
            lock_complete = self.lock_tree.change_lock(path_PR, 'add', new_lock.page_range)
            lock_complete = self.lock_tree.change_lock(path_P, 'add', new_lock.page)
            lock_complete = self.lock_tree.change_lock(path_R, 'add', new_lock.row)
            target[path_R] = new_lock
            return 1
        """
        if lock_complete:
            target[path_R] = new_lock
            #self.lock_dict[thread_id] = self.locks
            return 1
        else:
            return 0
        """
        return 0


    def remove_lock(self):
        thread_id = threading.get_ident()
        target = self.lock_dict.get(thread_id)
        lock_complete = 0

        if target:
            for path, lock in target.items():

                query_opp = lock.row
                path = list(path)
                path_T = (path[0])
                path_PR = (path[0], path[1])
                path_P = (path[0], path[1], path[2])
                path_R = (path[0], path[1], path[2], path[3])

                if query_opp == X:
                    lock_complete = self.lock_tree.change_lock(path_R, 'remove', X)
                    lock_complete = self.lock_tree.change_lock(path_P, 'remove', IX)
                    lock_complete = self.lock_tree.change_lock(path_PR, 'remove', IX)
                    lock_complete = self.lock_tree.change_lock(path_T, 'remove', IX)
                else:
                    lock_complete = self.lock_tree.change_lock(path_R, 'remove', S)
                    lock_complete = self.lock_tree.change_lock(path_P, 'remove', IS)
                    lock_complete = self.lock_tree.change_lock(path_PR, 'remove', IS)
                    lock_complete = self.lock_tree.change_lock(path_T, 'remove', IS)
            del self.lock_dict[thread_id]
        """
        if lock_complete:
            del self.lock_dict[thread_id]
            return 1
        else:
            return 0
        """

"""
# TEST SCRIPT FOR LOCKTREE
tree = lockTree()

print(" ")
path1 = ["Grades", 1, 58, 275]
path2 = ["Grades", 0, 68, 175]
add = "add"
remove= "remove"

path1 = ["Grades", 1, 58, 275]
tree.change_lock(path1, add, 3)
path1 = ["Grades", 1, 58]
tree.change_lock(path1, add, 1)
path1 = ["Grades", 1]
tree.change_lock(path1, add, 1)
path1 = ["Grades"]
tree.change_lock(path1, add, 1)
tree.debug_print()

path1 = ["Grades"]
tree.change_lock(path1, add, 1)
path1 = ["Grades", 1]
tree.change_lock(path1, add, 1)
path1 = ["Grades", 1, 58]
tree.change_lock(path1, add, 1)
path1 = ["Grades", 1, 58, 275]
tree.change_lock(path1, add, 3)
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

"""
#TEST SCRIPT FOR LOCKMANAGER
class Address:
    #Base/Tail flag, Page-range number, Page number, Row number
    def __init__(self, pagerange, flag, pagenumber, row = None):
        self.pagerange = pagerange
        self.flag = flag  # values: 0--base, 1--tail, 2--base page copy used for merge
        self.pagenumber = pagenumber
        self.page = (flag, pagenumber)
        self.row = row

tableName = 'Grades'
address = Address(2, 0, 58, 275)
address1 = Address(2, 0, 58, 277)
address2 = Address(3, 0, 36, 120)

lm =  LockManager()

lm.add_lock(INSERT, tableName, address)
lm.lock_tree.debug_print()
lm.remove_lock()
lm.lock_tree.debug_print()
"""

"""
from address import Address
address = Address(2, 1, 59, 123)
address2 = Address(1, 4, 61, 101)
lm = LockManager()
print("1")
for key in lm.lock_dict:
    for val in lm.lock_dict[key]:
        print(key, val)
lm.add_lock(INSERT, 'Grades', address)
lm.add_lock(INSERT, 'Grades', address2)

print("2")
for key in lm.lock_dict:
    for val in lm.lock_dict[key]:
        print(key, val)

lm.remove_lock()

print("3")
for key in lm.lock_dict:
    for val in lm.lock_dict[key]:
        print(key, val)
"""
