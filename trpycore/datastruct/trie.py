from collections import deque

class TrieNode(object):
    """Represents a Trie node.

    Each node represents a single letter in the Trie.
    This letter / node may represent a valid item 
    (is_item True) which was explicitly added 
    with optional data, or simply a node needed
    to represent an explicitly added child node. 
    """

    def __init__(self, is_item=False, data=None):
        """TrieNode constructor.

        Args:
            is_item: If True, indicates that this node
                represents a value that was inserted
                directly by the end user.
            data: Optional user data which should be associated
                with the node. This data will only ever be  present
                for nodes with  is_item set to True.
        """
        self.is_item = is_item
        self.data = data

        #Dict of children nodes, where the key is
        #a single character and the value is a TrieNode.
        self.child_nodes = {}


class Trie(object):
    """Trie data structure (Prefix Tree).

    Prefix tree for quick lookups. This is
    a good choice for an autocomplete data
    structure.
    """
    def __init__(self):
        """Trie constructor."""
        self.root = TrieNode()

    def clear(self):
        """Clear all nodes."""
        self.root = TrieNode()

    def insert(self, value, data=None):
        """Insert a new value into the Trie.
        
        This method will insert the value into
        the Trie if it does not exist or update
        the existing value.

        Args:
            value: String value to insert into
                the Trie.
            data: Optional data to associate with
                the value. The data will be returned
                along with the value when it's 
                retrieved.
        """
        node = self.root
        for character in value:
            if character not in node.child_nodes:
                node.child_nodes[character] = TrieNode()
            node = node.child_nodes[character]
        node.is_item = True
        node.data = data

    def remove(self, value):
        """Remove the node representing value.

        Args:
            value: String value to remove if present.
        """

        prev_node, node  = (None, self.root)
        
        #find the node to remove and its parent node
        for character in value:
            if character in node.child_nodes:
                prev_node = node
                node = node.child_nodes[character]
            else:
                #Node does not exist so nothing to remove.
                node = None
                break
        
        #If node was found, remove it.
        if prev_node and node:
            #If node has children adjust flag and set data to None.
            #Otherwise it's safe to remove the node entirely.
            if node.child_nodes:
                node.is_item = False
                node.data = None
            else:
                del prev_node.child_nodes[character]
    
    def _get_node(self, value):
        """Get the node for value.

        Args:
            value: String value of node.
        
        Returns:
            TrieNode if value is found, None otherwise.
        """
        if not value:
            return self.root

        node = self.root
        for character in value:
            if character in node.child_nodes:
                node = node.child_nodes[character]
            else:
                node = None
                break
        return node                    
    
    def get(self, value):
        """Get the node data for value.

        Args:
            value: String value of node.
        
        Returns:
            data if value is found, None otherwise.
        """
        node = self._get_node(value)
        if node:
            return node.data
        else:
            return None
    
    def depth_first(self, value=None, max_results=None):
        """Depth first generator.
        
        Depth first traversal of the Trie, which will yield
        (value, data) tuples.

        Args:
            value: String value specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
        
        Yields:
            (value, data) tuple.
        """
        value = value or ""
        node = self._get_node(value)
        
        if node is None:
            raise StopIteration

        queue = deque([(value, node)])
        
        results = 0
        while queue:
            value, node = queue.pop()

            for character, n in node.child_nodes.items():
                queue.append((value+character, n))

            if node.is_item:
                if max_results is not None and results >= max_results:
                    raise StopIteration
                else:
                    yield (value, node.data)
                    results += 1

        raise StopIteration

    def breadth_first(self, value=None, max_results=None):
        """Breadth first generator.
        
        Breadth first traversal of the Trie, which will yield
        (value, data) tuples.

        Args:
            value: String value specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
        
        Yields:
            (value, data) tuple.
        """
        value = value or ""
        node = self._get_node(value)

        if node is None:
            raise StopIteration

        queue = deque([(value, node)])
        
        results = 0
        while queue:
            value, node = queue.pop()

            if node.is_item:
                if max_results is not None and results >= max_results:
                    raise StopIteration
                else:
                    yield (value, node.data)
                    results += 1

            for c, n in node.child_nodes.items():
                queue.appendleft((value+c, n))

        raise StopIteration

    def find(self, prefix, max_results=None, breadth_first=True):
        """Find all values starting with prefix.

        Args:
            prefix: String prefix to match.
            max_results: Optional integer controlling
                max number of results to return.
            breadth_first: If True, results will be
                found using breadth first traversal,
                otherwise a depth first traversal 
                will be used.
        
        Returns:
            list of (value, data) tuples.
        """
        if breadth_first:
            method = self.breadth_first
        else:
            method = self.depth_first
            
        return [(value, data) for value, data in method(prefix, max_results)]
