from collections import deque

class TrieNode(object):
    """Represents a Trie node.

    Each node represents a single letter of a key
    in the Trie.  This letter / node may represent
    a valid item (is_item True) which was explicitly
    added with optional value, or simply a node needed
    to represent an explicitly added child node. 
    """

    def __init__(self, is_item=False, value=None):
        """TrieNode constructor.

        Args:
            is_item: If True, indicates that this node
                represents a value that was inserted
                directly by the end user.
            value: Optional value which should be associated
                with the node. This value will only ever be
                present for nodes with  is_item set to True.
        """
        self.is_item = is_item
        self.value = value

        #Dict of children nodes, where the key is
        #a single character and the value is a TrieNode.
        self.child_nodes = {}


class Trie(object):
    """Trie data structure (Prefix Tree).

    Prefix tree for quick lookups.
    """
    def __init__(self, dict=None, **kwargs):
        """Trie constructor.

        Args:
            dict: Optional dict to populate Trie from.
            kwargs: Optional keyword args to populate Trie from.
        """
        self.root = TrieNode()
        self.update(dict, **kwargs)

    def _get_node(self, key):
        """Get the value for node key.

        Args:
            key: String key of node.
        
        Returns:
            TrieNode if value is found, None otherwise.
        """
        if not key:
            return self.root

        node = self.root
        for character in key:
            if character in node.child_nodes:
                node = node.child_nodes[character]
            else:
                node = None
                break
        return node                    
    
    def __contains__(self, key):
        """Returns True if key in Trie, False otherwise."""
        if self._get_node(key) is not None:
            return True
        else:
            return False
    
    def __delitem__(self, key):
        """Remove key from Trie."""
        return self.remove(key)

    def __getitem__(self, key):
        """Get value by trie[key].
        
        Returns:
            value for key.
        Raises:
            KeyError if key not found.
        """
        node = self._get_node(key)
        if node is not None:
            return node.value
        else:
            raise KeyError
    
    def __iter__(self):
        """Breadth first (key, value) iterator."""
        return self.breadth_first()

    def clear(self):
        """Clear all nodes."""
        self.root = TrieNode()

    def insert(self, key, value=None):
        """Insert a new key / value.
        
        This method will insert the key into
        the Trie if it does not exist or update
        the existing value if it does.

        Args:
            key: String key to insert into
                the Trie.
            value: Optional value to associate with
                the key. The value will be returned
                along with the key when it's 
                retrieved.
        """
        node = self.root
        for character in key:
            if character not in node.child_nodes:
                node.child_nodes[character] = TrieNode()
            node = node.child_nodes[character]
        node.is_item = True
        node.value = value
    
    def update(self, dict=None, **kwargs):
        """Update Trie per dict or keyword args.

        key, value pairs will be created if they do
        not already exist, otherwise they will be
        created.

        Args:
            dict: Optional dict to populate Trie from.
            kwargs: Optional keywords args to populate Trie from.
        """
        if dict:
            for k,v in dict.items():
                self.insert(k, v)
        
        for k,v in kwargs.items():
            self.insert(k, v)

    def remove(self, key):
        """Remove key from Trie.

        Args:
            key: String key to remove if present.
        """

        prev_node, node  = (None, self.root)
        
        #find the node to remove and its parent node
        for character in key:
            if character in node.child_nodes:
                prev_node = node
                node = node.child_nodes[character]
            else:
                #Node does not exist so nothing to remove.
                node = None
                break
        
        #If node was found, remove it.
        if prev_node and node:
            #If node has children adjust flag and set value to None.
            #Otherwise it's safe to remove the node entirely.
            if node.child_nodes:
                node.is_item = False
                node.value = None
            else:
                del prev_node.child_nodes[character]
    
    def get(self, key, default=None):
        """Get the value for node key.

        Args:
            key: String key of node.
            default: Optional value to return
                if key is not found.
        
        Returns:
             key's value is found, default otherwise.
        """
        node = self._get_node(key)
        if node:
            return node.value
        else:
            return default
    
    def depth_first(self, key=None, max_results=None, include_values=True):
        """Depth first generator.
        
        Depth first traversal of the Trie, which will yield
        (key, value) tuples.

        Args:
            key: String key specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
        
        Yields:
            (key, value) tuple if include_values is True.
            key if include_values is False.
        """
        key = key or ""
        node = self._get_node(key)
        
        if node is None:
            raise StopIteration

        queue = deque([(key, node)])
        
        results = 0
        while queue:
            key, node = queue.pop()

            for character, n in node.child_nodes.items():
                queue.append((key+character, n))

            if node.is_item:
                if max_results is not None and results >= max_results:
                    raise StopIteration
                else:
                    if include_values:
                        yield (key, node.value)
                    else:
                        yield key
                    results += 1

        raise StopIteration

    def breadth_first(self, key=None, max_results=None, include_values=True):
        """Breadth first generator.
        
        Breadth first traversal of the Trie, which will yield
        (key, value) tuples.

        Args:
            key: String key specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
        
        Yields:
            (key, value) tuple if include_values is True.
            key if include_values is False
        """
        key = key or ""
        node = self._get_node(key)

        if node is None:
            raise StopIteration

        queue = deque([(key, node)])
        
        results = 0
        while queue:
            key, node = queue.pop()

            if node.is_item:
                if max_results is not None and results >= max_results:
                    raise StopIteration
                else:
                    if include_values:
                        yield (key, node.value)
                    else:
                        yield key
                    results += 1

            for c, n in node.child_nodes.items():
                queue.appendleft((key+c, n))

        raise StopIteration
    
    def keys(self):
        """Return list of keys."""
        return [key for key in self.breadth_first(include_values=False)]

    def items(self):
        """Return list of all (key, value) tuples."""
        return self.find()

    def find(self, prefix=None, max_results=None, breadth_first=True):
        """Find all (key, value) tuples with keys starting with prefix.

        Args:
            prefix: Optional string prefix to match.
            max_results: Optional integer controlling
                max number of results to return.
            breadth_first: If True, results will be
                found using breadth first traversal,
                otherwise a depth first traversal 
                will be used.
        
        Returns:
            list of (key, value) tuples.
        """
        if breadth_first:
            method = self.breadth_first
        else:
            method = self.depth_first
            
        return [(key, value) for key, value in method(prefix, max_results)]



class MultiTrieNode(object):
    """Represents a Multi Trie node.

    Each node represents a single letter of a key
    in the Trie.  This letter / node may represent
    one or more valid items (is_item True) which was explicitly
    added with optional value, or simply a node needed
    to represent an explicitly added child node. 
    """

    def __init__(self, is_item=False, values=None):
        """MultiTrieNode constructor.

        Args:
            is_item: If True, indicates that this node
                represents one or more values inserted
                directly by the end user.
            values: Optional list of values which should be
                associated with the node. values will only
                be present in list for nodes with  is_item
                set to True.
        """
        self.is_item = is_item
        self.values = values

        #Dict of children nodes, where the key is
        #a single character and the value is a MultiTrieNode.
        self.child_nodes = {}


class MultiTrie(object):
    """MultiTrie data structure (Prefix Tree).

    Prefix tree for quick lookups with support for storing
    multiple entries for the same key.
    """
    def __init__(self, dict=None, **kwargs):
        """Trie constructor.

        Args:
            dict: Optional dict to populate Trie from.
            kwargs: Optional keyword args to populate Trie from.
        """
        self.root = MultiTrieNode()
        self.update(dict, **kwargs)

    def _get_node(self, key):
        """Get the value for node key.

        Args:
            key: String key of node.
        
        Returns:
            MultiTrieNode if value is found, None otherwise.
        """
        if not key:
            return self.root

        node = self.root
        for character in key:
            if character in node.child_nodes:
                node = node.child_nodes[character]
            else:
                node = None
                break
        return node                    
    
    def __contains__(self, key):
        """Returns True if key in Trie, False otherwise."""
        if self._get_node(key) is not None:
            return True
        else:
            return False
    
    def __delitem__(self, key):
        """Remove key from Trie."""
        return self.remove(key)

    def __getitem__(self, key):
        """Get value by trie[key].
        
        Returns:
            list of values for key.
        Raises:
            KeyError if key not found.
        """
        node = self._get_node(key)
        if node is not None:
            return node.values
        else:
            raise KeyError
    
    def __iter__(self):
        """Breadth first (key, value) iterator."""
        return self.breadth_first()

    def clear(self):
        """Clear all nodes."""
        self.root = MultiTrieNode()

    def insert(self, key, value=None):
        """Insert a new key / value.
        
        This method will insert the key into
        the Trie if it does not exist or update
        the existing value if it does.

        Args:
            key: String key to insert into
                the Trie.
            value: Optional value to associate with
                the key. The value will be returned
                along with the key when it's 
                retrieved.
        """
        node = self.root
        for character in key:
            if character not in node.child_nodes:
                node.child_nodes[character] = MultiTrieNode()
            node = node.child_nodes[character]
        node.is_item = True
        if node.values is None:
            node.values = []
        node.values.append(value)
    
    def update(self, dict=None, **kwargs):
        """Update Trie per dict or keyword args.

        key, value pairs will be created if they do
        not already exist, otherwise they will be
        created.

        Args:
            dict: Optional dict to populate Trie from.
            kwargs: Optional keywords args to populate Trie from.
        """
        if dict:
            for k,v in dict.items():
                self.insert(k, v)
        
        for k,v in kwargs.items():
            self.insert(k, v)

    def remove(self, key):
        """Remove key from Trie.

        Args:
            key: String key to remove if present.
        """

        prev_node, node  = (None, self.root)
        
        #find the node to remove and its parent node
        for character in key:
            if character in node.child_nodes:
                prev_node = node
                node = node.child_nodes[character]
            else:
                #Node does not exist so nothing to remove.
                node = None
                break
        
        #If node was found, remove it.
        if prev_node and node:
            #If node has children adjust flag and set value to None.
            #Otherwise it's safe to remove the node entirely.
            if node.child_nodes:
                node.is_item = False
                node.values = None
            else:
                del prev_node.child_nodes[character]
    
    def get(self, key, default=None):
        """Get the values for node key.

        Args:
            key: String key of node.
            default: Optional value to return
                if key is not found.
        
        Returns:
             list of key's value is found, default otherwise.
        """
        node = self._get_node(key)
        if node:
            return node.values
        else:
            return default
    
    def depth_first(self, key=None, max_results=None, include_values=True):
        """Depth first generator.
        
        Depth first traversal of the Trie, which will yield
        (key, value) tuples.

        Args:
            key: String key specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
            include_values: Optional boolean indicating if
                (key, value) tuples or only keys should
                be yielded.
        
        Yields:
            (key, value) tuple if include_values is True.
            key if include_values is False.
        """
        key = key or ""
        node = self._get_node(key)
        
        if node is None:
            raise StopIteration

        queue = deque([(key, node)])
        
        results = 0
        while queue:
            key, node = queue.pop()

            for character, n in node.child_nodes.items():
                queue.append((key+character, n))

            if node.is_item:
                if include_values:
                    for value in node.values:
                        if max_results is not None and results >= max_results:
                            raise StopIteration
                        else:
                            yield (key, value)
                            results += 1
                else:
                    if max_results is not None and results >= max_results:
                        raise StopIteration
                    else:
                        yield key
                        results += 1

        raise StopIteration

    def breadth_first(self, key=None, max_results=None, include_values=True):
        """Breadth first generator.
        
        Breadth first traversal of the Trie, which will yield
        (key, value) tuples.

        Args:
            key: String key specifying node from which
                to start iteration.
            max_results: Optional integer specifying the
                maximum number of results to yield.
            include_values: Optional boolean indicating if
                (key, value) tuples or only keys should
                be yielded.
        
        Yields:
            (key, value) tuple if include_values is True.
            key if include_values is False
        """
        key = key or ""
        node = self._get_node(key)

        if node is None:
            raise StopIteration

        queue = deque([(key, node)])
        
        results = 0
        while queue:
            key, node = queue.pop()

            if node.is_item:
                if include_values:
                    for value in node.values:
                        if max_results is not None and results >= max_results:
                            raise StopIteration
                        else:
                            yield (key, value)
                            results += 1
                else:
                    if max_results is not None and results >= max_results:
                        raise StopIteration
                    else:
                        yield key
                        results += 1

            for c, n in node.child_nodes.items():
                queue.appendleft((key+c, n))

        raise StopIteration
    
    def keys(self):
        """Return list of keys."""
        return [key for key in self.breadth_first(include_values=False)]

    def items(self):
        """Return list of all (key, value) tuples."""
        return self.find()

    def find(self, prefix=None, max_results=None, breadth_first=True):
        """Find all (key, value) tuples with keys starting with prefix.

        Args:
            prefix: Optional string prefix to match.
            max_results: Optional integer controlling
                max number of results to return.
            breadth_first: If True, results will be
                found using breadth first traversal,
                otherwise a depth first traversal 
                will be used.
        
        Returns:
            list of (key, value) tuples.
        """
        if breadth_first:
            method = self.breadth_first
        else:
            method = self.depth_first
            
        return [(key, value) for key, value in method(prefix, max_results)]
