import abc

class Counter(object):
    """Counter abstract base class."""
    
    @abc.abstractmethod
    def get(self):
        """Get current counter value.

        Returns:
            current counter value
        """
        return

    @abc.abstractmethod
    def set(self, value):
        """Set counter value.

        Args:
            value: new counter value
        Returns:
            previous counter value
        """
        return

    @abc.abstractmethod
    def increment(self, n=1):
        """Increment counter value by n.

        Args:
            n: optional value to increment counter by
        Returns:
            new counter value.
        """
        return

    @abc.abstractmethod
    def decrement(self, n=1):
        """Decrement counter value by n.

        Args:
            n: optional value to decrement counter by
        Returns:
            new counter value.
        """
        return

class Counters(object):
    """Counters abstract base class."""
    
    @abc.abstractmethod
    def get_counter(self, counter_name):
        """Get Counter object.

        If the Counter object does not exist, It will created
        and initialized with initial_value.
        
        Args:
            counter_name: counter name
        Returns:
            Counter object
        """
        return

    @abc.abstractmethod
    def get(self, counter_name):
        """Get counter value.

        If the Counter object does not exist, It will created
        and initialized with initial_value.
        
        Args:
            counter_name: counter name
        Returns:
            current counter value
        """
        return

    @abc.abstractmethod
    def set(self, counter_name, value):
        """Set counter value.

        If the Counter object does not exist, It will created
        and initialized with initial_value, and then
        set to the given value.
        
        Args:
            counter_name: counter name
            value: new counter value
        Returns:
            previous counter value
        """
        return

    @abc.abstractmethod
    def increment(self, counter_name, n=1):
        """Increment counter value by n.

        If the Counter object does not exist, It will created
        and initialized with initial_value, and then
        incremented by n.
        
        Args:
            counter_name: counter name
            n: Optional value to increment counter by
        Returns:
            new counter value
        """
        return

    @abc.abstractmethod
    def decrement(self, counter_name, n=1):
        """Decrement counter value by n.

        If the Counter object does not exist, It will created
        and initialized with initial_value, and then
        decremented by n.
        
        Args:
            counter_name: counter name
            n: Optional value to decrement counter by
        Returns:
            new counter value
        """
        return

    @abc.abstractmethod
    def as_dict(self):
        """Return counters as dict of the form {name: value}.

        Returns:
            dict of the form {name: value}
        """
        return
