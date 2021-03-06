from trpycore.counter.base import Counter, Counters

class BasicCounter(Counter):
    """Non-threadsafe Counter class.
    
    This class is not threadsafe and should be used accordingly.
    """
    def __init__(self, name, value=0):
        """Counter constructor.

        Args:
            name: counter name
            value: Optional initial counter value
        """
        self.name = name
        self.value = 0
    
    def get(self):
        """Get current counter value.

        Returns:
            current counter value
        """
        return self.value

    def set(self, value):
        """Set counter value.

        Args:
            value: new counter value
        Returns:
            previous counter value
        """
        old_value = self.value
        self.value = value
        return old_value

    def increment(self, n=1):
        """Increment counter value by n.

        Args:
            n: optional value to increment counter by
        Returns:
            new counter value.
        """
        self.value += n
        return self.value

    def decrement(self, n=1):
        """Decrement counter value by n.

        Args:
            n: optional value to decrement counter by
        Returns:
            new counter value.
        """
        self.value -= n
        return self.value


class BasicCounters(Counters):
    """Non-threadsafe Counters class.

    This class is not threadsafe and should be used accordingly.
    """
    def __init__(self, initial_value=0, counter_names=None):
        """Counters constructor.

        Args:
            initial_value: Optional initial value for new counters
            counter_names: Optional list of counter_name to
                create counters during initialization. Otherwise,
                counters will automatically be created as needed.
        """
        self.initial_value = initial_value
        self.counters = {}

        for counter_name in counter_names or []:
            counter = BasicCounter(counter_name, self.initial_value)
            self.counters[counter_name] = counter

    def __contains__(self, counter_name):
        """Check if counter_name is contained within Counters.

        Returns:
            True if counter_name exists, False otherwise.
        """
        return counter_name in self.counters

    def __getitem__(self, counter_name):
        """Get counter value for counter_name if it exists.
        
        Returns:
            Counter value if counter_name exists.
        Raises:
            KeyError if counter_name does not exist.
        """
        if counter_name not in self.counters:
            raise KeyError
        return self.counters[counter_name].get()

    def _get_or_create_counter(self, counter_name):
        """Helper method to get or create counters on demand.
        
        Args:
            counter_name: counter name
        Returns:
            Counter object
        """
        if counter_name not in self.counters:
            counter = BasicCounter(counter_name, self.initial_value)
            self.counters[counter_name] = counter
        return self.counters[counter_name]

    def get_counter(self, counter_name):
        """Get Counter object.

        If the Counter object does not exist, It will created
        and initialized with initial_value.
        
        Args:
            counter_name: counter name
        Returns:
            Counter object
        """
        return self._get_or_create_counter(counter_name)

    def get(self, counter_name):
        """Get counter value.

        If the Counter object does not exist, It will created
        and initialized with initial_value.
        
        Args:
            counter_name: counter name
        Returns:
            current counter value
        """
        return self._get_or_create_counter(counter_name).get()

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
        return self._get_or_create_counter(counter_name).set(value)

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
        return self._get_or_create_counter(counter_name).increment(n)

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
        return self._get_or_create_counter(counter_name).decrement(n)

    def as_dict(self):
        """Return counters as dict of the form {name: value}.

        Returns:
            dict of the form {name: value}
        """
        return {k : v.get() for k,v in self.counters.iteritems()}
