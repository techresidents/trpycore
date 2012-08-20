from trpycore.atomic.value import AtomicValue

class AtomicUpdateException(Exception):
    pass

class Atomic(object):
    """Atomic class provides lockless, atomic operations.
    
    This class acts as a wrapper around a given object
    providing atomic operations through the
    compare_and_set method.

    Usage:
        v = Atomic(1)
        v.update(lambda v: v+1)
    """

    def __init__(self, value=None):
        """Atomic constructor.

        Args:
            value: Object to wrap for atomic operations.
        """
        self.value = AtomicValue(value)

    def get(self):
        """Get the current value.

        Returns:
            Current value.
        """
        return self.value.get()

    def set(self, value):
        """Set the value.

        Args:
            value: new value to set
        Returns:
            old value
        """
        return self.value.set(value)

    def compare_and_set(self, expected_value, new_value):
        """Atomically compare and set value.

        Compare the current value to the expected_value and
        only set the new_value if the current value matches
        the expected_value.

        Returns:
            new value
        """
        return self.value.compare_and_set(expected_value, new_value)
    
    def update(self, func, spin=True):
        """Atomically update the value using func.

        Update the current_value with output of
        func(current_value) in an atomic manner.
        Note that func may be called several times,
        if spin=True and there is contention.

        Args:
            func: callable taking the current value as
                its sole argument and returning the
                new value.
            spin: boolean which if True indicates that
                failures due to contention should be
                retried until successful. Otherwise
                an AtomicUpdateException will be raised.
        Returns:
            new value
        Raises:
            AtomicUpdateException if spin=False and
            the update failes due to contention.
        """
        while True:
            current_value = self.get()
            new_value = func(current_value)
            if self.compare_and_set(current_value, new_value):
                break
            if not spin:
                raise AtomicUpdateException("atomic update failed")
        return new_value

    def increment(self, spin=True):
        """Atomic increment.
        
        Equivalent to v.update(lambda v: v+1)
        Returns:
            new value
        """
        return self.update(lambda v: v+1)

    def decrement(self, spin=True):
        """Atomic decrement.

        Equivalent to v.update(lambda v: v-1)

        Returns:
            new value
        """
        return self.update(lambda v: v-1)
