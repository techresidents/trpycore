import threading
import sys
import traceback

class StackDumper(threading.Thread):
    """Utility which dumps stack traces for all active threads periodically.

    This is an extremely useful utility for debugging deadlocks.
    Simple instantiate and start the StackDumper and it will
    output stack traces for all threads periodically.
    """

    def __init__(self, interval=60, filename=None):
        """StackDumper constructor.

        Args:
            interval: optional interval in seconds to dump stack traces
            filename: optional filename to write stack traces to.
                If None, stack traces will be written to stdout.
        """
        super(StackDumper, self).__init__()
        self.interval = interval
        self.filename = filename
        self.file = None
        self.exit_event = threading.Event()

        if self.filename is not None:
            self.file = open(self.filename, "w+")
        else:
            self.file = sys.stdout

    def dump_stack(self):
        """Dump stack traces for all threads."""
        self.file.write("BEGIN STACK DUMP\n")
        for thread_id, stackframe in sys._current_frames().iteritems():
            self.file.write("Thread (id=%d)\n" % thread_id)
            traceback.print_stack(stackframe, file=self.file)
        self.file.write("END STACK DUMP\n\n")

    def run(self):
        """Run stack dumper."""
        while not self.exit_event.is_set():
            self.dump_stack()
            self.exit_event.wait(self.interval)
        
        if self.filename and self.file:
            self.file.close()

    def stop(self):
        """Stop stack dumper."""
        self.exit_event.set()

