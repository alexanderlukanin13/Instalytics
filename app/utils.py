import contextlib
import logging
import time


def read_lines(filename):
    """Read non-blank lines from a text file. Strip each line."""
    result = []
    with open(filename) as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            result.append(line)
    return result


@contextlib.contextmanager
def measure_time(partition_key, step_name):
    """Context manager to log operation timings."""
    start_time = time.monotonic()
    yield
    log = logging.getLogger(__name__)
    log.debug(f'{partition_key}: {step_name} took {time.monotonic() - start_time}')
