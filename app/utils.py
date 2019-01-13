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

def measure_end(partitionkey,
                measure_start,
                step):
    log = logging.getLogger(__name__)
    log.debug(f'{partitionkey}: {step} took {time.time() - measure_start}')