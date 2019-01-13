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
