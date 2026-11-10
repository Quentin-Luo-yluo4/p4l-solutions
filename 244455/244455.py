import multiprocessing

def countChunk(chunk: str, pattern: str, resultQueue: multiprocessing.Queue) -> None:
    total = 0
    stepLen = len(pattern)

    for i in range(len(chunk) + 1 - stepLen):
        if chunk[i : i + stepLen] == pattern:
            total += 1

    resultQueue.put(total)


def parallel_count_substring(text: str, pattern: str, numProcs: int) -> int:
    """
    Count all (possibly overlapping) occurrences of `pattern` in `text`
    using multiprocessing.

    Args:
        text (str): The string to search.
        pattern (str): The substring to count.
        num_procs (int): Number of worker processes.

    Returns:
        int: Total number of (possibly overlapping) occurrences of pattern in text.
    """
    startIndex = 0
    total = 0
    stepLen = len(text) // numProcs
    resultQueue = multiprocessing.Queue()
    listp = []

    for i in range(numProcs):
        chunk = ""

        if i < numProcs - 1:
            chunk = text[startIndex : startIndex + stepLen + len(pattern) - 1]

        else:
            chunk = text[startIndex :]
        
        p = multiprocessing.Process(target = countChunk, args = (chunk, pattern, resultQueue))
        startIndex += stepLen
        p.start()
        listp.append(p)

    for p in listp:
        total += resultQueue.get()

    for p in listp:
        p.join()

    return total
