#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script creates a tree of idle processes with a specified depth and breadth.
The processes are spawned using fork and will wait for a signal to terminate.

Usage:
    create_process_tree.py <number_of_processes> <depth>

Example:
    create_process_tree.py 3 2

This will create a process tree with 3 processes at each level and a depth of 2.
It would look like:

```
$ pstree -a -pg 30881
python3,30881,30881 ./create_process_tree.py 3 2
  ├─python3,30899,30881 ./create_process_tree.py 3 2
  │   ├─python3,30902,30881 ./create_process_tree.py 3 2
  │   ├─python3,30903,30881 ./create_process_tree.py 3 2
  │   └─python3,30906,30881 ./create_process_tree.py 3 2
  ├─python3,30900,30881 ./create_process_tree.py 3 2
  │   ├─python3,30904,30881 ./create_process_tree.py 3 2
  │   ├─python3,30907,30881 ./create_process_tree.py 3 2
  │   └─python3,30909,30881 ./create_process_tree.py 3 2
  └─python3,30901,30881 ./create_process_tree.py 3 2
      ├─python3,30905,30881 ./create_process_tree.py 3 2
      ├─python3,30908,30881 ./create_process_tree.py 3 2
      └─python3,30910,30881 ./create_process_tree.py 3 2
```

Each process will print its PID.

The script also includes a signal handler to terminate the whole process group
when a SIGTERM signal is received.
"""

import argparse
import logging
import os
import signal


def spawn_processes(nb: int, depth: int) -> None:
    """
    Spawn a number of processes in a tree structure.

    Args:
        nb (int): Number of processes to spawn.
        depth (int): Depth of the process tree.
    """
    if depth == 0 or nb <= 0:
        return

    for i in range(nb):
        pid = os.fork()
        if pid == 0:  # Child process
            print(os.getpid(), flush=True)
            logging.info(f"Child PID: {os.getpid()}, Parent PID: {os.getppid()}")
            spawn_processes(nb, depth - 1)
            signal.pause()


def handler(signum: int, frame) -> None:
    """
    Signal handler to terminate the process group.

    Args:
        signum (int): Signal number.
        frame (signal.FrameType): Current stack frame.
    """
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    os.killpg(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a process tree.")
    parser.add_argument("nb", type=int, help="Number of processes to spawn per level")
    parser.add_argument("depth", type=int, help="Depth of the process tree")
    parser.add_argument(
        "--log-level",
        type=str,
        default="warning",
        help="Logging level",
        choices=["debug", "info", "warning", "error", "critical"],
    )
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper())

    os.setpgid(0, 0)

    spawn_processes(args.nb, args.depth)

    signal.signal(signal.SIGTERM, handler)
    signal.pause()
