import json
import os
from lading_py.capture.line import Line


class JsonlWriter:
    def __init__(self, path: str):
        self._path = path
        # Truncate/create on start
        open(self._path, "w").close()

    def flush(self, lines: list[Line]) -> None:
        if not lines:
            return
        with open(self._path, "a") as f:
            for line in lines:
                f.write(json.dumps(line.to_dict()) + "\n")

    def finalize(self) -> None:
        pass  # file is already flushed
