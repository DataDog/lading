import asyncio
import threading


class Signals:
    def __init__(self):
        self.experiment_started = asyncio.Event()
        self.shutdown = asyncio.Event()
        # Threading version for sync code running in threads
        self._shutdown_thread = threading.Event()

    def set_shutdown(self):
        self.shutdown.set()
        self._shutdown_thread.set()

    def shutdown_is_set(self) -> bool:
        return self._shutdown_thread.is_set()
