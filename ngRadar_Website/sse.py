import asyncio
import threading


class SSEBroker:
    """
    Fan Kafka events out to every currently connected SSE client.

    Each browser gets its own asyncio.Queue.
    """

    def __init__(self):
        self._subscribers = set()
        self._lock = threading.Lock()

    def subscribe(self):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=100)

        subscriber = (loop, queue)

        with self._lock:
            self._subscribers.add(subscriber)

        return subscriber

    def unsubscribe(self, subscriber):
        with self._lock:
            self._subscribers.discard(subscriber)

    def publish(self, event):
        """
        Called from the Kafka consumer thread.

        Schedule queue writes safely on each subscriber's
        asyncio event loop.
        """
        with self._lock:
            subscribers = list(self._subscribers)

        for loop, queue in subscribers:
            loop.call_soon_threadsafe(
                self._put_event,
                queue,
                event,
            )

    @staticmethod
    def _put_event(queue, event):
        # Don't allow a slow/disconnected browser to grow
        # memory indefinitely.
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass

        queue.put_nowait(event)


sse_broker = SSEBroker()