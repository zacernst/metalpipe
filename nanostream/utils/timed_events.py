import threading
import time


def make_timer(interval=None):
    event_obj = threading.Event()

    def event_loop():
        while 1:
            time.sleep(interval)
            event_obj.set()
            while event_obj.is_set():
                pass

    event_thread = threading.Thread(target=event_loop, daemon=True)
    return event_thread, event_obj

event_thread, event_obj = make_timer()

