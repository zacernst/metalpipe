import threading
import time
from nanostream.utils.required_arguments import required_arguments


class RepeateEvent:

    @required_arguments('interval')
    def __init__(self, daemon=True, interval=None, number_of_events=None, start=True):
        self.event_obj = threading.Event()
        self.run = start

        def event_loop():
            while kill_switch.run:
                time.sleep(interval)
                self.event_obj.set()
                while self.event_obj.is_set():
                    pass

        self.event_thread = threading.Thread(target=event_loop, daemon=daemon)
        if self.run:
            self.event_thread.start()

    def start(self):
        self.event_thread.start()

    def kill(self):
        self.run = False




event_thread, event_obj, kill_switch = make_timer(interval=2)
event_thread.start()
counter = 0
while 1:
    print('hi: ' + str(counter))
    counter += 1
    while not event_obj.is_set():
        pass
    event_obj.clear()
    if counter > 3:
        kill_switch.kill()

