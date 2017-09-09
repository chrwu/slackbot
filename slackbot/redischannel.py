import redis
import time
import funcy as fy
import pickle
import queue
import threading


from collections import defaultdict


def ensure_ascii_bytes(chars):
    if isinstance(chars, str):
        return chars.encode('ascii')
    elif isinstance(chars, bytes):
        return chars
    else:
        return str(chars).encode('ascii')


class RedisChannel:
    def __init__(self, host='localhost', port=6379, db=0):
        self.r = redis.StrictRedis(host, port=port, db=db)
        self.pubsub = self.r.pubsub()
        self.channel_funcs = defaultdict(set)
        self.pkg_queue = queue.Queue()
        self.thread = threading.Thread(target=self.loop_package_to_queue)
        self.thread.start()

    def loop_package_to_queue(self):
        while True:
            if len(self.pubsub.channels) == 0:
                continue
            pkg = self.pubsub.get_message()
            if pkg is not None:
                self.pkg_queue.put(pkg)
            time.sleep(0.05)

    def get_pkg(self):
        try:
            pkg = self.pkg_queue.get_nowait()
            return pkg
        except queue.Empty:
            return None

    @fy.collecting
    def get_all_pkgs(self):
        while True:
            pkg = self.get_pkg()
            if pkg is None:
                break
            yield pkg

    def dispatch_pkg(self):
        pkgs = self.get_all_pkgs()
        for pkg in pkgs:
            if pkg['type'] == 'message':
                channel = ensure_ascii_bytes(pkg['channel'])
                data = pickle.loads(pkg['data'])
                if channel in self.channel_funcs:
                    for f in self.channel_funcs[channel]:
                        f(data)

                else:
                    self.pkg_queue.put(pkg)

    def listen_to(self, channel):
        channel = ensure_ascii_bytes(channel)

        def _listen_to(f):
            if channel not in self.pubsub.channels:
                self.pubsub.subscribe(channel)

            self.channel_funcs[channel].add(f)
            return f

        return _listen_to

    def subscribe(self, channel):
        channel = ensure_ascii_bytes(channel)
        if channel not in self.pubsub.channels:
            self.pubsub.subscribe(channel)

    def get_channel_data(self, channel, wait=True):
        channel = ensure_ascii_bytes(channel)
        while True:
            pkg = self.get_pkg()

            if pkg and pkg['type'] == 'message':
                pkg_channel = ensure_ascii_bytes(pkg['channel'])
                if channel == pkg_channel:
                    data = pickle.loads(pkg['data'])
                    return data
                else:
                    self.pkg_queue.put(pkg)
            if wait:
                time.sleep(0.05)
            else:
                return

    def send_to_channel(self, channel, data):
        channel = ensure_ascii_bytes(channel)
        data = pickle.dumps(data)
        return self.r.publish(channel, data)
