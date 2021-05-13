from redis import Redis
from rediscluster import RedisCluster
from multiprocessing import Process, Pipe
import time

REDIS_CHANNEL_NAME = "casbin-role-watcher"


def redis_casbin_subscription(process_conn, redis_host=None, redis_port=None,
                              startup_nodes=None, delay=0):
    # in case we want to delay connecting to redis (redis connection failure)
    time.sleep(delay)
    if startup_nodes:
        r = RedisCluster(startup_nodes)
    else:
        r = Redis(redis_host, redis_port)
    p = r.pubsub()
    p.subscribe(REDIS_CHANNEL_NAME)
    print("Waiting for casbin policy updates...")
    while True and r:
        # wait 20 seconds to see if there is a casbin update
        try:
            message = p.get_message(timeout=20)
        except Exception as e:
            print("Casbin watcher failed to get message from redis due to: {}"
                  .format(repr(e)))
            p.close()
            r = None
            break

        if message and message.get('type') == "message":
            print("Casbin policy update identified.."
                  " Message was: {}".format(message))
            try:
                process_conn.send(message)
            except Exception as e:
                print("Casbin watcher failed sending update to piped"
                      " process due to: {}".format(repr(e)))
                p.close()
                r = None
                break


class RedisWatcher(object):
    def __init__(self, redis_host=None, redis_port=None, redis_startup_nodes=None, start_process=True):
        if (redis_host is not None or redis_port is not None) and redis_startup_nodes is not None:
            raise RuntimeError("Both cluster and standalone settings are specified "
                               "on RedisWatcher. Either set redis_startuo_nodes to "
                               "None, or set both redis_host and redis_port to Nne")
        if not redis_startup_nodes and not redis_host:
            redis_host = '127.0.0.1'
        if isinstance(redis_startup_nodes, str):
            redis_startup_nodes = [{"host": host, "port": 6379} for host in redis_startup_nodes.split(",")]
        self.startup_nodes = redis_startup_nodes
        self.redis_url = redis_host
        self.redis_port = redis_port
        self.subscribed_process, self.parent_conn = \
            self.create_subscriber_process(start_process)

    def create_subscriber_process(self, start_process=True, delay=0):
        parent_conn, child_conn = Pipe()
        p = Process(target=redis_casbin_subscription,
                    args=(child_conn, self.redis_url, self.redis_port, self.startup_nodes, delay),
                    daemon=True)
        if start_process:
            p.start()
        return p, parent_conn

    def set_update_callback(self, fn):
        self.update_callback = fn

    def update_callback(self):
        print('callback called because casbin role updated')

    def update(self):
        r = Redis(self.redis_url, self.redis_port)
        r.publish(REDIS_CHANNEL_NAME, 'casbin policy'
                                      ' updated at {}'.format(time.time()))

    def should_reload(self):
        try:
            if self.parent_conn.poll():
                message = self.parent_conn.recv()
                return True
        except EOFError:
            print("Child casbin-watcher subscribe prococess has stopped, "
                  "attempting to recreate the process in 10 seconds...")
            self.subscribed_process, self.parent_conn = \
                self.create_subscriber_process(delay=10)
            return False
