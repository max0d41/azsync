import gevent
import logging
import cPickle

from gevent.lock import Semaphore
from gevent.event import Event
from gevent.queue import Queue

from azrpc import AZRPCTimeout

logger = logging.getLogger(__name__)


class RPCSyncListener(object):
    def __init__(self):
        self.id = 1
        self.queue = Queue()

    def add(self, action, data):
        id = self.id
        self.id += 1
        self.queue.put((id, action, data))


class RPCSync(object):
    def __init__(self, rpc, name, instance_id=None, target=None):
        self.name = name
        self.is_master = True if instance_id is None else False
        rpc_name = '%s/%s/sync' % (__name__, name)

        if self.is_master:
            self._lock = Semaphore()
            self._listeners = set()
            rpc.add(self._push_loop, rpc_name)
        else:
            self.live_event = Event()
            self._stream = lambda: rpc.stream(target, rpc_name, instance_id)
            self._greenlet = None

    def start(self):
        if not self.is_master:
            assert self._greenlet is None, 'Already running'
            self._greenlet = gevent.spawn(self._pull_loop)

    def stop(self):
        if not self.is_master and self._greenlet:
            self._greenlet.kill()
            self._greenlet = None

    def wait_live(self, timeout=None):
        if not self.is_master:
            assert self._greenlet is not None, 'Not running'
            self.live_event.wait(timeout=timeout)

    # Push functions

    def add(self, action, data):
        assert self.is_master
        with self._lock:
            for listener in self._listeners:
                listener.add(action, data)

    def _push_loop(self, instance_id):
        assert self.is_master
        with self._lock:
            listener = RPCSyncListener()
            data = list(self.on_init_push_loop())
            listener.add('init', data)
            self._listeners.add(listener)
        try:
            while True:
                yield listener.queue.get()
        except AZRPCTimeout:
            logger.warning('RPC sync push "%s" to "%s" timed out', self.name, instance_id)
        except Exception as e:
            logger.exception('RPC sync push "%s" to "%s" got an error: %s', self.name, instance_id, e)
        finally:
            self._listeners.discard(listener)

    def on_init_push_loop(self):
        raise NotImplementedError()

    # Pull functions

    def _pull_loop(self):
        assert not self.is_master
        while True:
            self.live_event.clear()
            try:
                state = 'init'
                next_id = 1
                for id, action, data in self._stream():
                    if next_id != id:
                        raise RuntimeError('Out of sync: %s / %s' % (next_id, id))
                    next_id = id + 1

                    if action == 'init':
                        assert state == 'init', state
                        not_found_ids = set(self.get_all_ids())
                        for d in data:
                            if isinstance(d, basestring):
                                d = cPickle.loads(d)
                            self.on_update(d)
                            not_found_ids.discard(d['id'])
                        if not_found_ids:
                            self.on_not_found_ids(not_found_ids)
                        del not_found_ids
                        state = 'live'
                        self.live_event.set()
                    elif action == 'update':
                        assert state == 'live', state
                        if isinstance(data, basestring):
                            data = cPickle.loads(data)
                        self.on_update(data)
                    elif action == 'del':
                        assert state == 'live', state
                        self.on_delete(data)
                    else:
                        raise RuntimeError('Invalid action: %s / %s' % (state, action))
            except AZRPCTimeout:
                logger.warning('RPC sync pull "%s" timed out', self.name)
            except Exception as e:
                logger.exception('RPC sync pull "%s" got an error: %s', self.name, e)
            gevent.sleep(0.1)

    def get_all_ids(self):
        assert not self.is_master
        raise NotImplementedError()

    def on_not_found_ids(self, ids):
        assert not self.is_master
        raise NotImplementedError()

    def on_update(self, data):
        assert not self.is_master
        raise NotImplementedError()

    def on_delete(self, id):
        assert not self.is_master
        raise NotImplementedError()


class RPCPusher(object):
    _rpc_members_current = None
    _rpc_members_current_serialized = None

    def rpc_serialize(self, use_cache=False, serialized=True):
        if not use_cache or self._rpc_members_current is None:
            data = dict()
            for key in self.__rpc_members__:
                data[key] = getattr(self, key)
            self._rpc_members_current = data
            self._rpc_members_current_serialized = cPickle.dumps(data)
        return self._rpc_members_current_serialized if serialized else self._rpc_members_current


class RPCPuller(object):
    def __init__(self, data):
        self._rpc_data = data

    def __getattr__(self, key):
        if key in self.__rpc_members__:
            return self._rpc_data[key]
        return getattr(super(RPCPuller, self), key)

    def __setattr__(self, key, value):
        assert key not in self.__rpc_members__, key
        return super(RPCPuller, self).__setattr__(key, value)
