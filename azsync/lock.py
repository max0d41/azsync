import logging

from gevent import GreenletExit, Timeout
from gevent.lock import Semaphore
from contextlib import contextmanager
from weakref import WeakValueDictionary, WeakSet

from azrpc import AZRPCTimeout

logger = logging.getLogger(__name__)


# Helper classes

class MySemaphore(object):
    def __init__(self):
        self.sema = Semaphore()


class Waiter(object):
    pass


# Main RPC class

class RPCLock(object):
    def __init__(self, rpc, name, target=None):
        self.rpc = rpc
        self.name = '%s.%s' % (__name__, name)

        self.stats = {
            'requests': 0,
            'already_locked': 0,
            'try_failed': 0,
            'acquired': 0,
            'released': 0,
            'timeout': 0,
            'unexpected': 0,
            'failed': 0,
            'failed_timeout': 0,
            'exceptions': 0,
        }

        self.lock = Semaphore()
        self.locks = WeakValueDictionary()
        self.waiting = WeakSet()

        self._get_lock = rpc.add(self._get_lock, '%s.get_lock' % self.name)
        self._get_lock_stream_sync = lambda *args: self._get_lock.stream_sync(target, *args)

        self._is_locked = rpc.add(self._is_locked, '%s.is_locked' % self.name)
        self._is_locked_execute = lambda *args: self.is_locked.execute(target, *args)

    def get_server_stats(self):
        stats = (
            '{requests} requests, {already_locked} already_locked, '
            '{waiting} waiting, {active} active, '
            '{try_failed} try_failed, {acquired} acquired, {released} released, '
            '{timeout} timeout, {unexpected} unexpected, '
            '{failed} failed, {failed_timeout} failed_timeout, '
            '{exceptions} exceptions'.format(
                active=len(self.locks),
                waiting=len(self.waiting),
                **self.stats))
        return stats

    def _get_lock(self, name, try_=False):
        self.stats['requests'] += 1
        with self.lock:
            if name not in self.locks:
                lock = MySemaphore()
                self.locks[name] = lock
            else:
                lock = self.locks[name]
        if lock.sema.locked():
            self.stats['already_locked'] += 1
            if try_:
                self.stats['try_failed'] += 1
                yield False
                return
        logger.debug('%s: Trying to acquire', name)
        try:
            waiter = Waiter()
            self.waiting.add(waiter)
            with lock.sema:
                del waiter
                self.stats['acquired'] += 1
                logger.debug('%s: Acquired', name)
                try:
                    while True:
                        yield True
                except (GeneratorExit, GreenletExit):
                    self.stats['released'] += 1
                    logger.debug('%s: Released', name)
                except AZRPCTimeout:
                    self.stats['timeout'] += 1
                    logger.info('%s: Timed out', name)
                else:
                    self.stats['unexpected'] += 1
                    logger.warning('%s: Released without error', name)
        except (GeneratorExit, GreenletExit):
            self.stats['failed'] += 1
            logger.warning('%s: Released before getting lock', name)
        except AZRPCTimeout:
            self.stats['failed_timeout'] += 1
            logger.warning('%s: Timed out before getting lock', name)
        except:
            self.stats['exception'] += 1
            logger.exception('Exception at lock %s', name)
            raise

    def _is_locked(self, name):
        with self.lock:
            if name not in self.locks:
                return False
            lock = self.locks[name]
        return lock.sema.locked()

    # Client functions

    @contextmanager
    def get_lock(self, name, try_=False):
        """Acquires or tries to acquire the lock. Returns `True` when the lock is
        acquired.
        """
        lock = Lock(self, name, try_)
        with lock as got_lock:
            yield got_lock

    def locked(self, name):
        return self._is_locked_execute(name)
    is_locked = locked


# Client lock helper class

class Lock(object):
    gen = None
    got = False

    def __init__(self, rpc_lock, name, try_=False):
        self.rpc_lock = rpc_lock
        self.name = name
        self.try_ = try_

    def acquire(self):
        self.gen = self.rpc_lock._get_lock_stream_sync(self.name, self.try_)
        self.got = next(self.gen)
        return self.got

    def release(self):
        assert self.got
        if self.got:
            self.got = False
            del self.gen
            try:
                with Timeout(1):
                    self.locked()
            except Exception:
                pass

    def locked(self):
        return self.rpc_lock._is_locked_execute(self.name)
    is_locked = locked

    def idle(self):
        assert self.got
        try:
            next(self.gen)
        except StopIteration:
            raise AZRPCTimeout('Stream closed while idling')

    def __enter__(self):
        return self.acquire()

    def __exit__(self, type, value, traceback):
        if self.got:
            self.release()
