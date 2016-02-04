import logging

from gevent import GreenletExit
from gevent.lock import Semaphore
from gevent.event import Event
from weakref import WeakValueDictionary, WeakSet

from azrpc import AZRPCTimeout

from .sync import RPCSync, RPCPuller

logger = logging.getLogger(__name__)


class Master(object):
    def __init__(self, id, max_slots):
        self.id = id
        self.max_slots = max_slots
        self.lock = Semaphore()
        self.slots = WeakValueDictionary()

    def serialize(self):
        return {
            'id': self.id,
            'max_slots': self.max_slots,
            'slots': len(self.slots),
            'workers': sum(len(slot.workers) for slot in self.slots.values())
        }


class MasterSlot(object):
    def __init__(self):
        self.workers = WeakSet()


class Waiter(object):
    pass


class SlotKeeper(RPCSync):
    def __init__(self, rpc, name, instance_id=None, target=None):
        super(SlotKeeper, self).__init__(rpc, __name__, instance_id, target)

        self.stats = {
            'requests': 0,
            'full': 0,
            'created_slots': 0,
            'created_workers': 0,
            'empty': 0,
            'acquired': 0,
            'released': 0,
            'timeout': 0,
            'unexpected': 0,
        }

        self.objects = dict()
        self.lock = Semaphore()

        self._acquire_slot = rpc.add(self._acquire_slot, '%s.acquire' % self.name)
        self._acquire_slot_stream_sync = lambda *args: self._acquire_slot.stream_sync(target, *args)

        self.start()

    def get_server_stats(self):
        assert self.is_master
        slots = 0
        workers = 0
        for obj in self.objects.values():
            slots += len(obj.slots)
            for slot in obj.slots.values():
                workers += len(slot.workers)
        stats = (
            '{objects} objects, {slots} slots, {workers} workers, '
            '{requests} requests, '
            '{created_slots} created slots, {created_workers} created workers, {full} full, {empty} empty, '
            '{acquired} acquired, {released} released, '
            '{timeout} timeout, {unexpected} unexpected'.format(
                objects=len(self.objects),
                slots=slots,
                workers=workers,
                **self.stats))
        return stats

    def _acquire_slot(self, id, max_slots, slot_id):
        assert self.is_master
        self.stats['requests'] += 1
        with self.lock:
            if id not in self.objects:
                master = Master(id, max_slots)
                self.objects[id] = master
                self.stats['created_slots'] += 1
            else:
                master = self.objects[id]

        got = True
        with master.lock:
            if slot_id not in master.slots:
                if master.max_slots > 0 and len(master.slots) >= master.max_slots:
                    got = False
                else:
                    self.stats['created_workers'] += 1
                    slot = MasterSlot()
                    master.slots[slot_id] = slot
            else:
                slot = master.slots[slot_id]

        if not got:
            self.stats['full'] += 1
            yield False
            return

        worker = Waiter()
        slot.workers.add(worker)
        self.add('update', master.serialize())

        self.stats['acquired'] += 1
        logger.debug('%s: Acquired', id)
        try:
            try:
                while True:
                    yield True
            except (GeneratorExit, GreenletExit):
                self.stats['released'] += 1
                logger.debug('%s: Released', id)
            except AZRPCTimeout:
                self.stats['timeout'] += 1
                logger.info('%s: Timed out', id)
            else:
                self.stats['unexpected'] += 1
                logger.warning('%s: Released without error', id)
        finally:
            with master.lock:
                slot.workers.remove(worker)
                if len(slot.workers) == 0:
                    del master.slots[slot_id]
                    self.stats['empty'] += 1
                self.add('update', master.serialize())

    def on_init_push_loop(self):
        assert self.is_master
        for obj in self.objects.values():
            yield obj.serialize()

    def get_all_ids(self):
        assert not self.is_master
        return self.objects.keys()

    def on_not_found_ids(self, ids):
        assert not self.is_master
        for id in ids:
            if id in self.objects:
                del self.objects[id]

    def on_update(self, data):
        assert not self.is_master
        if data['id'] not in self.objects:
            self.objects[data['id']] = Keeper(self, data)
        else:
            self.objects[data['id']]._rpc_data = data
        self.objects[data['id']].updated.set()

    def on_delete(self, id):
        assert not self.is_master
        del self.objects[id]

    # Client code

    def get_slotkeeper(self, id, max_slots):
        assert not self.is_master
        self.wait_live()
        if id not in self.objects:
            data = {
                'id': id,
                'max_slots': max_slots,
                'slots': 0,
                'workers': 0
            }
            obj = Keeper(self, data)
            self.objects[data['id']] = obj
        else:
            obj = self.objects[id]
            if obj.max_slots != max_slots:
                logger.warning('Object "%s" max_slots value of "%s" differs in requested "%s"', id, obj.max_slots, max_slots)
        return obj
    get = get_slotkeeper


class Keeper(RPCPuller):
    __rpc_members__ = ('id', 'max_slots', 'slots', 'workers')

    def __init__(self, sync, data):
        super(Keeper, self).__init__(data)
        self.sync = sync
        self.updated = Event()

    def get_slot(self, slot_id):
        return Slot(self, slot_id)

    def __repr__(self):
        return 'SlotKeeper<name="%s", max=%s, slots=%s, workers=%s>' % (self.sync.name, self.max_slots, self.slots, self.workers)


class Slot(object):
    gen = None
    got = False

    def __init__(self, keeper, id):
        self.keeper = keeper
        self.id = id

    def acquire(self):
        self.keeper.updated.clear()
        self.gen = self.keeper.sync._acquire_slot_stream_sync(self.keeper.id, self.keeper.max_slots, self.id)
        self.got = next(self.gen)
        if self.got:
            print "WAIT"
            self.keeper.updated.wait(timeout=2)
        return self.got

    def release(self):
        assert self.got
        if self.got:
            self.keeper.updated.clear()
            self.got = False
            del self.gen
            self.keeper.updated.wait(timeout=2)

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

    def __repr__(self):
        return 'SlotKeeperSlot<name="%s/%s", max=%s, slots=%s, workers=%s>' % (self.keeper.sync.name, self.id, self.keeper.max_slots, self.keeper.slots, self.keeper.workers)
