from gevent.monkey import patch_all
patch_all()

import time
import logging
import unittest

from gevent.pool import Group

from azrpc import AZRPC, AZRPCServer

from .lock import RPCLock, Lock
from slotkeeper import SlotKeeper


rpc_name = 'azsync-test'
rpc_port = 9999

rpc = AZRPC(rpc_name, rpc_port, heartbeat_timeout=10)


class TestLock(unittest.TestCase):
    lock = RPCLock(rpc, 'test-lock')

    def tassert(self, g, gw, result, value):
        assert g not in gw or result is value

    def _test_many_worker(self, g):
        result = self.lock.is_locked('foo')
        self.tassert(g, 'ABC', result, False)
        print g, 1, result

        with self.lock.get_lock('foo') as result:
            self.tassert(g, 'ABC', result, True)
            print g, 2, result

            result = self.lock.is_locked('foo')
            self.tassert(g, 'ABC', result, True)
            print g, 3, result

            time.sleep(1)

        result = self.lock.is_locked('foo')
        self.tassert(g, 'AB', result, True)
        self.tassert(g, 'C', result, False)
        print g, 4, result

    def test_many(self):
        group = Group()
        group.spawn(self._test_many_worker, 'A')
        group.spawn(self._test_many_worker, 'B')
        group.spawn(self._test_many_worker, 'C')
        group.join()

    def _test_long_worker(self, g):
        result = self.lock.is_locked('foo')
        self.tassert(g, 'AB', result, False)
        print g, 1, result

        with self.lock.get_lock('foo') as result:
            self.tassert(g, 'AB', result, True)
            print g, 2, result

            result = self.lock.is_locked('foo')
            self.tassert(g, 'AB', result, True)
            print g, 3, result

            time.sleep(15)

        result = self.lock.is_locked('foo')
        self.tassert(g, 'A', result, True)
        self.tassert(g, 'B', result, False)
        print g, 4, result

    def test_long(self):
        group = Group()
        group.spawn(self._test_long_worker, 'A')
        group.spawn(self._test_long_worker, 'B')
        group.join()

    def test_idle(self):
        lock = Lock(self.lock, 'test_service')
        self.tassert('X', 'X', lock.is_locked(), False)
        with lock as result:
            self.tassert('X', 'X', result, True)
            self.tassert('X', 'X', lock.is_locked(), True)
            for _ in xrange(3):
                lock.idle()
                time.sleep(1)
        self.tassert('X', 'X', lock.is_locked(), False)


class TestSlotKeeper(unittest.TestCase):
    def test(self):
        master = SlotKeeper(rpc, 'foo1')
        n1 = SlotKeeper(AZRPC(rpc_name, rpc_port), 'foo1', 'some-account-or-something')
        n2 = SlotKeeper(AZRPC(rpc_name, rpc_port), 'foo1', 'some-account-or-something')

        n1k1 = n1.get('A', 2)
        n2k1 = n2.get('A', 2)
        print n1k1
        n1k1s1a = n1k1.get_slot('slot-1')
        with n1k1s1a as got:
            assert got
            assert n1k1.slots == 1
            assert n1k1.workers == 1
            print n1k1s1a
            n1k1s1b = n1k1.get_slot('slot-1')
            with n1k1s1b as got:
                assert got
                assert n1k1.slots == 1
                assert n1k1.workers == 2
                print n1k1s1b
                n1k1s1c = n1k1.get_slot('slot-1')
                with n1k1s1c as got:
                    assert got
                    assert n1k1.slots == 1
                    assert n1k1.workers == 3
                    print n1k1s1c
                assert got
                assert n1k1.slots == 1
                assert n1k1.workers == 2
                print n1k1s1b

                n2k1s1a = n2k1.get_slot('slot-1')
                with n2k1s1a as got:
                    assert got
                    assert n1k1.slots == 1
                    assert n1k1.workers == 3
                    print n1k1s1a
                    print n2k1s1a

                n2k1s2a = n2k1.get_slot('slot-2')
                with n2k1s2a as got:
                    assert got
                    assert n1k1.slots == 2
                    assert n1k1.workers == 3
                    print n1k1s1a
                    print n2k1s2a

                    n2k1s3a = n2k1.get_slot('slot-3')
                    with n2k1s3a as got:
                        assert not got
                        assert n1k1.slots == 2
                        assert n1k1.workers == 3
                        print n1k1s1a
                        print n2k1s3a

                        print master.get_server_stats()
                        n2k1s2b = n2k1.get_slot('slot-2')
                        with n2k1s2b as got:
                            assert got
                            assert n1k1.slots == 2
                            assert n1k1.workers == 4
                            print n1k1s1a
                            print n2k1s2b

                            n1k2 = n1.get('B', 2)

                            n1k2s2a = n1k2.get_slot('slot-1')
                            with n1k2s2a as got:
                                assert got
                                assert n1k1.slots == 2
                                assert n1k1.workers == 4
                                assert n1k2.slots == 1
                                assert n1k2.workers == 1
                                print n1k1s1a
                                print n1k2s2a
                                print master.get_server_stats()

        assert n1k1.slots == 0
        assert n1k1.workers == 0
        print master.get_server_stats()


def main():
    logging.basicConfig(level=logging.INFO)
    AZRPCServer(rpc)
    unittest.main(failfast=True, catchbreak=True)

if __name__ == '__main__':
    main()
