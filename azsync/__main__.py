from gevent.monkey import patch_all
patch_all()

import sys
import logging

from gevent import wait, sleep
from getopt import getopt

from azrpc import AZRPC, AZRPCServer

from .lock import RPCLock
from .slotkeeper import SlotKeeper

logger = logging.getLogger(__name__)


def main(args):
    opts, args = getopt(args, None, ['log-level=', 'stats-interval=', 'name=', 'port=', 'heartbeat-timeout=', 'all', 'lock', 'slotkeeper'])
    if args:
        print >>sys.stderr, "Invalid arguments: %s" % args
        sys.exit(1)
    opts = dict(opts)

    log_level = opts.pop('--log-level', 'INFO')
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    stats_interval = int(opts.pop('--stats-interval', 60))

    name = opts.pop('--name', 'azsync')
    port = int(opts.pop('--port', 47002))
    heartbeat_timeout = int(opts.pop('--heartbeat-timeout', 10))

    rpc = AZRPC(name, port, heartbeat_timeout=heartbeat_timeout)

    workers = dict()
    if '--all' in opts or '--lock' in opts:
        workers['lock'] = RPCLock(rpc, 'lock')
    if '--all' in opts or '--slotkeeper' in opts:
        workers['slot'] = SlotKeeper(rpc, 'slotkeeper')
    if not workers:
        print >>sys.stderr, "Use at least one of --lock or --slotkeeper options"
        sys.exit(1)

    AZRPCServer(rpc)
    logger.info('Listening on port %s', port)

    try:
        if stats_interval <= 0:
            wait()
        else:
            while True:
                sleep(stats_interval)
                for name, obj in workers.iteritems():
                    print name, obj.get_server_stats()
    except KeyboardInterrupt:
        pass

main(sys.argv[1:])
