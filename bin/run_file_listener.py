#!/usr/bin/env python
'''
Created on Feb 27, 2014

@author: hotdog16
'''

# system
import time
import os
JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])
import logging
import logging.config
from lib.qusion.sample_logging import DICT_CONFIG
logging.config.dictConfig(DICT_CONFIG)

# 3rd party
import gevent
from gevent.queue import Queue as geventQueue
from gevent.queue import PriorityQueue as geventPriorityQueue
# Patch all to gevent greenlet
from gevent.monkey import patch_all
patch_all()


# ours
from lib.qusion.qusion_gateway.qusion_gateway import QusionGateway
from lib.qusion.file_processor import FileProcessor
from lib.qusion.file_walker import FileWalker
from lib.qusion.file_listener import FileListener
from lib.qusion.db_mgr import FileDbOperator
from lib.qusion.config import *


LOGGER_NAME = 'qusion'
logger = logging.getLogger(LOGGER_NAME).getChild(__name__)


if __name__ == "__main__":
    import sys
    pidfile = '/tmp/%s.pid' % os.path.basename(sys.argv[0])
    pid = str(os.getpid())
    if os.path.isfile(pidfile):
        print "%s already exists, exiting" % pidfile
        sys.exit()
    else:
        file(pidfile, 'w').write(pid)
    stat = "STARTING"
    watches = {}
    startup_ts = time.time()
    queue = geventQueue
    pqueue = geventPriorityQueue

    fprocessor = FileProcessor(queue)
    fwalker = FileWalker(fprocessor, FileDbOperator(DB_PATH), pqueue)
    flistener = FileListener(fwalker)
#    fwalker.start()
#    fprocessor.start()
    gevent.spawn(flistener.init_conf_monitor)
    gevent.spawn(flistener.init_scanner, curr_stat=stat)
    try:
        gevent.joinall([
            gevent.spawn(fwalker.walker_consumer),
            gevent.spawn(fprocessor.process_consumer),
#            gevent.spawn(flistener.init_conf_monitor),
#            gevent.spawn(flistener.init_scanner),
            gevent.spawn(flistener.fnotify_producer),
            gevent.spawn(flistener.fnotify_consumer),
            gevent.spawn(flistener.database_syncer),
            ])
    except Exception as e:
        logging.info(repr(e))
#    except:
#        fwalker.stop()
#        fprocessor.stop()
