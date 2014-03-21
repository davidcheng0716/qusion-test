'''
Created on Feb 27, 2014

@author: hotdog16
'''

# system
import time
import os
import logging
import logging.config
from lib.qusion.sample_logging import DICT_CONFIG
logging.config.dictConfig(DICT_CONFIG)

JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])

# 3rd party
import gevent

# ours
from lib.qusion.file_processor import FileProcessor
from lib.qusion.qusion_gateway.qusion_gateway import QusionGateway


LOGGER_NAME = 'qusion'
logger = logging.getLogger(LOGGER_NAME).getChild(__name__)

if __name__ == '__main__':

#    QusionGateway.erase_all_indexes()
#    QusionGateway.init_structure()

    file_processor = FileProcessor()
    gevent.spawn(file_processor.proceducer, '/Users/hotdog16/Downloads/openoffice').join()
    t = time.time()
    try:
        gevent.joinall([
            gevent.spawn(file_processor.process_consumer),
            gevent.spawn(file_processor.process_consumer),
            gevent.spawn(file_processor.process_consumer)
        #    gevent.spawn(file_processor.time_consumer, 'hotdog'),
        ])
    except:
        pass
    print "total time=", time.time() - t
