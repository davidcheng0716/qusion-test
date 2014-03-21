import os
import sys
import subprocess
import json

JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])

from jnius import autoclass
SimpleTextExtractor = autoclass('SimpleTextExtractor')

import time

extractor = SimpleTextExtractor()
extractor.init()


#for i in xrange(1):
#    resp = extractor.extract('call_java.py', 1000000)
#    json_resp = json.loads(resp)
#
#    for i in json_resp:
#        print i, json_resp.get(i)
#
#print time.time() - t

from os import walk
path = '/share/HDA_DATA/eBooks'
#path = '/Users/hotdog16/Documents/workspace/nbp_cloud/vm/stress_test/eBooks/'
file_paths = []
count = 0

t = time.time()
for (dirpath, dirnames, filenames) in walk(path):
    for filename in filenames:
        filepath = os.path.join(dirpath, filename)
        print "filepath", filepath
        resp = extractor.extract(filepath, 100)
        extract_obj = json.loads(resp)
        content = extract_obj.get('content', u'').encode('utf8')
        f = open('/share/HDA_DATA/qusion/bin/txt_2MB/%s.txt' % (count), 'wb')
        f.write(content)
        f.close()
        count += 1



print "total spend time=", time.time() - t

#path = 'AAAA.docx'
#
#import time
#t = time.time()
#for i in xrange(100):
#    subprocess.call(['java', '-Dfile.encoding=UTF-8', '-cp', JAR_PATH, 'SimpleTextExtractor', path])
##    resp = SimpleTextExtractor.main(['AAAA.docx'])
#
#print time.time() - t





#Hello = autoclass('Hello')
#print Hello().hello('jav')
