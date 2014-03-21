import os, sys
JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])
import time, datetime
import copy
import json
import random
import requests

from lib.qusion.qusion_gateway.qusion_gateway import QusionGateway

HOST = 'http://localhost:9200'
INDEX = 'qusion'
TYPE = 'main'

EXTENSION = ["doc",
"docx",
"log",
"msg",
"odt",
"rtf",
"tex",
"txt",
"wpd",
"wps",
"csv",
"dat",
"gbr",
"ged",
"key",
"pps",
"ppt",
"pptx",
"sdf",
"tar",
"vcf",
"xml",
"aif",
"iff",
"m3u",
"m4a",
"mid",
"mp3",
"mpa",
"ra",
"wav",
"wma",
"3g2",
"3gp",
"asf",
"asx",
"avi",
"flv",
"m4v",
"mov",
"mp4",
"mpg",
"rm",
"srt",
"swf",
"vob",
"wmv",
"3dm",
"3ds",
"max",
"obj",
"bin",
"cue",
"dmg",
"iso",
"mdf",
"vcd",
"c",
"class",
"cpp",
"cs",
"dtd",
"h",
"java",
"lua",
"pl",
"py",
"sh"]


NAME = ["Filiberto", "York",
"Mitzie", "Cleary",
"Eddy", "Nevius",
"Alica", "Knott",
"Renda", "Tracey",
"Carrie", "Pries",
"Nicki", "Masters",
"Kimbery", "Koster",
"Azzie", "Burford",
"Philip", "Bermeo",
"Kandis", "Tingey",
"Freeman", "Baxendale",
"Ahmad", "Messinger",
"Shaunda", "Bruder",
"Emilia", "Stickley",
"Maye", "Braggs",
"Colene", "Baker",
"Laci", "Welden",
"Jade", "Frith",
"Jaquelyn", "Furry",
"Alma", "Rick",
"Mindy", "Sthilaire",
"Jeneva", "Okamura",
"Alva", "Eanes",
"Cheryll", "Hannahs",
"Heriberto", "Ranum",
"Jannie", "Cirillo",
"Tonisha", "Marcinkowski",
"Robbie", "Dorey",
"Tracey", "Session",
"Nova", "Weinstock",
"Claudette", "Macdowell",
"Mozelle", "Zeitler",
"Florida", "Michels",
"Faviola", "Milam",
"Thea", "Coil",
"Chloe", "Donelan",
"Maddie", "Gibbens",
"Fallon", "Conlon",
"Lloyd", "Witzel",
"Raymonde", "Mell",
"Susanne", "Labarge",
"Amanda", "Meisel",
"Nu", "Heald",
"Genevieve", "Campanella",
"Earline", "Lacayo",
"Morris", "Dendy",
"Anjelica", "Rough",
"Jarvis", "Brady",
"Tamatha", "Vicknair"]

large_mata = {
    "id" : "978-0641723445",
    "data" : "05 Sparrow.mp3",
  }


doc = {'name': 'money_v.2.ots',
     'language': 'zh-cn',
     'created': '2013-12-27T11:30:35Z',
     'modified': '2013-12-27T11:30:35Z',
     'qusion_doc_type': 'file-directory',
     'source': 'qusion',
     'path': '/Users/hotdog16/Downloads/openoffice/calc',
     'qusion_doc_ver': '1.0',
     'metadata': {'asit_content': ''},
     'type': 'file',
     'id': '/Users/hotdog16/Downloads/openoffice/OO Extra/english/attendanceCalendar.info',
     'mime_type': 'text/plain; charset=ISO-8859-1',
     'size': 695}



def get_large_word(size):
    basestr = "Because the _source field causes some storage overhead we may choose to compress information stored in that field. In order to do that, we would have to set the compress parameter to true. Although this will shrink the index, it will make the operations made on the _source field a bit more CPU-intensive. However, ElasticSearch allows us to decide when to compress the _source field. Using the compress_threshold property, we can control how big the _source field's content needs to be in order for ElasticSearch to compress it. This property accepts a size value in bytes (for example, 100b, 10kb)"

    times = size / len(basestr)
    left = size % len(basestr)
    return basestr * times + basestr[:left]

def main():
    rootdir = sys.argv[1]


    if len(sys.argv) < 4:
        print 'Argument not enough. python post_metadata.py field_size post_size_in_once repeat_times \nExiting...'
        exit()

#    QusionGateway.erase_all_indexes()
#    QusionGateway.init_structure()

    field_size = eval(sys.argv[1])

    array_size = eval(sys.argv[2])

    print "field_size", field_size
    print "array_size", array_size

    ids = []
    # generate id list
    for i in xrange(array_size):
        _id = '%07d' % (i)
        ids.append('%s' % _id)
    last = 0
    size = 0
    start = time.time()
    headers = {
               'Content-Type':'application/json'
               }
    for i in xrange(eval(sys.argv[3])):
        print "@@@@@@", i
        for _id in ids:
            es_doc = copy.copy(doc)
            _id = '%s_%s' % (i, _id)

            ext_idx = random.randint(0, len(EXTENSION) - 1)
            name_idx = random.randint(0, len(NAME) - 1)

            es_doc['name'] = 'Name_ %s %s %s.%s' % (i, _id, NAME[name_idx], EXTENSION[ext_idx])
            es_doc['created'] = datetime.datetime.fromtimestamp(time.time()).isoformat() + "Z"
            es_doc['modified'] = datetime.datetime.fromtimestamp(time.time()).isoformat() + "Z"
            es_doc['path'] = '/test/data-01/path_%s' % (i)
            es_doc['id'] = '%s/%s' % (es_doc['path'], es_doc['name'])
#            es_doc['id'] = _id
            es_doc['size'] = random.randint(0, 1000000)

            f = open('txt_2MB/%s.txt' % (i % 118), 'rb')
            es_doc['metadata']['asit_content'] = {'value':f.read()}
#	    add_index.append(es_doc)

            is_exist = QusionGateway.overwrite(es_doc)
            if is_exist is True:
                pass

    print "Spent time=" + str(time.time() - start)



def batch():
    rootdir = sys.argv[1]
    if len(sys.argv) < 4:
        print 'Argument not enough. python post_metadata.py field_size post_size_in_once repeat_times \nExiting...'
        exit()

    QusionGateway.erase_all_indexes()
    QusionGateway.init_structure()

    field_size = eval(sys.argv[1])

    array_size = eval(sys.argv[2])

    print "field_size", field_size
    print "array_size", array_size

    ids = []
    # generate id list
    for i in xrange(array_size):
        _id = '%010d' % (i)
        ids.append('%s' % _id)
    last = 0
    size = 0
    start = time.time()
    headers = {
               'Content-Type':'application/json'
               }
    for i in xrange(eval(sys.argv[3])):
        add_index = []
        for _id in ids:
            es_doc = copy.copy(doc)
            _id = '%s_%s' % (i, _id)

            ext_idx = random.randint(0, len(EXTENSION) - 1)
            name_idx = random.randint(0, len(NAME) - 1)
            es_doc['name'] = 'Name_ %s %s %s.%s' % (i, _id, NAME[name_idx], EXTENSION[ext_idx])
            es_doc['created'] = datetime.datetime.fromtimestamp(time.time()).isoformat() + "Z"
            es_doc['modified'] = datetime.datetime.fromtimestamp(time.time()).isoformat() + "Z"
            es_doc['path'] = '/test/data-01/path_%s' % (i)
#            es_doc['id'] = '%s/%s' % (es_doc['path'], es_doc['name'])
            es_doc['id'] = _id
            es_doc['size'] = random.randint(0, 1000000)
            f = open('txt_2MB/%s.txt' % ((i % 118)), 'rb')
            es_doc['metadata']['asit_content'] = f.read()
            add_index.append(es_doc)

        pre_cmd = { "index": {"_index": INDEX, "_type": TYPE}}
        content = ""
        for data in add_index:
            content += json.dumps(pre_cmd) + "\n" + json.dumps(data) + "\n"
        res = requests.post("%s/%s/%s/_bulk" % (HOST, INDEX, TYPE), data=content)
        if res.status_code / 100 == 4:
            print "post document failed:" + content
            raise


    print "Spent time=" + str(time.time() - start)

#===============================================================================
# 	if len(add_index) == 1:
#   	    content = json.dumps(add_index[0])
# 	    res = requests.post("%s/%s/%s" % (HOST, INDEX, TYPE), data=content)
# 	    if res.status_code / 100 == 4:
# 		print "post document failed:" + content
# 		raise
# 	else:
# 	    # need to use bulk api to send many document at single api.
# 	    pre_cmd = { "index": {"_index": INDEX, "_type": TYPE}}
# 	    content = ""
#             for data in add_index:
# 	        content += json.dumps(pre_cmd) + "\n" + json.dumps(data) + "\n"
# 	    res = requests.post("%s/%s/%s/_bulk" % (HOST, INDEX, TYPE), data=content)
#             if res.status_code / 100 == 4:
#                 print "post document failed:" + content
#                 raise
#
#
# 	size += len(add_index)
# 	last += len(add_index)
# 	if last > 10:
# 	    print "\nPOST file size=" + str(size) + " current cost time=" + str(time.time() - start)
# 	    last = 0
#
#
#     print "\nPOST file size=" + str(size)
#===============================================================================



def clearAndResetIndex():
    print "clearAndResetIndex-start"
    cmd = 'curl -s -X DELETE "%s/%s"' % (HOST, INDEX) + " >/dev/null"
    os.system(cmd)
    cmd = 'curl -s -X PUT "%s/%s" -d \'{"settings" : { "index" : { "number_of_shards" : 1, "number_of_replicas" : 0 }}}\'' % (HOST, INDEX) + " >/dev/null"
    os.system(cmd)
    cmd = 'curl -s -X GET "%s/_cluster/health?wait_for_status=green&pretty=1&timeout=5s"' % HOST + " >/dev/null"
    os.system(cmd)

    cmd = 'curl -s -X PUT "{}/{}/{}/_mapping" -d'.format(HOST, INDEX, TYPE) + SCHEMA + " >/dev/null"
    os.system(cmd)

    print "clearAndResetIndex-end"


def createIndexIfDoesntExist():
    import urllib2

    class HeadRequest(urllib2.Request):
        def get_method(self):
            return "HEAD"

    # check if type exists by sending HEAD request to index
    try:
        urllib2.urlopen(HeadRequest(HOST + '/' + INDEX + '/' + TYPE))
    except urllib2.HTTPError, e:
        if e.code == 404:
            print 'Index doesnt exist, creating...'

            os.system('curl -X PUT "{}/{}/{}/_mapping" -d'.format(HOST, INDEX, TYPE) + SCHEMA)
        else:
            print 'Failed to retrieve index with error code - %s.' % e.code

# kick off the main function when script loads
main()
