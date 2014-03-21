"""
ES server initialization script of Qusion

"""
# system
import time, sys, json, argparse, os
import uuid

JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])

# 3-rd
import requests
from requests.exceptions import ConnectionError
# ours
from lib.constant.gateway_const import ELASTIC_SERVER_HOST, ELASTIC_SERVER_PORT
from lib.qusion.qusion_gateway.qusion_gateway import QusionGateway

CHECK_ALIVE_TIMEOUT = 100
WAITING_TO_READY_TIMEOUT = 120
SLEEP_OFFSET = 5
ES_CONFIG_TEMPLATE = "../lib/schema/elasticsearch.template"

parser = argparse.ArgumentParser(description='Qusion ES Server utilities')
parser.add_argument("-p", "--operation", required=True, help="Indicate the operation to be performed. Supported operations included: [\"install\", \"startup\"], \"install\"=Qusion ES install before launching ES server(need ES_HOME environment variable), \"startup\"=Perform startup operation of Qusion ES server.")
parser.add_argument('--erase', action='store_true', help="erase entire index data of Qusion during the startup operation")
parser.add_argument('--init-test-analyzer', action='store_true', help="init the test analyzer during the startup operation")
args = vars(parser.parse_args())

if args["operation"] not in ["install", "startup"]:
    print "Error! Not valid operation=%s. Only accept [%s, %s]." % (args["operation"], "install", "startup")
    sys.exit(1)
 
# setup ES config xml
if args["operation"] == "install":
    if "ES_HOME" not in os.environ:
        print "Error!!Cannot find the environment variable: ES_HOME"
        sys.exit(1)
    if not os.path.isdir(os.environ["ES_HOME"]):
        print "Error! The ES_HOME is not a valid directory. ES_HOME=%s" % os.environ["ES_HOME"]
        sys.exit(1)
    
    ES_CONFIG_PATH = os.environ["ES_HOME"] + "/config/elasticsearch.yml"
    template_str = open(ES_CONFIG_TEMPLATE, "r").read()
    cluster_name =  "qusion_%s" % uuid.uuid1().hex
    # substitute the cluster name
    template_str = template_str.format(cluster_name=cluster_name)
    config_file = open(ES_CONFIG_PATH, "w")
    config_file.write(template_str)
    config_file.close()
    print "ES configuration file of Qusion generated in %s." % ES_CONFIG_PATH
    sys.exit(0)
else:
    # wait for ES to 
    is_success = False
    params = {"wait_for_status": "yellow", "timeout": "%ss" % WAITING_TO_READY_TIMEOUT}
    now = 0
    uri = "http://%s:%s/_cluster/health" % (ELASTIC_SERVER_HOST, ELASTIC_SERVER_PORT)
    
    while now < CHECK_ALIVE_TIMEOUT:
        try:
            res = requests.get(uri, params=params)
        except ConnectionError:
            print "Elasticsearch server connection failure.. sleep and retry.."
            time.sleep(SLEEP_OFFSET)
            now += SLEEP_OFFSET
        else:
            print "Elasticsearch server is ready.."
            try:
                res = json.loads(res.text)
                
            except:
                print "Return payload of ES server is not valid! res= %s" % res.text
            else:
                is_success = True if not res["timed_out"] else False
            break
        
    if not is_success:
        print "Elasticsearch server cannot be launched successfully...abort operation!"
        sys.exit(1)
    
    # whether to erase the index data
    if args["erase"]:
        print "Erase entire index data.."
        QusionGateway.erase_all_indexes()
    
    print "Configure Qusion index schema.."
    QusionGateway.init_structure()
    
    if args["init_test_analyzer"]:
        print "Configure test analyzer index schema.."
        QusionGateway.init_test_analyzer()
        
    print "Finished."   
    sys.exit(0)

    
