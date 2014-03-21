# -*- coding: utf-8 -*-
"""
Created on Feb 18, 2014

@author: hotdog16

The loader of Qusion web service.
qusion init module: add all url function, handle before and after requests

"""

# system
import os
import json
import logging
import logging.config
from lib.qusion.sample_logging import DICT_CONFIG
logging.config.dictConfig(DICT_CONFIG)

# 3rd party
from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer

from flask.views import MethodView
from flask import Flask, request
from flask import g, Response


# ----------------------------------------------------------------------------- #
JAR_FILENAMES = ['tika-app-1.4.jar', 'langdetect.jar', 'jsonic-1.2.7.jar', '']
CLASSPATH_PREFIX = '%s%s' % (os.path.abspath('.'), '/../lib/java_lib/')
os.environ['CLASSPATH'] = ':'.join(['%s%s' % (CLASSPATH_PREFIX, jar_filename) for jar_filename in JAR_FILENAMES])
# ----------------------------------------------------------------------------- #


# ours
from lib.qusion.flask_request_parse import RequestParser
from lib.constant.app_const import LOGGER_NAME, TOKEN
from lib.qusion.utils import ReturnObj

from lib.qusion.item_api import Qusion
from lib.qusion.suggest_api import SuggestApi


app = Flask(__name__)
app.secret_key = '[%f\x94\xc0\xe5\xa3\x0e\xf5X\x10\xd0\x15*\xc0\x06q\x9c]\xa7\xec\x90\xfc&'



logger = logging.getLogger(LOGGER_NAME).getChild(__name__)


# pylint: disable=C0103
def registerApi(view, endpoint, url, pk='id'):
    """ register REST API with Flask MethodView
    """

    viewFunc = view.as_view(endpoint)
    app.add_url_rule(
        url, defaults={pk: None},
        view_func=viewFunc, methods=['GET', 'PUT', 'DELETE']
    )
    app.add_url_rule(
        url,
        view_func=viewFunc, methods=['POST']
    )
    app.add_url_rule(
        '%s/<%s>' % (url, pk),
        view_func=viewFunc, methods=['GET', 'PUT', 'DELETE']
    )


@app.before_request
def beforeRequest():
    """  get token with a sequence of [headers, args, cookies] and set token to g.token
    
    """
    g.return_obj = ReturnObj()
    parser = RequestParser()
    parser.add_argument(TOKEN, type=str, help='%s not found' % (TOKEN),
                        location=('args', 'headers', 'cookies'), required=False)

    try:
        args = parser.parse_args()
    except Exception as e:
        return g.return_obj.response_error('002', str(e), 401)
    g.token = args.get(TOKEN)



@app.after_request
def after_request(res):
    res.headers['Access-Control-Allow-Origin'] = '*'
    res.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,DELETE'
    res.headers['Access-Control-Allow-Headers'] = 'X-QNAP-AUTH-TOKEN, Content-Type, accept, origin, Content-Disposition'
    res.headers['Access-Control-Max-Age'] = '1000000'
    return res



registerApi(Qusion, 'qusion', '/v1/search', 'id')
registerApi(SuggestApi, 'suggest', '/v1/suggest', 'id')

#app.add_url_rule('/v1/suggest', 'qusion_suggest', Qusion.suggest, methods=['GET'])
app.add_url_rule('/v1/item/attribute', 'qusion_item_attribute', Qusion.item_attribute, methods=['GET'])


if __name__ == "__main__":
#    http = WSGIServer(('', 5000), app)
#    http.serve_forever()

    logger.debug('server start')
    app.run(host='0.0.0.0', debug=True)


