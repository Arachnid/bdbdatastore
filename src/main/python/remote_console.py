#!/usr/bin/python
import code
import getpass
import os
import sys

base_path = "/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine"
sys.path.append(base_path)
sys.path.append(base_path + "/lib/yaml/lib")
sys.path.append(base_path + "/lib/webob")
sys.path.append(base_path + "/lib/django")

from google.appengine.api import apiproxy_stub_map
from google.appengine.ext import db

import socket_apiproxy_stub

if len(sys.argv) < 3:
  print "Usage: %s app_id [host] [port]" % (sys.argv[0],)
app_id = sys.argv[1]
host = sys.argv[2]
port = 9123
if len(sys.argv) > 3:
  port = int(sys.argv[3])

os.environ['APPLICATION_ID'] = app_id
datastore_stub = socket_apiproxy_stub.SocketApiProxyStub((host, port))
apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', datastore_stub)

code.interact('App Engine interactive console for %s' % (app_id,), None, locals())
