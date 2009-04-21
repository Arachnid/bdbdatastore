import socket_apiproxy_stub
import os
import sys

sys.path.append("/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine")

from google.appengine.api import appinfo
from google.appengine.api import api_base_pb
from google.appengine.datastore import datastore_index
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import entity_pb


def getArguments(args):
  action = args[1]
  path = args[2]
  host = args[3]
  port = 9123
  if len(args) > 3:
    port = args[4]
  return action, path, host, port


def getAppId(path):
  fh = open(os.path.join(path, "app.yaml"))
  if not fh:
    raise Exception("First argument must specify an application directory.")
  app_yaml = appinfo.LoadSingleAppInfo(fh)
  fh.close()
  return app_yaml.application


def getIndexes(path):
  fh = open(os.path.join(path, "app.yaml"))
  if not fh:
    raise Exception("First argument must specify an application directory.")
  indexes = datastore_index.ParseIndexDefinitions(fh)
  fh.close()
  return datastore_index.IndexDefinitionsToKeys(indexes)


def getServerIndexes(stub, app_id):
  request = api_base_pb.StringProto()
  request.set_value(app_id)
  response = datastore_pb.CompositeIndices()
  stub.MakeSyncCall("datastore_v3", "GetIndices", request, response)
  indexes = {}
  for index in response.index_list():
    idx_def = index.definition()
    idx = ((idx_def.entity_type(), idx_def.ancestor(),
        tuple(x.name(), x.direction() for x in idx_def.property_list())))
    indexes[idx] = index
  return indexes


def deleteIndexes(stub, indexes):
  for index in indexes:
    stub.MakeSyncCall("datastore_v3", "DeleteIndex", index,
                      api_base_pb.VoidProto())


def addIndexes(stub, app_id, indexes):
  for index in indexes:
    request = entity_pb.CompositeIndex()
    request.app_id = app_id
    request.id = 0
    request.state = entity_pb.CompositeIndex.READ_WRITE
    request.definition = index
    response = api_base_pb.Integer64Proto()
    stub.MakeSyncCall("datastore_v3", "CreateIndex", request, response)


def main(args):
  if len(args) < 4:
    print "Usage: %s (update|vacuum) path host [port]"
  stub = socket_apiproxy_stub.SocketApiProxyStub((host, port))
  action, path, host, port = getArguments(args)
  app_id = getAppId(path)
  indexes = getIndexes(path)
  server_indexes = getServerIndexes(stub, app_id)
  if action == "vacuum":
    deleteIndexes(stub, [server_indexes[x]
                         for x in set(server_indexes.keys()) - indexes])
  else:
    addIndexes(stub, app_id, indexes - set(server_indexes.keys()))


if __name__ == '__main__':
  main(sys.argv)
