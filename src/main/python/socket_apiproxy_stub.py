import rpc_pb2
import socket
import struct

from google.appengine.datastore import datastore_pb
from google.appengine.runtime import apiproxy_errors


MAX_REQUEST_SIZE = 1 << 20

# Stolen from google.appengine.runtime.apiproxy
OK                =  0
RPC_FAILED        =  1
CALL_NOT_FOUND    =  2
ARGUMENT_ERROR    =  3
DEADLINE_EXCEEDED =  4
CANCELLED         =  5
APPLICATION_ERROR =  6
OTHER_ERROR       =  7
OVER_QUOTA        =  8
REQUEST_TOO_LARGE =  9
CAPABILITY_DISABLED = 10

_ExceptionsMap = {
  RPC_FAILED:
  (apiproxy_errors.RPCFailedError,
   "The remote RPC to the application server failed for the call %s.%s()."),
  CALL_NOT_FOUND:
  (apiproxy_errors.CallNotFoundError,
   "The API package '%s' or call '%s()' was not found."),
  ARGUMENT_ERROR:
  (apiproxy_errors.ArgumentError,
   "An error occurred parsing (locally or remotely) the arguments to %s.%s()."),
  DEADLINE_EXCEEDED:
  (apiproxy_errors.DeadlineExceededError,
   "The API call %s.%s() took too long to respond and was cancelled."),
  CANCELLED:
  (apiproxy_errors.CancelledError,
   "The API call %s.%s() was explicitly cancelled."),
  OTHER_ERROR:
  (apiproxy_errors.Error,
   "An error occurred for the API request %s.%s()."),
  OVER_QUOTA:
  (apiproxy_errors.OverQuotaError,
  "The API call %s.%s() required more quota than is available."),
  REQUEST_TOO_LARGE:
  (apiproxy_errors.RequestTooLargeError,
  "The request to API call %s.%s() was too large."),
  CAPABILITY_DISABLED:
  (apiproxy_errors.CapabilityDisabledError,
  "The API call %s.%s() is temporarily unavailable."),
}


class SocketApiProxyStub(object):
  def __init__(self, endpoint, max_request_size=MAX_REQUEST_SIZE):
    self._endpoint = endpoint
    self._max_request_size = max_request_size
    self._sock = None
    self._next_rpc_id = 0
  
  def closeSession(self):
    if self._sock:
      self._sock.close()
      self._sock = None

  def _recvAll(self, size):
    # We should use buffers here, but can't in the dev_appserver.
    data = []
    data_size = 0
    while data_size < size:
      d = self._sock.recv(size)
      if len(d) == 0:
        raise socket.error()
      data.append(d)
      data_size += len(d)
    return ''.join(data)

  def _writePB(self, pb):
    self._sock.sendall(struct.pack("!i", pb.ByteSize()) + pb.SerializeToString())

  def _readPB(self, pb):
    size = struct.unpack("!i", self._recvAll(4))[0]
    data = self._recvAll(size)
    pb.MergeFromString(data)
    return pb

  def _sendRPC(self, service, method, request, response):
    try:
      request_wrapper = rpc_pb2.Request()
      request_wrapper.rpc_id = self._next_rpc_id
      self._next_rpc_id += 1
      request_wrapper.service = service
      request_wrapper.method = method
      request_wrapper.body = request.Encode()
      self._writePB(request_wrapper)
      
      response_wrapper = rpc_pb2.Response()
      self._readPB(response_wrapper)
    except:
      # Any exception here should cause us to close the socket to make sure we
      # don't leave it in an unknown state.
      self.closeSession()
      raise

    assert response_wrapper.rpc_id == request_wrapper.rpc_id
    
    if response_wrapper.status == APPLICATION_ERROR:
      raise apiproxy_errors.ApplicationError(
          response_wrapper.application_error,
          response_wrapper.error_detail)
    elif response_wrapper.status in _ExceptionsMap:
      ex, message = _ExceptionsMap[response_wrapper.status]
      raise ex(message % (service, method))
    else:
      response.ParseFromString(response_wrapper.body)
  
  def MakeSyncCall(self, service, call, request, response):
    if not self._sock:
      self._sock = socket.socket()
      self._sock.connect(self._endpoint)
    
    self._sendRPC(service, call, request, response)


class RecordingSocketApiProxyStub(SocketApiProxyStub):
  def __init__(self, endpoint, max_request_size=MAX_REQUEST_SIZE):
    super(RecordingSocketApiProxyStub, self).__init__(
        endpoint, max_request_size)
    self.__query_history = {}

  def MakeSyncCall(self, service, call, request, response):
    if service == 'datastore_v3' and call == 'RunQuery':
      clone = datastore_pb.Query()
      clone.CopyFrom(request)
      clone.clear_hint()
      self.__query_history[clone] = self.__query_history.get(clone, 0) + 1
    super(RecordingSocketApiProxyStub, self).MakeSyncCall(
        service, call, request, response)
  
  def QueryHistory(self):
    return dict((pb, times) for pb, times in self.__query_history.items())
