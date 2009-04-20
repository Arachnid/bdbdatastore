import rpc_pb2
import socket
import struct

MAX_REQUEST_SIZE = 1 << 20


class SocketApiProxyStub(object):
  def __init__(self, endpoint, max_request_size=MAX_REQUEST_SIZE):
    self._endpoint = endpoint
    self._max_request_size = max_request_size
    self._sock = None
    self._next_rpc_id = 0
  
  def closeSession(self):
    self._sock.close()
    self._sock = None

  def _writePB(self, pb):
    self._sock.sendall(struct.pack("!i", pb.ByteSize()) + pb.SerializeToString())

  def _readPB(self, pb):
    size = struct.unpack("!i", self._sock.recv(4, socket.MSG_WAITALL))[0]
    data = self._sock.recv(size, socket.MSG_WAITALL)
    pb.MergeFromString(data)
    return pb

  def _sendRPC(self, service, method, request, response):
    for i in range(5):
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
        assert response_wrapper.rpc_id == request_wrapper.rpc_id
        assert response_wrapper.status == rpc_pb2.Response.OK
        response.ParseFromString(response_wrapper.body)
        return
      except socket.error, e:
        if e.args[0] != 54:
          raise
  
  def MakeSyncCall(self, service, call, request, response):
    if not self._sock:
      self._sock = socket.socket()
      self._sock.connect(self._endpoint)
    
    self._sendRPC(service, call, request, response)
