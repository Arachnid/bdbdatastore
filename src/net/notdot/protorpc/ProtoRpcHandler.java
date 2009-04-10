package net.notdot.protorpc;

import net.notdot.protorpc.Rpc.Response;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;
import com.google.protobuf.Descriptors.MethodDescriptor;

public class ProtoRpcHandler extends IoHandlerAdapter {
	protected class ProtoRpcCallback implements RpcCallback<Message> {
		protected IoSession session = null;
		protected boolean called = false;
		
		protected ProtoRpcCallback(IoSession s) {
			session = s;
		}
		
		public void run(Message arg0) {
			if(!called) {
				Rpc.Response response = Rpc.Response.newBuilder()
					.setStatus(Response.ResponseType.OK)
					.setBody(arg0.toByteString()).build();
				session.write(response);
				called = true;
			}
		}

		public boolean isCalled() {
			return called;
		}
	}
	
	protected Service service = null;
	
	public ProtoRpcHandler(Service s) {
		service = s;
	}
	
	@Override
	public void exceptionCaught(IoSession session, Throwable cause)
			throws Exception {
		Rpc.Response response = Rpc.Response.newBuilder()
		.setStatus(Rpc.Response.ResponseType.RPC_FAILED)
		.setErrorDetail(cause.toString()).build();
		session.write(response);
		System.out.println("Exception");
	}

	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		Rpc.Request request = (Rpc.Request) message;

		MethodDescriptor method = service.getDescriptorForType().findMethodByName(request.getMethod());
		if(method == null) {
			session.write(Rpc.Response.newBuilder().setStatus(Rpc.Response.ResponseType.CALL_NOT_FOUND).build());
			return;
		}

		Message request_data;
		try {
			request_data = service.getRequestPrototype(method).newBuilderForType().mergeFrom(request.getBody()).build();
		} catch(InvalidProtocolBufferException ex) {
			session.write(Rpc.Response.newBuilder().setStatus(Rpc.Response.ResponseType.ARGUMENT_ERROR).build());
			return;
		}

		ProtoRpcCallback callback = new ProtoRpcCallback(session);
		ProtoRpcController controller = new ProtoRpcController(session);
		service.callMethod(method, controller, request_data, callback);
		if(!callback.isCalled()) {
			if(controller.failed()) {
				session.write(Rpc.Response.newBuilder()
						.setStatus(Rpc.Response.ResponseType.APPLICATION_ERROR)
						.setErrorDetail(controller.errorText())
						.setApplicationError(controller.getApplicationError()).build());
			} else {
				session.write(Rpc.Response.newBuilder()
						.setStatus(Rpc.Response.ResponseType.RPC_FAILED)
						.setErrorDetail("RPC handler failed to issue a response").build());
			}
		}
	}
}
