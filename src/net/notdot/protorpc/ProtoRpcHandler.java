package net.notdot.protorpc;

import net.notdot.protorpc.Rpc.Response;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

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
				System.out.println("Response");
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
		.setStatus(Rpc.Response.ResponseType.FAILED)
		.setError(cause.toString()).build();
		session.write(response);
		System.out.println("Exception");
	}

	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		RpcController controller = new RpcController(session);
		Rpc.Request request = (Rpc.Request) message;
		MethodDescriptor method = service.getDescriptorForType().findMethodByName(request.getMethod());
		Message request_data = service.getRequestPrototype(method).newBuilderForType().mergeFrom(request.getBody()).build();
		ProtoRpcCallback callback = new ProtoRpcCallback(session);
		service.callMethod(method, controller, request_data, callback);
		if(!callback.isCalled()) {
			Rpc.Response response = Rpc.Response.newBuilder()
					.setStatus(Rpc.Response.ResponseType.FAILED)
					.setError(controller.errorText()).build();
			System.out.println("Failed");
			session.write(response);
		}
	}
}
