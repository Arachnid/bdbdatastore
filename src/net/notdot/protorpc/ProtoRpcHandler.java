package net.notdot.protorpc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.MethodDescriptor;

@ChannelPipelineCoverage("one")
public class ProtoRpcHandler extends SimpleChannelHandler {
	protected class ProtoRpcCallback implements RpcCallback<Message> {
		protected Channel channel = null;
		protected boolean called = false;
		
		protected ProtoRpcCallback(Channel ch) {
			this.channel = ch;
		}
		
		public void run(Message arg0) {
			if(!called) {
				Rpc.Response response = Rpc.Response.newBuilder()
					.setStatus(Rpc.Response.ResponseType.OK)
					.setBody(arg0.toByteString()).build();
				this.channel.write(response);
				called = true;
			}
		}

		public boolean isCalled() {
			return called;
		}
	}

	protected Service service;
	protected ChannelGroup open_channels;
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		open_channels.add(e.getChannel());
	}

	public ProtoRpcHandler(Service s, ChannelGroup channels) {
		this.service = s;
		this.open_channels = channels;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		Channel ch = e.getChannel();
		Rpc.Request request = (Rpc.Request) e.getMessage();

		MethodDescriptor method = service.getDescriptorForType().findMethodByName(request.getMethod());
		if(method == null) {
			ch.write(Rpc.Response.newBuilder().setStatus(Rpc.Response.ResponseType.CALL_NOT_FOUND).build());
			return;
		}

		Message request_data;
		try {
			request_data = service.getRequestPrototype(method).newBuilderForType().mergeFrom(request.getBody()).build();
		} catch(InvalidProtocolBufferException ex) {
			ch.write(Rpc.Response.newBuilder().setStatus(Rpc.Response.ResponseType.ARGUMENT_ERROR).build());
			return;
		}

		ProtoRpcCallback callback = new ProtoRpcCallback(ch);
		ProtoRpcController controller = new ProtoRpcController(ch);
		service.callMethod(method, controller, request_data, callback);
		if(!callback.isCalled()) {
			if(controller.failed()) {
				ch.write(Rpc.Response.newBuilder()
						.setStatus(Rpc.Response.ResponseType.APPLICATION_ERROR)
						.setErrorDetail(controller.errorText())
						.setApplicationError(controller.getApplicationError()).build());
			} else {
				ch.write(Rpc.Response.newBuilder()
						.setStatus(Rpc.Response.ResponseType.RPC_FAILED)
						.setErrorDetail("RPC handler failed to issue a response").build());
			}
		}
	}
	
}
