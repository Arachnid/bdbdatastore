package net.notdot.protorpc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		protected long rpcId;
		
		protected ProtoRpcCallback(Channel ch, long rpcId) {
			this.channel = ch;
			this.rpcId = rpcId;
		}
		
		public void run(Message arg0) {
			if(!called) {
				Rpc.Response response = Rpc.Response.newBuilder()
					.setRpcId(this.rpcId)
					.setStatus(Rpc.Response.ResponseType.OK)
					.setBody(arg0.toByteString()).build();
				this.channel.write(response);
				called = true;
				logger.trace("Response to RPC {}: {}", this.rpcId, arg0);
			}
		}

		public boolean isCalled() {
			return called;
		}
	}

	final Logger logger = LoggerFactory.getLogger(ProtoRpcHandler.class);

	protected Service service;
	protected ChannelGroup open_channels;
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		open_channels.add(e.getChannel());
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		if(service instanceof Disposable)
			((Disposable)service).close();
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
				
		// For now we ignore the service name in the message.

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

		logger.trace("Handling RPC {} for {}.{}. Request: {}",
				new Object[] { request.getRpcId(), request.getService(),
				               request.getMethod(), request_data});

		ProtoRpcCallback callback = new ProtoRpcCallback(ch, request.getRpcId());
		ProtoRpcController controller = new ProtoRpcController();
		try {
			service.callMethod(method, controller, request_data, callback);
		} catch(RpcFailedError ex) {
			if(ex.getCause() != null) {
				logger.error("Internal error", ex.getCause());
			}
			controller.setFailed(ex.getMessage());
			controller.setApplicationError(ex.getApplicationError());
		}
		if(!callback.isCalled()) {
			if(controller.failed()) {
				ch.write(Rpc.Response.newBuilder()
						.setRpcId(request.getRpcId())
						.setStatus(Rpc.Response.ResponseType.APPLICATION_ERROR)
						.setErrorDetail(controller.errorText())
						.setApplicationError(controller.getApplicationError()).build());
			} else {
				ch.write(Rpc.Response.newBuilder()
						.setRpcId(request.getRpcId())
						.setStatus(Rpc.Response.ResponseType.RPC_FAILED)
						.setErrorDetail("RPC handler failed to issue a response").build());
			}
		}
	}	
}
