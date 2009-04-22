package net.notdot.protorpc;

import java.net.SocketAddress;

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
		protected ProtoRpcController controller;
		
		protected ProtoRpcCallback(ProtoRpcController controller) {
			this.controller = controller;
		}
		
		public void run(Message arg0) {
			Rpc.Response response = Rpc.Response.newBuilder()
				.setRpcId(this.controller.getRpcId())
				.setStatus(Rpc.Response.ResponseType.OK)
				.setBody(arg0.toByteString()).build();
			this.controller.sendResponse(response);
		}
	}

	final Logger logger = LoggerFactory.getLogger(ProtoRpcHandler.class);

	protected Service service;
	protected ChannelGroup open_channels;
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		open_channels.add(e.getChannel());
		logger.info("New connection from {}.", ctx.getChannel().getRemoteAddress());
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		if(service instanceof Disposable)
			((Disposable)service).close();
		logger.info("Client {} disconnected.", ctx.getChannel().getRemoteAddress());
	}

	public ProtoRpcHandler(Service s, ChannelGroup channels) {
		this.service = s;
		this.open_channels = channels;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		Channel ch = e.getChannel();
		SocketAddress remote_addr = ch.getRemoteAddress();
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

		logger.debug("Client {} RPC {} request data: {}", new Object[] { remote_addr, request.getRpcId(), request_data });

		ProtoRpcController controller = new ProtoRpcController(ch, request.getService(), request.getMethod(), request.getRpcId());
		ProtoRpcCallback callback = new ProtoRpcCallback(controller);
		try {
			service.callMethod(method, controller, request_data, callback);
		} catch(RpcFailedError ex) {
			if(ex.getCause() != null) {
				logger.error("Internal error: ", ex.getCause());
			}
			controller.setFailed(ex.getMessage(), ex.getApplicationError());
		}
		if(!controller.isResponseSent()) {
			controller.sendResponse(Rpc.Response.newBuilder()
				.setRpcId(request.getRpcId())
				.setStatus(Rpc.Response.ResponseType.RPC_FAILED)
				.setErrorDetail("RPC handler failed to issue a response").build());
			logger.error("Client {} RPC {} failed to return a response.",
					new Object[] { remote_addr, request.getRpcId() });
		}
	}	
}
