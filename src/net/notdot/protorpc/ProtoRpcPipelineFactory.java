package net.notdot.protorpc;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

public class ProtoRpcPipelineFactory implements ChannelPipelineFactory {
	protected ServiceFactory service_factory;
	protected ChannelGroup open_channels;
	protected int max_size;
	
	public ProtoRpcPipelineFactory(ServiceFactory f, ChannelGroup channels, int max_size) {
		this.service_factory = f;
		this.open_channels = channels;
		this.max_size = max_size;
	}
	
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline pipeline = Channels.pipeline();
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(max_size, 0, 4, 0, 4));
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(Rpc.Request.getDefaultInstance()));
		pipeline.addLast("handler", new ProtoRpcHandler(service_factory.getService(), open_channels));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());
		return pipeline;
	}

}
