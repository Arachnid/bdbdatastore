package net.notdot.bdbdatastore.server;

import java.io.IOException;
import java.net.InetSocketAddress;

import net.notdot.bdbdatastore.server.Test.EchoRequest;
import net.notdot.bdbdatastore.server.Test.EchoResponse;
import net.notdot.protorpc.ProtoRpcHandler;
import net.notdot.protorpc.Rpc;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.protobuf.ProtobufCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class DatastoreServer extends Test.EchoService {
	private static final int PORT = 9123;

	@Override
	public void echo(RpcController controller, EchoRequest request,
			RpcCallback<EchoResponse> done) {
		System.out.println(request.getBody());
		done.run(EchoResponse.newBuilder().setBody(request.getBody()).build());
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter(ProtobufCodecFactory.newInstance(Rpc.Request.getDefaultInstance())));

        acceptor.setHandler( new ProtoRpcHandler(new DatastoreServer()));
    	acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.bind( new InetSocketAddress(PORT) );
	}

}
