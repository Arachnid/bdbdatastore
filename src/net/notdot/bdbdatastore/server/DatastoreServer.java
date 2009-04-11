package net.notdot.bdbdatastore.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import net.notdot.protorpc.ProtoRpcPipelineFactory;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class DatastoreServer {
	final Logger logger = LoggerFactory.getLogger(DatastoreServer.class);

	private static Datastore datastore;
	private static ChannelFactory factory;
	private static ChannelGroup openChannels = new DefaultChannelGroup("DatastoreServer");
	private static final int PORT = 9123;
	private static final String PATH = "datastore";

	/**
	 * @param args
	 * @throws IOException 
	 * @throws DatabaseException 
	 * @throws EnvironmentLockedException 
	 */
	public static void main(String[] args) throws IOException, EnvironmentLockedException, DatabaseException {
		datastore = new Datastore(PATH);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				datastore.close();
				ChannelGroupFuture future = openChannels.close();
				future.awaitUninterruptibly();
				factory.releaseExternalResources();
				System.out.println("Closing!");
			}
		});
		
		factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		ServerBootstrap bootstrap = new ServerBootstrap(factory);
		bootstrap.setPipelineFactory(new ProtoRpcPipelineFactory(new DatastoreServiceFactory(datastore), openChannels));
		bootstrap.bind(new InetSocketAddress(PORT));
	}
}
