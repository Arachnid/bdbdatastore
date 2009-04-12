package net.notdot.bdbdatastore.server;

import java.io.File;
import java.io.FileInputStream;
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
	static final Logger logger = LoggerFactory.getLogger(DatastoreServer.class);

	private static Datastore datastore;
	private static ChannelFactory factory;
	private static ChannelGroup openChannels = new DefaultChannelGroup("DatastoreServer");
	
	public static TypedProperties properties = new TypedProperties();

	/**
	 * @param args
	 * @throws IOException 
	 * @throws DatabaseException 
	 * @throws EnvironmentLockedException 
	 */
	public static void main(String[] args) throws IOException, EnvironmentLockedException, DatabaseException {
		if(args.length != 1) {
			System.out.println(String.format("Usage: DatastoreServer <datastoredir>"));
			return;
		}
		
		String datastore_path = args[0];
		File prop_path = new File(datastore_path, "datastore.properties");
		if(prop_path.exists())
			properties.load(new FileInputStream(prop_path));
		
		datastore = new Datastore(datastore_path);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				ChannelGroupFuture future = openChannels.close();
				future.awaitUninterruptibly();
				datastore.close();
				factory.releaseExternalResources();
			}
		});
		
		int max_pb_size = properties.getInt("datastore.max_pb_size", 1048576);
		
		factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		ServerBootstrap bootstrap = new ServerBootstrap(factory);
		DatastoreServiceFactory ds_factory = new DatastoreServiceFactory(datastore);
		bootstrap.setPipelineFactory(new ProtoRpcPipelineFactory(ds_factory, openChannels, max_pb_size));
		bootstrap.bind(new InetSocketAddress(properties.getInt("datastore.port", 9123)));
		logger.info("Server started.");
	}
}
