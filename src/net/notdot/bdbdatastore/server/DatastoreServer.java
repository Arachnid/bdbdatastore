package net.notdot.bdbdatastore.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import net.notdot.protorpc.ProtoRpcController;
import net.notdot.protorpc.ProtoRpcHandler;
import net.notdot.protorpc.Rpc;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.protobuf.ProtobufCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.base.ApiBase.Integer64Proto;
import com.google.appengine.base.ApiBase.StringProto;
import com.google.appengine.base.ApiBase.VoidProto;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.datastore_v3.DatastoreV3.CompositeIndices;
import com.google.appengine.datastore_v3.DatastoreV3.Cursor;
import com.google.appengine.datastore_v3.DatastoreV3.DatastoreService;
import com.google.appengine.datastore_v3.DatastoreV3.DeleteRequest;
import com.google.appengine.datastore_v3.DatastoreV3.GetRequest;
import com.google.appengine.datastore_v3.DatastoreV3.GetResponse;
import com.google.appengine.datastore_v3.DatastoreV3.NextRequest;
import com.google.appengine.datastore_v3.DatastoreV3.PutRequest;
import com.google.appengine.datastore_v3.DatastoreV3.PutResponse;
import com.google.appengine.datastore_v3.DatastoreV3.Query;
import com.google.appengine.datastore_v3.DatastoreV3.QueryExplanation;
import com.google.appengine.datastore_v3.DatastoreV3.QueryResult;
import com.google.appengine.datastore_v3.DatastoreV3.Schema;
import com.google.appengine.datastore_v3.DatastoreV3.Transaction;
import com.google.appengine.datastore_v3.DatastoreV3.GetResponse.Entity;
import com.google.appengine.entity.Entity.CompositeIndex;
import com.google.appengine.entity.Entity.EntityProto;
import com.google.appengine.entity.Entity.Reference;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.EnvironmentLockedException;

public class DatastoreServer extends DatastoreService {
	final Logger logger = LoggerFactory.getLogger(DatastoreServer.class);
	
	protected String basedir;
	protected Map<String, AppDatastore> datastores = new HashMap<String, AppDatastore>();
		
	protected DatastoreServer(String basedir) {
		this.basedir = basedir;
	}
	
	protected AppDatastore getDatastore(ProtoRpcController controller, String app_id) {
		AppDatastore ret = datastores.get(app_id);
		if(ret != null)
			return ret;
		synchronized(datastores) {
			ret = datastores.get(app_id);
			if(ret != null)
				return ret;
			try {
				ret = new AppDatastore(basedir, app_id);
				datastores.put(app_id, ret);
				return ret;
			} catch(DatabaseException ex) {
				logger.error("Could not open datastore for app {}: {}", app_id, ex);
				controller.setFailed(String.format("Unable to get datastore instance for app '%s'", app_id));
				controller.setApplicationError(DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
				return null;
			}
		}
	}
	
	public void close() {
		synchronized(datastores) {
			for(Map.Entry<String,AppDatastore> entry : datastores.entrySet()) {
				try {
					entry.getValue().close();
				} catch(DatabaseException ex) {
					logger.error("Error shutting down datastore for app '{}': {}", entry.getKey(), ex);
				}
			}
		}
	}
	
	@Override
	public void beginTransaction(RpcController controller, VoidProto request,
			RpcCallback<Transaction> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(RpcController controller, Transaction request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void count(RpcController controller, Query request,
			RpcCallback<Integer64Proto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createIndex(RpcController controller, CompositeIndex request,
			RpcCallback<Integer64Proto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(RpcController controller, DeleteRequest request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteCursor(RpcController controller, Cursor request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteIndex(RpcController controller, CompositeIndex request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void explain(RpcController controller, Query request,
			RpcCallback<QueryExplanation> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void get(RpcController c, GetRequest request,
			RpcCallback<GetResponse> done) {
		ProtoRpcController controller = (ProtoRpcController)c;
		
		GetResponse.Builder response = GetResponse.newBuilder();
		if(request.getKeyCount() == 0) {
			done.run(response.build());
			return;
		}
		
		String app_id = request.getKey(0).getApp();
		AppDatastore datastore = this.getDatastore(controller, app_id);
		if(datastore == null)
			return;
		try {
			for(Reference ref : request.getKeyList()) {
				if(ref.getApp() != app_id) {
					controller.setFailed("All entities must have the same app_id");
					controller.setApplicationError(DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
					return;
				}
				Entity.Builder ent = Entity.newBuilder();
				EntityProto entity = datastore.get(ref);
				if(entity != null) {
					ent.setEntity(entity);
				}
				response.addEntity(ent);
			}
			done.run(response.build());
		} catch(DeadlockException ex) {
			controller.setFailed("Operation was terminated to resolve a deadlock.");
			controller.setApplicationError(DatastoreV3.Error.ErrorCode.CONCURRENT_TRANSACTION.getNumber());
		} catch(DatabaseException ex) {
			controller.setFailed(ex.toString());
			controller.setApplicationError(DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void getIndices(RpcController controller, StringProto request,
			RpcCallback<CompositeIndices> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getSchema(RpcController controller, StringProto request,
			RpcCallback<Schema> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void next(RpcController controller, NextRequest request,
			RpcCallback<QueryResult> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void put(RpcController c, PutRequest request,
			RpcCallback<PutResponse> done) {
		ProtoRpcController controller = (ProtoRpcController)c;
		
		PutResponse.Builder response = PutResponse.newBuilder();
		if(request.getEntityCount() == 0) {
			done.run(response.build());
			return;
		}
		
		String app_id = request.getEntity(0).getKey().getApp();
		AppDatastore datastore = this.getDatastore(controller, app_id);
		if(datastore == null)
			return;
		try {
			for(EntityProto ent : request.getEntityList())
				response.addKey(datastore.put(ent));
			done.run(response.build());
		} catch(DatabaseException ex) {
			controller.setFailed(ex.toString());
		}
	}

	@Override
	public void rollback(RpcController controller, Transaction request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runQuery(RpcController controller, Query request,
			RpcCallback<QueryResult> done) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateIndex(RpcController controller, CompositeIndex request,
			RpcCallback<VoidProto> done) {
		// TODO Auto-generated method stub
		
	}

	private static DatastoreServer server;
	private static final int PORT = 9123;
	private static final String PATH = "datastore";

	/**
	 * @param args
	 * @throws IOException 
	 * @throws DatabaseException 
	 * @throws EnvironmentLockedException 
	 */
	public static void main(String[] args) throws IOException, EnvironmentLockedException, DatabaseException {
		server = new DatastoreServer(PATH);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				server.close();
				System.out.println("Closing!");
			}
		});

		IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter(ProtobufCodecFactory.newInstance(Rpc.Request.getDefaultInstance())));
        acceptor.setHandler( new ProtoRpcHandler(new DatastoreServer(PATH)));
    	acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 60 );
        acceptor.bind( new InetSocketAddress(PORT) );
	}
}
