package net.notdot.bdbdatastore.server;

import net.notdot.protorpc.ProtoRpcController;

import com.google.appengine.base.ApiBase.Integer64Proto;
import com.google.appengine.base.ApiBase.StringProto;
import com.google.appengine.base.ApiBase.VoidProto;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.datastore_v3.DatastoreV3.CompositeIndices;
import com.google.appengine.datastore_v3.DatastoreV3.Cursor;
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
import com.google.protobuf.Service;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;

public class DatastoreService extends
		com.google.appengine.datastore_v3.DatastoreV3.DatastoreService
		implements Service {
	protected Datastore datastore;
	
	public DatastoreService(Datastore ds) {
		this.datastore = ds;
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
		AppDatastore datastore = this.datastore.getAppDatastore(controller, app_id);
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
		AppDatastore datastore = this.datastore.getAppDatastore(controller, app_id);
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

}
