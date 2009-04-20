package net.notdot.bdbdatastore.server;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.notdot.protorpc.Disposable;
import net.notdot.protorpc.RpcFailedError;

import com.google.appengine.base.ApiBase;
import com.google.appengine.base.ApiBase.Integer64Proto;
import com.google.appengine.base.ApiBase.StringProto;
import com.google.appengine.base.ApiBase.VoidProto;
import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.datastore_v3.DatastoreV3.CompositeIndices;
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
import com.google.appengine.datastore_v3.DatastoreV3.GetResponse.Entity;
import com.google.appengine.entity.Entity.CompositeIndex;
import com.google.appengine.entity.Entity.EntityProto;
import com.google.appengine.entity.Entity.Reference;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Transaction;

public class DatastoreService extends
		com.google.appengine.datastore_v3.DatastoreV3.DatastoreService
		implements Service, Disposable {
	final Logger logger = LoggerFactory.getLogger(AppDatastore.class);

	protected Datastore datastore;
	
	protected long next_tx_id = 0;
	protected Map<DatastoreV3.Transaction,Transaction> transactions = new HashMap<DatastoreV3.Transaction,Transaction>();
	
	protected long next_cursor_id = 0;
	protected Map<DatastoreV3.Cursor,AbstractDatastoreResultSet> cursors = new HashMap<DatastoreV3.Cursor,AbstractDatastoreResultSet>();
	
	public DatastoreService(Datastore ds) {
		this.datastore = ds;
	}
		
	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}
	
	public void close() {
		try {
			// Close any open cursors
			for(AbstractDatastoreResultSet cursor : this.cursors.values())
				cursor.close();
			this.cursors.clear();
	
			// Clean up any outstanding transactions
			for(Transaction tx : this.transactions.values())
				if(tx != null)
					tx.abort();
			this.transactions.clear();
		} catch(DatabaseException ex) {
			this.logger.error("Exception encountered while disposing DatastoreService", ex);
		}
	}
	
	protected Transaction getTransaction(DatastoreV3.Transaction handle, AppDatastore ds) {
		return getTransaction(handle, ds, false);
	}
	
	protected Transaction getTransaction(DatastoreV3.Transaction handle, AppDatastore ds, boolean createImplicitTx) {
		if(handle == null || !handle.hasHandle()) {
			// No handle - not in a transaction
			if(createImplicitTx) {
				try {
					return ds.newTransaction();
				} catch(DatabaseException ex) {
					logger.error("Unable to create implicit transaction", ex);
				}
			}
			return null;
		}
		
		Transaction ret = transactions.get(handle);
		if(ret == null) {
			synchronized(transactions) {
				ret = transactions.get(handle);
				if(ret == null) {
					if(!transactions.containsKey(handle))
						throw new RpcFailedError("Invalid transaction handle",
								DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
					if(ds == null)
						return null;
					try {
						ret = ds.newTransaction();
					} catch(DatabaseException ex) {
						throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
					}
					transactions.put(handle, ret);
				}
			}
		}
		return ret;
	}
	
	protected void commitImplicitTransaction(DatastoreV3.Transaction handle, Transaction tx) {
		if((handle == null || !handle.hasHandle()) && tx != null) {
			try {
				tx.commit();
			} catch(DatabaseException ex) {
				throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
			}
		}
	}

	protected void rollbackImplicitTransaction(DatastoreV3.Transaction handle, Transaction tx) {
		if((handle == null || !handle.hasHandle()) && tx != null) {
			try {
				tx.abort();
			} catch(DatabaseException ex) {
				throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
			}
		}
	}

	@Override
	public void beginTransaction(RpcController controller, VoidProto request,
			RpcCallback<DatastoreV3.Transaction> done) {
		DatastoreV3.Transaction tx;
		synchronized(transactions) {
			tx = DatastoreV3.Transaction.newBuilder().setHandle(next_tx_id++).build();
			//The actual transaction object is created on first use
			transactions.put(tx, null);
		}
		done.run(tx);
	}

	@Override
	public void commit(RpcController c, DatastoreV3.Transaction request,
			RpcCallback<VoidProto> done) {
		try {
			Transaction tx = this.getTransaction(request, null);
			if(tx != null)
				tx.commit();
			done.run(VoidProto.getDefaultInstance());
			this.transactions.remove(request);
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void count(RpcController controller, Query request,
			RpcCallback<Integer64Proto> done) {
		String app_id = request.getApp();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);
		
		try {
			AbstractDatastoreResultSet cursor = ds.executeQuery(request);
			int i = 0;
			while(cursor.getNext() != null)
				i++;
			done.run(ApiBase.Integer64Proto.newBuilder().setValue(i).build());
			cursor.close();
		} catch (DatabaseException ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}		
	}

	@Override
	public void createIndex(RpcController controller, CompositeIndex request,
			RpcCallback<Integer64Proto> done) {
		String app_id = request.getAppId();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);
		try {
			ds.addIndex(request, done);
			ds.saveCompositeIndexes();
		} catch (Exception ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void delete(RpcController controller, DeleteRequest request,
			RpcCallback<VoidProto> done) {
		if(request.getKeyCount() == 0) {
			done.run(VoidProto.getDefaultInstance());
			return;
		}
		
		String app_id = request.getKey(0).getApp();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);

		Transaction tx = null;
		try {
			tx = this.getTransaction(request.getTransaction(), ds, true);
			for(Reference ref : request.getKeyList()) {
				if(!ref.getApp().equals(app_id))
					throw new RpcFailedError("All entities must have the same app_id",
							DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
				ds.delete(ref, tx);
			}
			this.commitImplicitTransaction(request.getTransaction(), tx);
			done.run(VoidProto.getDefaultInstance());
		} catch(DeadlockException ex) {
			this.rollbackImplicitTransaction(request.getTransaction(), tx);
			throw new RpcFailedError("Operation was terminated to resolve a deadlock.",
					DatastoreV3.Error.ErrorCode.CONCURRENT_TRANSACTION.getNumber());
		} catch(DatabaseException ex) {
			this.rollbackImplicitTransaction(request.getTransaction(), tx);
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void deleteCursor(RpcController controller, DatastoreV3.Cursor request,
			RpcCallback<VoidProto> done) {
		AbstractDatastoreResultSet cursor;
		
		synchronized(this.cursors) {
			cursor = this.cursors.get(request);
			if(cursor == null)
				throw new RpcFailedError("Invalid cursor", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
			this.cursors.remove(request);
		}
		
		try {
			cursor.close();
			done.run(ApiBase.VoidProto.getDefaultInstance());
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void deleteIndex(RpcController controller, CompositeIndex request,
			RpcCallback<VoidProto> done) {
		String app_id = request.getAppId();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);
		try {
			if(!ds.deleteIndex(request))
				throw new RpcFailedError(String.format("Could not delete index %d: Not found or still building.", request.getId()),
						DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
			ds.saveCompositeIndexes();
			done.run(ApiBase.VoidProto.getDefaultInstance());
		} catch (Exception ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void explain(RpcController controller, Query request,
			RpcCallback<QueryExplanation> done) {
		throw new RpcFailedError("Operation not supported.", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
	}

	@Override
	public void get(RpcController c, GetRequest request,
			RpcCallback<GetResponse> done) {
		
		GetResponse.Builder response = GetResponse.newBuilder();
		if(request.getKeyCount() == 0) {
			done.run(response.build());
			return;
		}
		
		String app_id = request.getKey(0).getApp();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);

		try {
			Transaction tx = this.getTransaction(request.getTransaction(), ds);
			
			for(Reference ref : request.getKeyList()) {
				if(!ref.getApp().equals(app_id)) {
					throw new RpcFailedError("All entities must have the same app_id",
							DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
				}
				Entity.Builder ent = Entity.newBuilder();
				EntityProto entity = ds.get(ref, tx);
				if(entity != null) {
					ent.setEntity(entity);
				}
				response.addEntity(ent);
			}
			done.run(response.build());
		} catch(DeadlockException ex) {
			throw new RpcFailedError("Operation was terminated to resolve a deadlock.",
					DatastoreV3.Error.ErrorCode.CONCURRENT_TRANSACTION.getNumber());
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void getIndices(RpcController controller, StringProto request,
			RpcCallback<CompositeIndices> done) {
		AppDatastore ds = this.datastore.getAppDatastore(request.getValue());
		DatastoreV3.CompositeIndices response = ds.getIndices();
		done.run(response);
	}

	@Override
	public void getSchema(RpcController controller, StringProto request,
			RpcCallback<Schema> done) {
		throw new RpcFailedError("Operation not supported.", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
	}

	@Override
	public void next(RpcController controller, NextRequest request,
			RpcCallback<QueryResult> done) {
		QueryResult.Builder response = QueryResult.newBuilder();
		
		AbstractDatastoreResultSet cursor = this.cursors.get(request.getCursor());
		if(cursor == null)
			throw new RpcFailedError("Invalid cursor", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
		
		try {
			response.setCursor(request.getCursor());
			response.addAllResult(cursor.getNext(request.getCount()));
			response.setMoreResults(cursor.hasMore());
			
			done.run(response.build());
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void put(RpcController c, PutRequest request,
			RpcCallback<PutResponse> done) {
		PutResponse.Builder response = PutResponse.newBuilder();
		if(request.getEntityCount() == 0) {
			done.run(response.build());
			return;
		}
		
		String app_id = request.getEntity(0).getKey().getApp();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);
		
		Transaction tx = null;
		try {
			tx = this.getTransaction(request.getTransaction(), ds, true);
			for(EntityProto ent : request.getEntityList()) {
				if(!ent.getKey().getApp().equals(app_id)) {
					throw new RpcFailedError("All entities must have the same app_id",
							DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
				}
				response.addKey(ds.put(ent, tx));
			}
			this.commitImplicitTransaction(request.getTransaction(), tx);
			done.run(response.build());
		} catch(DeadlockException ex) {
			this.rollbackImplicitTransaction(request.getTransaction(), tx);
			throw new RpcFailedError("Operation was terminated to resolve a deadlock.",
					DatastoreV3.Error.ErrorCode.CONCURRENT_TRANSACTION.getNumber());
		} catch(DatabaseException ex) {
			this.rollbackImplicitTransaction(request.getTransaction(), tx);
			throw new RpcFailedError(ex, DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void rollback(RpcController c, DatastoreV3.Transaction request,
			RpcCallback<VoidProto> done) {
		try {
			Transaction tx = this.getTransaction(request, null);
			if(tx != null)
				tx.abort();
			done.run(VoidProto.getDefaultInstance());
			this.transactions.remove(request);
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex.toString(),
					DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void runQuery(RpcController controller, Query request,
			RpcCallback<QueryResult> done) {
		if(!request.hasKind())
			throw new RpcFailedError("All queries must specify a kind.", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
		
		QueryResult.Builder response = QueryResult.newBuilder();
		
		String app_id = request.getApp();
		AppDatastore ds = this.datastore.getAppDatastore(app_id);
		
		try {
			AbstractDatastoreResultSet results = ds.executeQuery(request);
			if(results == null)
				throw new RpcFailedError(String.format("No index found to satisfy query %s", request),
						DatastoreV3.Error.ErrorCode.NEED_INDEX.getNumber());
			DatastoreV3.Cursor dscursor;
			synchronized(this.cursors) {
				dscursor = DatastoreV3.Cursor.newBuilder().setCursor(next_cursor_id++).build();
			}
			this.cursors.put(dscursor, results);
			response.setCursor(dscursor);
			response.setMoreResults(true);
			done.run(response.build());
		} catch(DatabaseException ex) {
			throw new RpcFailedError(ex.toString(),
					DatastoreV3.Error.ErrorCode.INTERNAL_ERROR.getNumber());
		}
	}

	@Override
	public void updateIndex(RpcController controller, CompositeIndex request,
			RpcCallback<VoidProto> done) {
		throw new RpcFailedError("Operation not supported.", DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
	}

}
