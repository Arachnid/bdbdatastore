package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.notdot.protorpc.RpcFailedError;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
import com.google.protobuf.ByteString;

public class QuerySpec {
	protected String app;
	protected ByteString kind;
	protected Entity.Reference ancestor = null;
	protected Map<ByteString, List<FilterSpec>> filters = new HashMap<ByteString, List<FilterSpec>>();
	protected List<DatastoreV3.Query.Order> orders = new ArrayList<DatastoreV3.Query.Order>();
	protected int offset = 0;
	protected int limit = -1;
	
	protected Entity.Index index = null;
	protected boolean _hasInequalities;
	
	public QuerySpec(DatastoreV3.Query query) {
		this.app = query.getApp();
		if(!query.hasKind())
			throw new RpcFailedError("Queries must specify a kind",
					DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
		this.kind = query.getKind();
		if(query.hasAncestor())
			this.ancestor = query.getAncestor();
		this.filters = FilterSpec.FromQuery(query);
		this.orders = query.getOrderList();
		if(query.hasOffset())
			this.offset = query.getOffset();
		if(query.hasLimit())
			this.limit = query.getLimit();
	}
	
	public Entity.Index getIndex() {
		// TODO: Refactor this to support multiple possible indexes to satisfy the same query.
		if(this.index == null) {
			Entity.Index.Builder builder = Entity.Index.newBuilder();
			builder.setEntityType(this.kind);
			builder.setAncestor(this.ancestor != null);
			ByteString inequalityprop = null;
			
			// Add all equality filters
			for(List<FilterSpec> filterList : this.filters.values()) {
				for(FilterSpec filter : filterList) {
					if(filter.getOperator() == DatastoreV3.Query.Filter.Operator.EQUAL.getNumber()) {
						builder.addProperty(Entity.Index.Property.newBuilder()
							.setName(filter.getName())
							.setDirection(Entity.Index.Property.Direction.ASCENDING));
					} else {
						this._hasInequalities = true;
						if(inequalityprop != null && !filter.getName().equals(inequalityprop))
							throw new RpcFailedError("Only one inequality property is permitted per query",
									DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
						inequalityprop = filter.getName();
					}
				}
			}
			
			if(inequalityprop != null && this.orders.size() > 0 && !this.orders.get(0).getProperty().equals(inequalityprop))
				throw new RpcFailedError("First sort order must match inequality property",
						DatastoreV3.Error.ErrorCode.BAD_REQUEST.getNumber());
			
			// If there's no sort orders, add the inequality, ascending
			if(inequalityprop != null && this.orders.size() == 0)
				builder.addProperty(Entity.Index.Property.newBuilder().setName(inequalityprop));
				
			// Add the sort orders
			for(DatastoreV3.Query.Order order : this.orders) {
				builder.addProperty(Entity.Index.Property.newBuilder()
						.setName(order.getProperty())
						.setDirection(Entity.Index.Property.Direction.valueOf(order.getDirection())));
			}
			
			this.index = builder.build();
		}
		return this.index;
	}

	public boolean getBounds(Entity.Index idx, int direction, List<Entity.PropertyValue> bounds) {
		boolean exclusiveBound = false;
		
		for(Entity.Index.Property prop : idx.getPropertyList()) {
			int currentDirection = direction * (prop.getDirection()==Entity.Index.Property.Direction.ASCENDING?1:-1);
			List<FilterSpec> filterList = this.filters.get(prop.getName());
			Entity.PropertyValue currentBound = null;
			if(filterList != null) {
				// Property is filtered on - figure out the appropriate bounds
				int cmp = 0;
				for(FilterSpec filter : this.filters.get(prop.getName())) {
					if(currentBound != null)
						cmp = PropertyValueComparator.instance.compare(filter.getValue(), currentBound);
					switch(filter.getOperator()) {
					case 1: // Less than
						if(currentDirection == -1 && (currentBound == null || cmp > 0)) {
							currentBound = filter.getValue();
							exclusiveBound = true;
						}
						break;
					case 2: // Less than or equal
						if(currentDirection == -1 && (currentBound == null || cmp >= 0)) {
							currentBound = filter.getValue();
							exclusiveBound = false;
						}
						break;
					case 3: // Greater than
						if(currentDirection == 1 && (currentBound == null || cmp < 0)) {
							currentBound = filter.getValue();
							exclusiveBound = true;
						}
						break;
					case 4: // Greater than or equal
						if(currentDirection == 1 && (currentBound == null || cmp <= 0)) {
							currentBound = filter.getValue();
							exclusiveBound = false;
						}
						break;
					case 5: // Equal
						currentBound = filter.getValue();
					}
				}
				if(currentBound != null)
					bounds.add(currentBound);
			}
			// First unfiltered property - add a sentinel if it's the upper bound
			if(currentBound == null && currentDirection == -1)
				bounds.add(Entity.PropertyValue.getDefaultInstance());
		}
		return exclusiveBound;
	}

	public String getApp() {
		return app;
	}

	public ByteString getKind() {
		return kind;
	}

	public Entity.Reference getAncestor() {
		return ancestor;
	}
	
	public boolean hasAncestor() {
		return ancestor != null;
	}
	
	public boolean hasInequalities() {
		if(this.index == null)
			this.getIndex();
		return this._hasInequalities;
	}

	public Map<ByteString, List<FilterSpec>> getFilters() {
		return filters;
	}

	public List<DatastoreV3.Query.Order> getOrders() {
		return orders;
	}

	public int getOffset() {
		return offset;
	}

	public int getLimit() {
		return limit;
	}
}
