package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
	protected boolean hasInequalities;
	
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
						this.hasInequalities = true;
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
		Entity.PropertyValue inequalityBound = null;
		Iterator<Entity.Index.Property> iter = idx.getPropertyList().iterator();
		int currentDirection = 0;
		ByteString currentProperty = null;
		
		for(FilterSpec filter : this.filters) {
			if(currentProperty == null || !currentProperty.equals(filter.getName())) {
				currentDirection = direction * (iter.next().getDirection()==Entity.Index.Property.Direction.ASCENDING?1:-1);
				currentProperty = filter.getName();
			}

			switch(filter.getOperator()) {
			case 1: // Less than
				if(currentDirection == -1 && (inequalityBound == null || PropertyValueComparator.instance.compare(filter.getValue(), inequalityBound) > 0)) {
					inequalityBound = filter.getValue();
					exclusiveBound = true;
				}
				break;
			case 2: // Less than or equal
				if(currentDirection == -1 && (inequalityBound == null || PropertyValueComparator.instance.compare(filter.getValue(), inequalityBound) >= 0)) {
					inequalityBound = filter.getValue();
					exclusiveBound = false;
				}
				break;
			case 3: // Greater than
				if(currentDirection == 1 && (inequalityBound == null || PropertyValueComparator.instance.compare(filter.getValue(), inequalityBound) < 0)) {
					inequalityBound = filter.getValue();
					exclusiveBound = true;
				}
				break;
			case 4: // Greater than or equal
				if(currentDirection == 1 && (inequalityBound == null || PropertyValueComparator.instance.compare(filter.getValue(), inequalityBound) <= 0)) {
					inequalityBound = filter.getValue();
					exclusiveBound = false;
				}
				break;
			case 5: // Equal
				bounds.add(filter.getValue());
			}
		}
		if(inequalityBound != null) {
			bounds.add(inequalityBound);
		} else {
			// Get the direction of the next field
			if(iter.hasNext()) {
				currentDirection = direction * (iter.next().getDirection()==Entity.Index.Property.Direction.ASCENDING?1:-1);
			} else {
				currentDirection = 1;
			}
			if(currentDirection == -1) {
				// Upper bound needs a delimiter
				bounds.add(Entity.PropertyValue.getDefaultInstance());
			}
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
		return this.hasInequalities;
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
