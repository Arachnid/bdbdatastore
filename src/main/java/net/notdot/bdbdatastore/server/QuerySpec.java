package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Arrays;
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
	public static final ByteString KEY_PROPERTY = ByteString.copyFromUtf8("__key__");
	
	protected String app;
	protected ByteString kind;
	protected Entity.Reference ancestor = null;
	protected Map<ByteString, List<FilterSpec>> filters = new HashMap<ByteString, List<FilterSpec>>();
	protected List<DatastoreV3.Query.Order> orders = new ArrayList<DatastoreV3.Query.Order>();
	protected int offset = 0;
	protected int limit = -1;
	
	// Used for matching indexes to queries
	private Entity.Index index = null;
	private List<ByteString> unordered_properties = null;
	private List<DatastoreV3.Query.Order> ordered_properties = null;
	private boolean hasInequalities = false;
	
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
	
	private void buildMatchData() {
		if(unordered_properties != null)
			return;
		
		// Build a list of properties used in equality filters.
		// We don't care what order they appear in the index, as long as it's
		// before any of the ordered properties.
		unordered_properties = new ArrayList<ByteString>();
		ByteString inequality_property = null;
		for(Map.Entry<ByteString, List<FilterSpec>> entry : this.filters.entrySet()) {
			for(FilterSpec filter : entry.getValue()) {
				if(filter.operator == DatastoreV3.Query.Filter.Operator.EQUAL.getNumber()) {
					unordered_properties.add(filter.name);
				} else {
					inequality_property = filter.name;
				}
			}
		}
		Collections.sort(unordered_properties, ByteStringComparator.instance);
		
		// Build a list of properties used in sort orders and inequality filters.
		if(this.orders.size() == 0 && inequality_property != null) {
			ordered_properties = new ArrayList<DatastoreV3.Query.Order>();
			ordered_properties.add(DatastoreV3.Query.Order.newBuilder()
					.setProperty(inequality_property)
					.setDirection(DatastoreV3.Query.Order.Direction.ASCENDING.getNumber())
					.build());
		} else {
			ordered_properties = this.orders;
		}
	}
	
	public boolean isValidIndex(Entity.Index index) {
		this.buildMatchData();
		
		if(!index.getEntityType().equals(this.kind))
			return false;
		
		if(unordered_properties.size() + ordered_properties.size() != index.getPropertyCount())
			return false;
		
		// Check that properties match as expected
		int unordered_count = unordered_properties.size();
		ByteString[] index_unordered_props = new ByteString[unordered_count];
		for(int i = 0; i < index.getPropertyCount(); i++) {
			Entity.Index.Property prop = index.getProperty(i);
			if(i < unordered_count) {
				// Check the list of unordered properties matches later
				index_unordered_props[i] = prop.getName();
			} else {
				// Ordered property must have the same name and sort order
				DatastoreV3.Query.Order orderprop = ordered_properties.get(i - unordered_count);
				if(!orderprop.getProperty().equals(prop.getName()))
					return false;
				if(i > unordered_count || this.orders.size() > 0) {
					// ... but we only care about order if there are sort orders
					if(orderprop.getDirection() != prop.getDirection().getNumber())
						return false;
				}
			}
		}
		
		// Check that the unordered properties from index and query match
		Arrays.sort(index_unordered_props, ByteStringComparator.instance);
		for(int i = 0; i < unordered_count; i++)
			if(!unordered_properties.get(i).equals(index_unordered_props[i]))
				return false;
		
		return true;
	}

	public Entity.Index getIndex() {
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
		Map<ByteString, Iterator<FilterSpec>> filters = new HashMap<ByteString, Iterator<FilterSpec>>();

		// Create an iterator for each item in the list
		for(Map.Entry<ByteString, List<FilterSpec>> entry : this.filters.entrySet())
			filters.put(entry.getKey(), entry.getValue().iterator());
		
		// Iterate through each property in the index
		for(Entity.Index.Property prop : idx.getPropertyList()) {
			boolean filtered = false;

			// Find the effective sort direction
			int currentDirection = direction * (prop.getDirection()==Entity.Index.Property.Direction.ASCENDING?1:-1);
			
			// Get an iterator for all the filters for this property
			Iterator<FilterSpec> iter = filters.get(prop.getName());
			FilterSpec filter = null;
			if(iter != null) {
			filters:
				// Iterate over the filters until we find a filter that fits the slot
				while(iter.hasNext()) {
					filter = iter.next();
					switch(filter.getOperator()) {
					case 1: // Less than
						if(currentDirection == -1) {
							filtered = true;
							exclusiveBound = true;
							break filters;
						}
						break;
					case 2: // Less than or equal
						if(currentDirection == -1) {
							filtered = true;
							exclusiveBound = false;
							break filters;
						}
						break;
					case 3: // Greater than
						if(currentDirection == 1) {
							filtered = true;
							exclusiveBound = true;
							break filters;
						}
						break;
					case 4: // Greater than or equal
						if(currentDirection == 1) {
							filtered = true;
							exclusiveBound = false;
							break filters;
						}
						break;
					case 5: // Equal
						filtered = true;
						exclusiveBound = false;
						break filters;
					}
				}
			}
			if(filtered) {
				if(!iter.hasNext())
					filters.remove(prop.getName());
				bounds.add(filter.getValue());
			} else {
				// First unfiltered property - add a sentinel if it's the upper bound
				if(currentDirection == -1)
					bounds.add(Entity.PropertyValue.getDefaultInstance());
				return exclusiveBound;
			}
		}
		// TODO: Check for unused filters and error
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
