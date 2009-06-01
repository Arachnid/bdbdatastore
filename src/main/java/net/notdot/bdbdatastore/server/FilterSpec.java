package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.PropertyValue;
import com.google.protobuf.ByteString;

public class FilterSpec implements Comparable<FilterSpec> {
	protected ByteString name;
	protected int operator;
	protected Entity.PropertyValue value;
	
	public FilterSpec(ByteString name, int operator, Entity.PropertyValue value) {
		this.name = name;
		this.operator = operator;
		this.value = value;
	}
	
	public static Map<ByteString, List<FilterSpec>> FromQuery(DatastoreV3.Query query) {
		Map<ByteString, List<FilterSpec>> ret = new HashMap<ByteString, List<FilterSpec>>();
		
		for(DatastoreV3.Query.Filter filter : query.getFilterList()) {
			for(Entity.Property prop : filter.getPropertyList()) {
				List<FilterSpec> filters = ret.get(prop.getName());
				if(filters == null) {
					filters = new ArrayList<FilterSpec>();
					ret.put(prop.getName(), filters);
				}
				filters.add(new FilterSpec(prop.getName(), filter.getOp(), prop.getValue()));
			}
		}
		
		// Remove any redundant filters
		for(List<FilterSpec> filters : ret.values()) {
			FilterSpec minBound = null;
			FilterSpec maxBound = null;
			boolean hasEquality = false;
			
			for(int i = filters.size() - 1; i >= 0; i--) {
				FilterSpec filter = filters.get(i);
				Entity.PropertyValue value = filter.getValue();
				
				if(!evaluateFilter(minBound, value)) {
					if(filter.getOperator() == DatastoreV3.Query.Filter.Operator.GREATER_THAN.getNumber()
					   || filter.getOperator() == DatastoreV3.Query.Filter.Operator.GREATER_THAN_OR_EQUAL.getNumber()) {
						// Filter is redundant
						filters.remove(i);
					} else {
						// Filter is contradictory
						return null;
					}
				} else if(!evaluateFilter(maxBound, value)) {
					if(filter.getOperator() == DatastoreV3.Query.Filter.Operator.LESS_THAN.getNumber()
					   || filter.getOperator() == DatastoreV3.Query.Filter.Operator.LESS_THAN_OR_EQUAL.getNumber()) {
						// Filter is redundant
						filters.remove(i);
					} else {
						// Filter is contradictory
						return null;
					}
				} else {
					// Check if this filter provides a tighter bound than we've already seen
					switch(filter.getOperator()) {
					case 1: // LESS_THAN
					case 2: // LESS_THAN_OR_EQUAL
						if(maxBound != null)
							filters.remove(maxBound);
						maxBound = filter;
						break;
					case 3: // GREATER_THAN
					case 4: // GREATER_THAN_OR_EQUAL
						if(minBound != null)
							filters.remove(minBound);
						minBound = filter;
						break;
					case 5: // EQUAL
						hasEquality = true;
						break;
					}
				}
			}
			
			if(hasEquality) {
				// In the presence of equality filters, inequalities are extraneous
				if(minBound != null)
					filters.remove(minBound);
				if(maxBound != null)
					filters.remove(maxBound);
			}
		}
		
		// Sort filters for each property so they can be used in indexes.
		for(List<FilterSpec> fl : ret.values())
			Collections.sort(fl);
		
		return ret;
	}
	
	private static boolean evaluateFilter(FilterSpec filter, PropertyValue value) {
		if(filter == null)
			return true;
		
		int cmp = PropertyValueComparator.instance.compare(value, filter.getValue());
		switch(filter.getOperator()) {
		case 1: // LESS_THAN
			return cmp < 0;
		case 2: // LESS_THAN_OR_EQUAL
			return cmp <= 0;
		case 3: // GREATER_THAN
			return cmp > 0;
		case 4: // GREATER_THAN_OR_EQUAL
			return cmp >= 0;
		case 5: // EQUAL
			return cmp == 0;
		default:
			// TODO: Better error handling
			return false;
		}
	}

	public ByteString getName() {
		return name;
	}

	public int getOperator() {
		return operator;
	}

	public Entity.PropertyValue getValue() {
		return value;
	}

	public int compareTo(FilterSpec o) {
		int ret = o.operator - operator; // Reversed so equality comes first
		if(ret != 0)
			return ret;
		ret = name.asReadOnlyByteBuffer().compareTo(o.name.asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		return PropertyValueComparator.instance.compare(this.value, o.value);
	}
}
