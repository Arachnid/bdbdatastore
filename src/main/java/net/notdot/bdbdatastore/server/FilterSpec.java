package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.appengine.datastore_v3.DatastoreV3;
import com.google.appengine.entity.Entity;
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
		
		// Sort filters for each property so they can be used in indexes.
		for(List<FilterSpec> fl : ret.values())
			Collections.sort(fl);
		
		return ret;
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
