package net.notdot.bdbdatastore.server;

import java.util.ArrayList;
import java.util.List;

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
	
	public static List<FilterSpec> FromQuery(DatastoreV3.Query query) {
		List<FilterSpec> ret = new ArrayList<FilterSpec>();
		
		for(DatastoreV3.Query.Filter filter : query.getFilterList()) {
			for(Entity.Property prop : filter.getPropertyList()) {
				ret.add(new FilterSpec(prop.getName(), filter.getOp(), prop.getValue()));
			}
		}
		return ret;
	}
	
	@Override
	public int compareTo(FilterSpec arg0) {
		int ret = name.asReadOnlyByteBuffer().compareTo(arg0.name.asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		ret = operator - arg0.operator;
		if(ret != 0)
			return ret;
		return PropertyValueComparator.instance.compare(this.value, arg0.value);
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
}
