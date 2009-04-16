package net.notdot.bdbdatastore.server;

import java.util.Comparator;
import com.google.appengine.entity.Entity;

public class PropertyValueComparator implements Comparator<Entity.PropertyValue> {
	public static final PropertyValueComparator instance = new PropertyValueComparator();
	
	protected int getValueType(Entity.PropertyValue p) {
		if(p.hasInt64Value())
			return 1;
		if(p.hasBooleanValue())
			return 2;
		if(p.hasStringValue())
			return 3;
		if(p.hasDoubleValue())
			return 4;
		if(p.hasPointValue())
			return 5;
		if(p.hasUserValue())
			return 6;
		if(p.hasReferenceValue())
			return 7;
		return 0;
	}
	
	protected int compareLongs(long i1, long i2) {
		if(i1 == i2)
			return 0;
		if(i1 < i2)
			return -1;
		return 1;
	}
	
	protected int compareBools(boolean b1, boolean b2) {
		if(!b1 && b2)
			return -1;
		if(b1 && !b2)
			return 1;
		return 0;
	}
	
	protected int compareDoubles(double d1, double d2) {
		if(d1 == d2)
			return 0;
		if(d1 < d2)
			return -1;
		return 1;
	}

	public int compare(Entity.PropertyValue p1, Entity.PropertyValue p2) {
		int ret;
		int p1type = this.getValueType(p1);
		int p2type = this.getValueType(p2);
		if(p1type != p2type)
			return p1type - p2type;
		
		switch(p1type) {
		case 1: // Integer
			return this.compareLongs(p1.getInt64Value(), p2.getInt64Value());
		case 2: // Boolean
			return this.compareBools(p1.getBooleanValue(), p2.getBooleanValue());
		case 3: // String
			return p1.getStringValue().asReadOnlyByteBuffer().compareTo(p2.getStringValue().asReadOnlyByteBuffer());
		case 4: // Double
			return this.compareDoubles(p1.getDoubleValue(), p2.getDoubleValue());
		case 5: // Point
			ret = this.compareDoubles(p1.getPointValue().getX(), p2.getPointValue().getX());
			if(ret != 0)
				return ret;
			return this.compareDoubles(p1.getPointValue().getY(), p2.getPointValue().getY());
		case 6: // User
			return p1.getUserValue().getEmail().asReadOnlyByteBuffer().compareTo(p2.getUserValue().getEmail().asReadOnlyByteBuffer());
		case 7: // Reference
			return ReferenceComparator.instance.compare(
					ReferenceComparator.toReference(p1.getReferenceValue()),
					ReferenceComparator.toReference(p2.getReferenceValue()));
		}
		return 0;
	}
}
