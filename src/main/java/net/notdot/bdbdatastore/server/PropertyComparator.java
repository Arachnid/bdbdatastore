package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import com.google.appengine.entity.Entity;

public class PropertyComparator implements Comparator<Entity.Property> {
	public static final PropertyComparator instance = new PropertyComparator(false);
	public static final PropertyComparator noValueInstance = new PropertyComparator(true);
	
	private boolean compareValues;
	
	public PropertyComparator(boolean compareValues) {
		this.compareValues = compareValues;
	}
	
	public int compare(Entity.Property o1, Entity.Property o2) {
		int ret = o1.getName().asReadOnlyByteBuffer().compareTo(o2.getName().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		
		ret = o1.getMeaning().compareTo(o2.getMeaning());
		if(ret != 0)
			return ret;
		
		if(!compareValues)
			return 0;
		return PropertyValueComparator.instance.compare(o1.getValue(), o2.getValue());
	}
}
