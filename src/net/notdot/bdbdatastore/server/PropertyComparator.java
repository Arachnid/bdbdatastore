package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import com.google.appengine.entity.Entity;

public class PropertyComparator implements Comparator<Entity.Property> {

	public int compare(Entity.Property o1, Entity.Property o2) {
		int ret = o1.getName().asReadOnlyByteBuffer().compareTo(o2.getName().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		
		ret = o1.getMeaning().compareTo(o2.getMeaning());
		if(ret != 0)
			return ret;
		
		return PropertyValueComparator.instance.compare(o1.getValue(), o2.getValue());
	}
}
