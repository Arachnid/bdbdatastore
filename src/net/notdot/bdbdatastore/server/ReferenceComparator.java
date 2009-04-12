package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.Path;
import com.google.protobuf.ByteString;

public class ReferenceComparator implements Comparator<Entity.Reference> {
	public final static ReferenceComparator instance = new ReferenceComparator();
	
	public static Entity.Reference toReference(Entity.PropertyValue.ReferenceValue refval) {
		Entity.Path.Builder path = Entity.Path.newBuilder();
		for(Entity.PropertyValue.ReferenceValue.PathElement pathel : refval.getPathElementList()) {
			Entity.Path.Element.Builder element = Entity.Path.Element.newBuilder();
			element.setType(pathel.getType());
			if(pathel.hasName())
				element.setName(pathel.getName());
			if(pathel.hasId())
				element.setId(pathel.getId());
			path.addElement(element);
		}
		
		return Entity.Reference.newBuilder().setApp(refval.getApp()).setPath(path).build();
	}
	
	protected int compareElements(Entity.Path.Element e1, Entity.Path.Element e2) {
		int ret = e1.getType().asReadOnlyByteBuffer().compareTo(e2.getType().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		//IDs are considered less than names
		if(e1.hasId()) {
			if(e2.hasName())
				return -1;
			long e1id = e1.getId();
			long e2id = e2.getId();
			if(e1id < e2id)
				return -1;
			if(e1id > e2id)
				return 1;
			return 0;
		} else {
			if(e2.hasId())
				return 1;
			return e1.getName().asReadOnlyByteBuffer().compareTo(e2.getName().asReadOnlyByteBuffer());
		}
	}
	
	public int compare(Entity.Reference o1, Entity.Reference o2) {
		// Compare App IDs
		int ret = o1.getApp().compareTo(o2.getApp());
		if(ret != 0)
			return ret;
		
		Entity.Path p1 = o1.getPath();
		Entity.Path p2 = o2.getPath();
		
		// Compare types
		ByteString p1type = p1.getElement(p1.getElementCount() - 1).getType();
		ByteString p2type = p2.getElement(p2.getElementCount() - 1).getType();
		ret = p1type.asReadOnlyByteBuffer().compareTo(p2type.asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		
		// Compare paths
		return comparePaths(p1, p2);
	}

	private int comparePaths(Path p1, Path p2) {
		int p1len = p1.getElementCount();
		int p2len = p2.getElementCount();
		int ret = 0;
		for(int i = 0; i < Math.min(p1len, p2len); i++) {
			ret = compareElements(p1.getElement(i), p2.getElement(i));
			if(ret != 0)
				return ret;
		}
		return p1len - p2len;
	}

}
