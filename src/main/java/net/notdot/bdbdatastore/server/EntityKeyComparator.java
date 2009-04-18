package net.notdot.bdbdatastore.server;

import java.util.Comparator;
import java.util.List;

import net.notdot.bdbdatastore.Indexing;

import com.google.appengine.entity.Entity;
import com.google.appengine.entity.Entity.Path;
import com.google.protobuf.ByteString;

public class EntityKeyComparator implements Comparator<Indexing.EntityKey> {
	public final static EntityKeyComparator instance = new EntityKeyComparator();
	
	public static Indexing.EntityKey toEntityKey(Entity.PropertyValue.ReferenceValue refval) {
		Entity.Path.Builder path = Entity.Path.newBuilder();
		
		List<Entity.PropertyValue.ReferenceValue.PathElement> elements = refval.getPathElementList();
		for(Entity.PropertyValue.ReferenceValue.PathElement pathel : elements) {
			Entity.Path.Element.Builder element = Entity.Path.Element.newBuilder();
			element.setType(pathel.getType());
			if(pathel.hasName())
				element.setName(pathel.getName());
			if(pathel.hasId())
				element.setId(pathel.getId());
			path.addElement(element);
		}
		
		ByteString kind = elements.get(elements.size() - 1).getType();
		return Indexing.EntityKey.newBuilder().setKind(kind).setPath(path).build();
	}
	
	private static int compareElements(Entity.Path.Element e1, Entity.Path.Element e2) {
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
	
	public int compare(Indexing.EntityKey o1, Indexing.EntityKey o2) {
		Entity.Path p1 = o1.getPath();
		Entity.Path p2 = o2.getPath();
		
		// Compare kinds
		int ret = o1.getKind().asReadOnlyByteBuffer().compareTo(o2.getKind().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		
		// Compare paths
		return comparePaths(p1, p2);
	}

	public static int comparePaths(Path p1, Path p2) {
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
