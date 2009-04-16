package net.notdot.bdbdatastore.server;

import java.util.Comparator;

import net.notdot.bdbdatastore.Indexing;
import net.notdot.bdbdatastore.Indexing.PropertyIndexKey;

public class PropertyIndexKeyComparator implements
		Comparator<Indexing.PropertyIndexKey> {
	public static final PropertyIndexKeyComparator instance = new PropertyIndexKeyComparator();

	public int compare(PropertyIndexKey arg0, PropertyIndexKey arg1) {
		int ret = arg0.getKind().asReadOnlyByteBuffer().compareTo(arg1.getKind().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		ret = arg0.getName().asReadOnlyByteBuffer().compareTo(arg1.getName().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		if(!arg0.hasValue())
			return -1;
		if(!arg1.hasValue())
			return 1;
		return PropertyValueComparator.instance.compare(arg0.getValue(), arg1.getValue());
	}
	
}
