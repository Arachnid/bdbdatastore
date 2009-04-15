package net.notdot.bdbdatastore.server;

import java.util.Comparator;

public class FilterTypeComparator implements Comparator<FilterSpec> {
	public final static FilterTypeComparator instance = new FilterTypeComparator();
	
	@Override
	public int compare(FilterSpec o1, FilterSpec o2) {
		int ret = o2.operator - o1.operator; // Reversed so equality comes first
		if(ret != 0)
			return ret;
		ret = o1.getName().asReadOnlyByteBuffer().compareTo(o2.getName().asReadOnlyByteBuffer());
		if(ret != 0)
			return ret;
		return PropertyValueComparator.instance.compare(o1.getValue(), o2.getValue());
	}

}
