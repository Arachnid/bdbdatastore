package net.notdot.protorpc;

import com.google.protobuf.Service;

public abstract class ServiceFactory {
	public abstract Service getService();
}
