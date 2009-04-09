package net.notdot.protorpc;

import org.apache.mina.core.session.IoSession;

import com.google.protobuf.RpcCallback;

public class RpcController implements com.google.protobuf.RpcController {
    protected IoSession session = null;
	protected String error = null;
	
	protected RpcController(IoSession s) {
		session = s;
	}
	
	public String errorText() {
		return error;
	}

	public boolean failed() {
		return error != null;
	}

	public boolean isCanceled() {
		return false;
	}

	public void notifyOnCancel(RpcCallback<Object> arg0) {
	}

	public void reset() {
		error = null;
	}

	public void setFailed(String arg0) {
		error = arg0;
	}

	public void startCancel() {
	}
}
