package net.notdot.protorpc;

public class RpcFailedError extends Error {
	private static final long serialVersionUID = 1966415719575133355L;
	
	protected int application_error;
	
	public RpcFailedError(String reason, int application_error) {
		super(reason);
		this.application_error = application_error;
	}
	
	public RpcFailedError(Throwable cause, int application_error) {
		super(cause);
		this.application_error = application_error;
	}

	public int getApplicationError() {
		return application_error;
	}
}
