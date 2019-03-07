package co.innovation.lab.dss.rest.api;

public class DataServicesCoreRestException extends Exception{

	public DataServicesCoreRestException(String message) {
        super(message);
    }

    public DataServicesCoreRestException(Throwable e) {
        super(e);
    }

    public DataServicesCoreRestException(String message, Throwable e) {
        super(message, e);
    }
}
