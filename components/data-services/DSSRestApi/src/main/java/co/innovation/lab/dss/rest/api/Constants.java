package co.innovation.lab.dss.rest.api;

public final class Constants {

    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String SUCCESS = "SUCCESS";
    public static final String INVALID = "INVALID";
    public static final String FORBIDDEN = "FORBIDDEN";
    public static final String FAILED = "FAILED";


    private Constants() {
    }

    public static final class Status {

        public static final String FAILED = "failed";
        public static final String SUCCESS = "success";
        public static final String NON_EXISTENT = "non existent";

        private Status() {
        }
    }

}