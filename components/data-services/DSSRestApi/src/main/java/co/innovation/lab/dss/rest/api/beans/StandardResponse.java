package co.innovation.lab.dss.rest.api.beans;

import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.wso2.carbon.dataservices.ui.beans.Data;

@XmlType(
        propOrder = {"status", "message"}
)
@XmlRootElement(
        name = "response"
)
public class StandardResponse {
    private String status;
    private String message;
    private Data data;

    public StandardResponse() {
    }

    public StandardResponse(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public StandardResponse(String status) {
        this.status = status;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return this.message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
    
    public Data getData() {
        return this.data;
    }

    public void setData(Data data) {
        this.data = data;
    }
    
}