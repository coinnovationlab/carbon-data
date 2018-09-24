package co.innovation.lab.dss.rest.api.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.wso2.carbon.dataservices.ui.beans.Data;
import org.wso2.carbon.service.mgt.ServiceMetaData;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {
        "data",
        "tenantDomain"
})
@XmlRootElement(name = "response")
public class ServiceMetaDataResponse {
    private String status;
    private String message;
    private int pages;
    private ServiceMetaData[] metadata;
    

    public ServiceMetaDataResponse() {
    }

    public ServiceMetaDataResponse(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public ServiceMetaDataResponse(String status) {
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
    
    public ServiceMetaData[] getMetaData() {
        return this.metadata;
    }

    public void setMetaData(ServiceMetaData[] metadata) {
        this.metadata = metadata;
    }
    
    public int getPages() {
        return this.pages;
    }

    public void setPages(int pages) {
        this.pages = pages;
    }
}
