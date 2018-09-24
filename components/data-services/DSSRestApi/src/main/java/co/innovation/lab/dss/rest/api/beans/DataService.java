package co.innovation.lab.dss.rest.api.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.wso2.carbon.dataservices.ui.beans.Data;

@XmlAccessorType(XmlAccessType.FIELD)

@XmlRootElement(name = "saveDataService")
public class DataService {
    @XmlElement(required = true)
    private Data data;
    
    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }
}
