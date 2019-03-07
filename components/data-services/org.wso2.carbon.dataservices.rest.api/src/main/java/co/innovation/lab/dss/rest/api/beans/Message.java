package co.innovation.lab.dss.rest.api.beans;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Message")
public class Message {
 
    private String message;
 
    public String getMessage() {
     return message;
    }
 
    public void setMessage(String message) {
     this.message = message;
    }
     
}
