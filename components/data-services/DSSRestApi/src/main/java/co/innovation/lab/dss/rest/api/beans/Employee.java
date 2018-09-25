package co.innovation.lab.dss.rest.api.beans;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Employee")
public class Employee {
 
    private String id;
    private String name;
    private String designation;
    private String salary;
 
    public String getId() {
     return id;
    }
 
    public void setId(String id) {
     this.id = id;
    }
 
    public String getName() {
     return name;
    }
 
    public void setName(String name) {
     this.name = name;
    }
 
    public String getDesignation() {
     return designation;
    }
 
    public void setDesignation(String designation) {
     this.designation = designation;
    }
 
    public String getSalary() {
     return salary;
    }
 
    public void setSalary(String salary) {
     this.salary = salary;
    }
}