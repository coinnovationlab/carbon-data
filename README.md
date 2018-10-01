# carbon-data

Latest Released Version v4.3.5.

#### carbon-data repo contains the following components.

* data-services      
* data-sources
* dbconsole  
    
carbon-data contains the source code of modules used for Data Services Server.  <br/>
This repo contains Service Stubs, Components and Features of each module. <br/>
In order to be informed on the way these modules are going to be integrated and used please refer to the [Wiki](https://github.com/coinnovationlab/product-dss/wiki) of the [product-dss](https://github.com/coinnovationlab/product-dss).

## 1. Co-Innovation - DigitalHub improvements

* OData - Implement ExpressionVisitor Olingo interface in order to elaborate SQL queries.
* Performance tunings regarding OData requests in order to limit the amount of records being retrieved.
* Extend OData configurations related to finlering tables to be exposed over odata.
* Extend OData configurations related to filtering columns to be exposed over odata and possibility to change their type.
* Bug fixes regarding MSSQL query formatting including schema parameter.
* OData now supported for Cassandra databases
* Related entities can now be queried with OData in MySQL, PostgreSQL, H2, Oracle, MicroSoft SQL Server databases.
* REST API for creating, updating, deleting dataservices within specific tenant. 


## 2. REST API for dataservices CRUD

All WSO2 products expose their management functionality as admin services, which are SOAP services. <br/>
In this fork of carbon.data we have exposed OSGi-level functionalities as REST APIs via JAX-RS web application. <br/>

![Exposing Admin Services](https://wso2.com/files/rest-way-of-exposing-wso2-identity-server-admin-services-2.png)

In order to expose a SOAP-based admin service as a REST API there are several tasks that need to be done:

* Create DataServiceManager packages to be integrated inside carbon.data OSGi bundles.
* Create Jax-RS  WebApp in order to provide the REST API that is going to consume the OSGi service.
* Provide multitenancy data services creation through the API, extend admin services in order to switch the AxisConfiguration according to current PrivilegedCarbonContext.
* Integrate the OAUTH2 security filter in order to allow only authorized access to the REST API for dataservices CRUD.

## 3. REST API Usage

The Swagger for the dataservices API is available at: ``https://mydomain.com/dss_proxy_context_path/DSSRestApi/api/swagger.json``.  <br/>
All the list of webservices is exposed in te following endpoint: ``https://mydomain.com/dss_proxy_context_path/DSSRestApi/rest_services`` <br/>
They are protected by OAuth2 token-based authentication method, therefore the token must be provided in the Header or it can be used the apikey as a query parameter. <br/>
The API can be accesed also by using Basic Authorization Header : Basic base64Encode(admin:admin)

### 3.1. Save dataservice   
To save or update the configuration of one dataservice inside specific tenant the following call should be performed:   

    POST /{tenantDomain}/saveDataService    HTTPS/1.1 
    Accept: application/json 
    Host: https://mydomain.com/dss_proxy_context_path/DSSRestApi/rest_services
    a. Authorization: Bearer <token-value>  
    b. ApiKey query param https://wso2server.com/dss/DSSRestApi/rest_services/newdomain.com/saveDataService?apikey=APIKEY_VALUE
    
### 3.2. Get dataservice configuration   
To get the configuration of one dataservice inside specific tenant the following call should be performed:   

    GET /{tenantDomain}/getDataService?serviceid=DATASERVICENAME    HTTPS/1.1 
    Host: https://mydomain.com/dss_proxy_context_path/DSSRestApi/rest_services
    a. Authorization: Bearer <token-value>  
    b. ApiKey query param https://wso2server.com/dss/DSSRestApi/rest_services/newdomain.com/getDataService?serviceid=DATASERVICENAME&apikey=APIKEY_VALUE
    

### 3.3. Get list of dataservices   
To filter the list of dataservices inside specific tenant is important to provide the term you want to search and also provide the page number starting from 0. <br/>
The following call should be performed:   

    GET /{tenantDomain}/listDataService?search=TERM_TO_SEARCH&page=0    HTTPS/1.1 
    Host: https://mydomain.com/dss_proxy_context_path/DSSRestApi/rest_services
    Accept: application/json 
    a. Authorization: Bearer <token-value>  
    b. ApiKey query param https://wso2server.com/dss/DSSRestApi/rest_services/newdomain.com/listDataService?search=TERM_TO_SEARCH&page=0&apikey=APIKEY_VALUE
    
### 3.4. Delete dataservice   
To delete one dataservice inside specific tenant the following call should be performed:   

    DELETE /{tenantDomain}/dataService/DATASERVICENAME    HTTPS/1.1 
    Host: https://mydomain.com/dss_proxy_context_path/DSSRestApi/rest_services
    a. Authorization: Bearer <token-value>  
    b. ApiKey query param https://wso2server.com/dss/DSSRestApi/rest_services/newdomain.com/dataService/DATASERVICENAME&apikey=APIKEY_VALUE

Consider the fact that you can delete a dataservice that could be placed inside specific folder,in that case you can provide the exact value: /dataService/FOLDERNAME/DATASERVICENAME 

## 4. REST WebApp Deployment

In order to deploy the JAX-RS webapp it is important to get the .war from components/data-services/DSSRestApi/target which can be genenrated after compiling the [carbon-data](https://github.com/coinnovationlab/carbon-data) module.
Then you should put that .war in the folder repository/deployment/server/webapps.
