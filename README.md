# carbon-data

---

|  Branch | Build Status |
| :------------ |:-------------
| master      | [![Build Status](https://wso2.org/jenkins/job/platform-builds/job/carbon-data/badge/icon)](https://wso2.org/jenkins/job/platform-builds/job/carbon-data/) |


---


Latest Released Version v4.3.5.

#### carbon-data repo contains the following components.

* data-services      
* data-sources
* dbconsole  
    
carbon-data contains the source code of modules uses for Data Services Server. Some of these modules uses by other products of the platform too.

Service Stubs, Components and Features of each module contains in this repo.

## SmartCommunity - DigitalHub improvements

* OData - Implement ExpressionVisitor Olingo interface in order to elaborate SQL queries.
* Performance tunings regarding OData requests in order to limit the amount of records being retrieved.
* Extend OData configurations related to finlering tables to be exposed over odata.
* Extend OData configurations related to filtering columns to be exposed over odata and possibility to change their type.
* Bug fixes regarding MSSQL query formatting including schema parameter.
* OData now supported for Cassandra databases
* Related entities can now be queried with OData in MySQL, PostgreSQL, H2, Oracle, MicroSoft SQL Server databases.


## How to Contribute
* Please report issues at [DSS JIRA] (https://wso2.org/jira/browse/DS).
* Send your bug fixes pull requests to [master branch] (https://github.com/wso2/carbon-data/tree/master) 

## Contact us
WSO2 Carbon developers can be contacted via the mailing lists:

* Carbon Developers List : dev@wso2.org
* Carbon Architecture List : architecture@wso2.org
