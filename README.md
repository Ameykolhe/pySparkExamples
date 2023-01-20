# PySpark Examples

[![pages-build-deployment](https://github.com/Ameykolhe/pySparkExamples/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/Ameykolhe/pySparkExamples/actions/workflows/pages/pages-build-deployment)

Tutorial Link - [ameyk.me](http://ameyk.com/pySparkExamples/)  
Repository to store pyspark code

### Project Setup

1. Create .env file in project root directory
   ```text
   MYSQL_DB=<MySQL Database name>
   MYSQL_USER=<MySQL Username>
   MYSQL_PASSWORD=<MySQL Password">
   MYSQL_ROOT_PASSWORD=<MySQL root Password>
   
   MONGO_ROOT_USER=<MongoDB Root Username>
   MONGO_ROOT_PASSWORD=<MongoDB Root Password>
   
   MONGOEXPRESS_LOGIN=<MongoExpress Username>
   MONGOEXPRESS_PASSWORD=<MongoExpress Password>
   ```
2. Requires Docker & docker-compose for database services
3. Docker data directories setup  
   1. Create the following Directory Structure
      ```text
      data
        ├───elastic_search
        │   ├───1
        │   ├───2
        │   └───3
        ├───mongodb
        └───mysql
      ```
4. Download [MySQL Java connector](https://search.maven.org/artifact/mysql/mysql-connector-java/8.0.27/jar) and paste it in project root directory
