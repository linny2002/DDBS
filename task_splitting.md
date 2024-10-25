### 1 Initialize

1. Create two mongo database sites in Docker. For replica consideration, there need to be another two backup sites.
2. Fragment the User table, Article table and Read table and upload them to the mongo database. 
3. Create the Be-Read table and Popular-Rank table in DB.



### 2 Demo Function

(How to manage?)

Query users, articles, users’ read tables (involving the join of User table and Article table) with and without query conditions.

1. Search Page

   * Search box

2. User Info Page

   * Basic info

   * Reading records

3. Article Info Page

   * Article info

4. Rank Page

   * Filtering box

#### API

1. /search
2. /user
3. /article
4. /rank



### 3 Monitoring

Monitoring the running status of DBMS servers, including its managed data (amount and location), workload, etc. 

**Mongo Compass**



### 4 (Optional) advanced functions

a) Hot / Cold Standby DBMSs for fault tolerance

b) Expansion at the DBMS-level allowing a new DBMS server to join

c) Dropping a DBMS server at will 

d) Data migration from one data center to others