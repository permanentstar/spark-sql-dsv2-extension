# spark-sql-dsv2-extension
A sql extension build on spark3 datasource v2 api, ex: hive v2 catalog support amoung multi clusters.  
If this repository makes you any benefits on spark dsv2 integration, feel free to star it and recommend to your workmates, hoo ~ daas yyds !!! ^_^

## HIVE-CATALOG
This is a dsv2 integration on hive building on top of spark isolated client. It is very common that we has multi hive clusters
when trying to migrate partial dw jobs to new hive version or we have to work coordinated to other department, etc.  
First of all, if your hive clusters spread among multiple hdfs clusters, I assume that a viewfs/rbf federation has been established globally.
### ARCHITECTURE
Main component as follows:
- Catalog Entrypoint  
In order to implement TableCatalog for hive, I elevate V2ExternalCatalog to manage a connected hive source(HMS). In initialization,
user defined properties will be recognized as to build a separate HiveExternalCatalog, here we reuse the create procedure of IsolatedClientLoader,
so user can define a new hive catalog by setting properties like `spark.sql.catalog.{catalog_name}.spark.sql.hive.metastore.version` to specify a certain
hive version, as same as other standard sessionCatalog initialing properties.  
- V2 Table Entrypoint  
The V2Table inherits from V1Table and in charge of establishing connector channel to special hive catalog, like read/write/partition-load, etc.
- Data exchange channel  
Reuse HiveFileFormat as write channel and elevate HiveFileScan as read channel.
- V2 catalog file indexer  
A cache based HMS file indexer was developed to fast list filtered files.
- V2 sink committer  
Combine File committer and HMS committer as a whole.

![spark-ds-v2-arch-open_source](https://user-images.githubusercontent.com/10155248/167095990-0cae22b6-8736-4f64-a1f7-0321680b317c.png)
### HOW TO USE
A serials additional properties should be appended into spark-defaults.conf or --conf args in spark-submit.  
For framework lib support, add com.yyds.daas:hive-catalog:3.1.2 to spark.jars or --jars.  
For each custom hive catalog,  
i) A mandatory hive-site location should always explicitly point out by `spark.sql.catalog.{catalog_name}.hive.conf`,
currently `classpath` and `path` manners(the latter is default) are supported.  
ii) User can specify actual path by `spark.sql.catalog.{catalog_name}.hive.conf.path`
or actual classpath by `spark.sql.catalog.{catalog_name}.hive.conf.classpath`.  
iii) A serials hive client connector properties should be provided by starting with prefix `spark.sql.catalog.{catalog_name}.`.  
#### example for spark-defaults.conf
```
spark.sql.hive.metastore.version        2.3.6
spark.sql.hive.metastore.jars           path
spark.sql.hive.metastore.jars.path      file:///opt/hive2/lib/*.jar
spark.sql.catalogImplementation         hive

spark.jars                              file:///opt/spark/ext-lib/hive-catalog-3.1.2.jar

# for catalog hive1_external
spark.sql.catalog.hive1_external                                        com.daasyyds.spark.sql.dsv2.hive.V2ExternalCatalog
spark.sql.catalog.hive1_external.spark.sql.hive.metastore.version       1.1.0
spark.sql.catalog.hive1_external.spark.sql.hive.metastore.jars          path
spark.sql.catalog.hive1_external.spark.sql.hive.metastore.jars.path     file:///opt/hive1/lib/*.jar
spark.sql.catalog.hive1_external.spark.sql.hive.conf                    path
spark.sql.catalog.hive1_external.spark.sql.hive.conf.path               /etc/hive/conf.hive1_external
```