#Run Book

#Build
mvn clean package

#Upload to cluster
scp target/AppTrans.jar root@ted-training-1-1.vpc.cloudera.com:./

#Log into Cluster
ssh root@ted-training-1-1.vpc.cloudera.com

#Set up Kafka Topic
kafka-topics --zookeeper ted-training-1-1.vpc.cloudera.com:2181 \
--partition 2 --replication-factor 2 --create --topic app-event-input

#Running the generator
java -cp AppTrans.jar com.cloudera.sa.apptrans.generator.AppEventProducer \
ted-training-1-4.vpc.cloudera.com:9092,ted-training-1-5.vpc.cloudera.com:9092 \
app-event-input 100 0 5 async 1000 10000 20000 ./data/uber_bay_area_lat_lon.csv 1000

#Listen to Kafka with Console 
kafka-topics --zookeeper ted-training-1-1.vpc.cloudera.com:2181 --list

kafka-console-producer --broker-list \
ted-training-1-4.vpc.cloudera.com:9092,ted-training-1-5.vpc.cloudera.com:9092 \
--topic app-event-input

kafka-console-consumer \
--zookeeper ted-training-1-1.vpc.cloudera.com:2181 \
--topic app-event-input

#Set up Kudu Tables
Run these in HUE
kudu/card/createCustomerMart.sql
kudu/card/createCustomerTrans.sql

#Set up SolR Collection
solrctl instancedir --generate app-event-collection
switch out the schema.xml with the following file solr/card/schema.xml
solrctl instancedir --create app-event-collection app-event-collection
solrctl collection --create app-event-collection -s 3 -r 2 -m 3

#Set up HBase Table
hadoop jar AppTrans.jar com.cloudera.sa.apptrans.setup.hbase.CreateSaltedTable app-event f 6 6
hadoop jar AppTrans.jar com.cloudera.sa.apptrans.setup.hbase.CreateSaltedTable account-mart f 6 6

#Spark Streaming to SolR
##Run Spark to SolR
export JAVA_HOME=/opt/jdk1.8.0_91/
spark-submit --class com.cloudera.sa.apptrans.streaming.ingestion.solr.SparkStreamingAppEventToSolR \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
AppTrans.jar \
ted-training-1-4.vpc.cloudera.com:9092,ted-training-1-5.vpc.cloudera.com:9092 \
app-event-input \
tmp/checkpoint \
1 \
c \
app-event-collection \
ted-training-1-1.vpc.cloudera.com:2181/solr

##Test SolR input
Go to the Hue Dashboard Page

#Spark Streaming to Kudu
##Run Spark to Kudu
export JAVA_HOME=/opt/jdk1.8.0_91/
spark-submit --class com.cloudera.sa.apptrans.streaming.ingestion.kudu.SparkStreamingAppEventToKudu \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
AppTrans.jar \
ted-training-1-4.vpc.cloudera.com:9092,ted-training-1-5.vpc.cloudera.com:9092 \
app-event-input \
1 \
c \
ted-training-1-2.vpc.cloudera.com \
account_mart_kudu \
app_event_kudu \
tmp/checkpoint

##Test Kudu input
select * from customer_tran_kudu;

##Rest Server
com.cloudera.sa.example.card.server.kudu.KuduRestServer \
4242 /
ted-training-1-1.vpc.cloudera.com  \
customer_mart_kudu \
customer_trans_kudu

#Spark Streaming to HBase
##Run Spark to HBase
export JAVA_HOME=/opt/jdk1.8.0_91/
spark-submit --class com.cloudera.sa.apptrans.streaming.ingestion.hbase.SparkStreamingAppEventToHBase \
--master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 \
AppTrans.jar \
ted-training-1-4.vpc.cloudera.com:9092,ted-training-1-5.vpc.cloudera.com:9092 \
app-event-input \
1 \
c \
app-event \
6 \
tmp/checkpoint \
/opt/cloudera/parcels/CDH/lib/hbase/conf/

##Test Kudu input
scan 'card-trans'

##Rest Server
com.cloudera.sa.example.card.server.hbase.HBaseRestService \
4242 /
/opt/cloudera/parcels/CDH/lib/hbase/conf/  \
6 \
customer_trans_kudu





