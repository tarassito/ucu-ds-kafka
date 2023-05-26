***Prerequisite***:
 - docker must be installed

**How to run:**  
1. Run kafka cluster: `docker compose -f init-kafka-cluster.yaml up` Cluster is created 
with 3 nodes and 4 topics (1,2,5,10 partitions topics).

2. To run data through system and see report results: `docker compose -f test.yaml up`

**Configs**  
 To interact with system it is  enough to change variables in .env file.   
 By default it is configured for 1 producer, 1 consumer and 1 partition topic (1part-topic).   
 To change producer quantity update PRODUCERS_NUMBER to 2 (Only 1 and 2 values supported).  
 To change consumers quantity update CONSUMERS_NUMBER variable.  
 To change topic name update TOPIC variable (1part-topic, 2part-topic, 5part-topic, 10part-topic)
 
**Finish**  
After all experiments are done run: `docker compose -f test.yaml down` and 
`docker compose -f init-kafka-cluster.yaml down`