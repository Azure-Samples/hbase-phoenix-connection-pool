# Instructions on Running Test
## Copy Required Jars
copy the phoenix-<version>-client.jar and phoenix-<version>-thin-client.jar into 'jars'
## Hosts File
Sample hosts file are under parent directory.
## Queries File
Put all query sql statements in a txt file, DO NOT leave space between queries.<br>
Sample queries file are under parent directory.
## Execution
### Running the Java program only
Command: java -classpath "jars/\*:." com.microsoft.azure.sample.phoenix.QueryMain \<hosts file\> \<queries file\> \<concurrency\> \<test mode\> \<optional: round robin\><br>
concurrency: int, size of the thread pool<br>
test mode: int, 1 - Sequential mode; 2 - ThreadPool mode; 3 - Connection Pool mode; others - run all 3 modes, split the queries into 3 groups<br>
round robin: boolean, true - all Phoenix query servers will be used, following round-robin assignment strategy; false, using only one phoenix query server<br>
Example: java -classpath "jars/\*:." com.microsoft.azure.sample.phoenix.QueryMain “hosts.txt” “queries.txt” 16 3 true<br>
### Running the full test
Command: sh run_test.sh \<cluster_name\> \<ambary username\> \<ambary password\> \<hosts file\> \<queries file\><br>
It is recommended to double quote every argument.<br>
Example: sh run_test.sh “my_cluster” “myusername” “mypassword” “hosts.txt” “queries.txt”
