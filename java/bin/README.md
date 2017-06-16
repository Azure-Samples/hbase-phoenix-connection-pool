# Instructions on Running Test
## Copy Required Jars
copy the phoenix-<version>-client.jar and phoenix-<version>-thin-client.jar into 'jars'
## Hosts File
Sample hosts file are under parent directory.
## Queries File
Put all query sql statements in a txt file, DO NOT leave space between queries.<br>
Sample queries file are under parent directory.
## Execution
Command: sh run_test.sh <cluster_name> <ambary username> <ambary password> <hosts file> <queries file><br>
It is recommended to double quote every argument.<br>
Example: sh run_test.sh “my_cluster” “myusername” “mypassword” “hosts.txt” “queries.txt”
