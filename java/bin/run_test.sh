#!/bin/bash

function restart_all_pqs() {
  cluster_name=$1
  ambari_uname=$2
  ambari_pwd=$3
  for h in $(cat "$4");
  do
    echo $h
    curl -s -S -k -u $ambari_uname:$ambari_pwd -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop PHOENIX_QUERY_SERVER via REST"}, "Body": {"HostRoles": {"state": "INSTALLED"}}}' "https://${cluster_name}.azurehdinsight.net/api/v1/clusters/${cluster_name}/hosts/${h}/host_components/PHOENIX_QUERY_SERVER" > /dev/null
    curl -s -S -k -u $ambari_uname:$ambari_pwd -i -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop PHOENIX_QUERY_SERVER via REST"}, "Body": {"HostRoles": {"state": "STARTED"}}}' "https://${cluster_name}.azurehdinsight.net/api/v1/clusters/${cluster_name}/hosts/${h}/host_components/PHOENIX_QUERY_SERVER" > /dev/null
  done
}

echo $#

if [ $# -lt 5 ]; then
  echo "Usage: ./run_test.sh <cluster_name> <ambari username> <password> <hosts> <queries>"
  exit
fi
arg_cname=$1
arg_ambari_uname=$2
arg_ambari_pwd=$3
arg_hosts=$4
queries=$5

echo "Test:" > results.txt
echo "Use Single Query Server:" >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_seq=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 1 false`
echo $ret_seq >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_tp=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 2 false`
echo "$ret_tp" >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_cp=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 3 false`
echo "$ret_cp" >> results.txt

echo "Use Multiple Query Servers:" >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_seq=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 1 false true`
echo $ret_seq >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_tp=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 2 false true`
echo "$ret_tp" >> results.txt
restart_all_pqs $arg_cname $arg_ambari_uname $arg_ambari_pwd $arg_hosts
sleep 10
ret_cp=`java -classpath "jars/*:." com.microsoft.azure.sample.phoenix.QueryMain $arg_hosts $queries 16 3 false true`
echo "$ret_cp" >> results.txt
