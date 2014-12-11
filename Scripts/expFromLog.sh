server="m8"
folder="smaifi8"
cassandraFolder="/home/nq/setup/sparkcass/apache-cassandra-2.0.7"
for count in 3 4 
do 
for sp1 in 16000 17000 18000 19000 20000 21000 22000 
 do 
  for sp2 in 16000 17000 18000 19000 20000 21000 22000 #26000 27000 28000 29000 30000
   do
    if [ "$sp1" -le "$sp2" ] 
    then
     echo $sp1-$sp2
     python creatrun.py $sp1 $sp2
      echo "start sleeping 5s"
      sleep 5
      ssh $server "sed -i 's/metrics.out/metrics$sp1-$sp2-$count.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"
      echo "end sleeping 5s - call command to stop Cassandra"  
      ssh $server "cd $cassandraFolder; ./halt.sh"     
      echo "start sleeping 30s"
      sleep 30
      echo "end sleeping 30s - call command to start Cassandra"
      ssh $server "cd $cassandraFolder; ./start.sh"
      echo "start sleeping 210s"
      sleep 210
      echo "end sleeping 210s - call command to clear DB"
      ssh $server "cd $cassandraFolder; ./clearDB2.sh"
      echo "start sleeping 210s"
      sleep 210
      echo "end sleeping 210s - call command to put data from clients"
      scp run.sh "m4:cassandra/perf/"$folder
      scp run.sh "m5:cassandra/perf/"$folder
      scp run.sh "m6:cassandra/perf/"$folder
      scp run.sh "m2:cassandra/perf/"$folder
      scp check.sh "m4:cassandra/perf/"$folder      
      scp check.sh "m5:cassandra/perf/"$folder      
      scp check.sh "m6:cassandra/perf/"$folder
      scp check.sh "m2:cassandra/perf/"$folder
      ./run.sh
#      echo "sleep 3"
 #     sleep 3
  #    echo "end sleep 3"
      command="cd /home/nq/cassandra/perf/$folder; ./run.sh"
      for host in $(cat hosts.txt)
       do 
	ssh "$host" "$command"
#	sleep 3
       done
      ssh $server "cd $cassandraFolder; ./runiotop.sh"
      echo "start sleeping 150s"
      sleep 150
      echo "end sleeping 150s - call command to get the average of disk statistics"
      echo $count-$1: >> result/$sp1-$sp2.out
      ssh $server "cd $cassandraFolder; ./avgDiskStat.sh" >> result/$sp1-$sp2.out 
      echo "start sleeping 180s"
      sleep 180
      echo "end sleeping 180s - call command to stop Cassandra"
      ssh $server "cd $cassandraFolder; ./killCass.sh" 
      ssh $server "cp /var/log/cassandra/system.log $cassandraFolder/runLogs/"$sp1-$sp2-$count".log"
      echo "m3" >> result/$sp1-$sp2.out
      python checkstatistics.py 0 4 4000 log2.out >> result/$sp1-$sp2.out
      for host in $(cat hosts.txt);
       do
	 #echo $sp1-$sp2: >> result/result$host.txt 
        echo $host >> result/$sp1-$sp2.out
        ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; python checkstatistics.py 0 4 4000 log2.out" >> result/$sp1-$sp2.out
       done
      echo "=====" >> result/$sp1-$sp2.out
      ssh $server "sed -i 's/metrics$sp1-$sp2-$count.out/metrics.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"
      fi
     done    
   done
 done
