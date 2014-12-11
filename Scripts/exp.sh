server="m7"
folder="sm7"
for count in 1 2 3 
do 
for sp1 in 10000 11000 12000 13000 14000 15000 16000 17000 18000 19000 # 19000 20000 21000 22000 23000 24000 25000 26000 27000 28000 29000 30000 
 do 
  for sp2 in 10000 11000 12000 13000 14000 15000 16000 17000 18000 19000 # 19000 20000 21000 22000 23000 24000 25000 26000 27000 28000 29000 30000
   do
    if [ "$sp1" -le "$sp2" ] 
    then
     echo $sp1-$sp2
     python creatrun.py $sp1 $sp2
      echo "start sleeping 15s"
      sleep 15
      echo "end sleeping 15s - call command to stop Cassandra"  
      ssh $server "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./halt.sh"     
      echo "start sleeping 5s"
      sleep 5
      echo "end sleeping 5s - call command to start Cassandra"
      ssh $server "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./start.sh"
      echo "start sleeping 15s"
      sleep 15
      echo "end sleeping 15s - call command to clear DB"
      ssh $server "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./clearDB2.sh"
      echo "start sleeping 15s"
      sleep 15
      echo "end sleeping 15s - call command to put data from clients"
      scp run.sh "m4:cassandra/perf/"$folder
      scp run.sh "m5:cassandra/perf/"$folder
      scp run.sh "m6:cassandra/perf/"$folder
      scp run.sh "m2:cassandra/perf/"$folder
      scp check.sh "m4:cassandra/perf/"$folder
      scp check.sh "m5:cassandra/perf/"$folder
      scp check.sh "m6:cassandra/perf/"$folder
      scp check.sh "m2:cassandra/perf/"$folder
      ./run.sh
      command="cd /home/nq/cassandra/perf/$folder; ./run.sh"
      for host in $(cat hosts.txt); do ssh "$host" "$command"; done
      ssh m7 "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./runiotop.sh"
      echo "start sleeping 10s"
      sleep 10
      echo "end sleeping 10s - call command to get the average of disk statistics"
      echo $count-$1: >> result/$sp1-$sp2.out
      ssh m7 "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./avgDiskStat.sh" >> result/$sp1-$sp2.out 
      echo "start sleeping 250s"
      sleep 250
      echo "end sleeping 250s - call command to stop Cassandra"
      ssh m7 "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./killCass.sh" 
      echo "start sleeping 10s"
      sleep 10
      echo "end sleeping 10s - call command to restart Cassandra"
      ssh m7 "cd /home/nq/setup/hadcass/apache-cassandra-2.0.5; ./start.sh"
      echo "start sleeping 60s"
      sleep 60
      echo "end sleeping 60s - call command to run checking program from m3"
      ./check.sh
      echo "start sleeping 45s"	
      sleep 45
      echo "end sleeping 45s - call command to check dropped rate of all clients on m3"
      echo "m3" >> result/$sp1-$sp2.out
      python checkstatistics.py 0 4 4000 lcheck2.out >> result/$sp1-$sp2.out
      echo "start sleeping 10s"
      sleep 10
      for host in $(cat hosts.txt);
       do ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; ./check.sh"
       echo "end sleeping 10s - call command to run checking program from "$host
        #echo $sp1-$sp2: >> result/result$host.txt 
        echo $host >> result/$sp1-$sp2.out
        echo "start sleeping 45s"
        sleep 45
        echo "end sleeping 45s - call command to check dropped rate of all clients on "$host
        ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; python checkstatistics.py 0 4 4000 lcheck2.out" >> result/$sp1-$sp2.out
        echo "start sleeping 10s"
        sleep 10
       done
      echo "=====" >> result/$sp1-$sp2.out
      fi
     done    
   done
 done
