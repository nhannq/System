server="maifi7"
folder="smaifi7"
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
      ssh $server "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./halt.sh"     
      echo "start sleeping 5s"
      sleep 5
      echo "end sleeping 5s - call command to start Cassandra"
      ssh $server "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./start.sh"
      echo "start sleeping 15s"
      sleep 15
      echo "end sleeping 15s - call command to clear DB"
      ssh $server "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./clearDB2.sh"
      echo "start sleeping 15s"
      sleep 15
      echo "end sleeping 15s - call command to put data from clients"
      scp run.sh "maifi4:cassandra/perf/"$folder
      scp run.sh "maifi5:cassandra/perf/"$folder
      scp run.sh "maifi6:cassandra/perf/"$folder
      scp run.sh "maifi2:cassandra/perf/"$folder
      scp check.sh "maifi4:cassandra/perf/"$folder
      scp check.sh "maifi5:cassandra/perf/"$folder
      scp check.sh "maifi6:cassandra/perf/"$folder
      scp check.sh "maifi2:cassandra/perf/"$folder
      ./run.sh
      command="cd /home/nqn12001/cassandra/perf/$folder; ./run.sh"
      for host in $(cat hosts.txt); do ssh "$host" "$command"; done
      ssh maifi7 "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./runiotop.sh"
      echo "start sleeping 10s"
      sleep 10
      echo "end sleeping 10s - call command to get the average of disk statistics"
      echo $count-$1: >> result/$sp1-$sp2.out
      ssh maifi7 "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./avgDiskStat.sh" >> result/$sp1-$sp2.out 
      echo "start sleeping 250s"
      sleep 250
      echo "end sleeping 250s - call command to stop Cassandra"
      ssh maifi7 "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./killCass.sh" 
      echo "start sleeping 10s"
      sleep 10
      echo "end sleeping 10s - call command to restart Cassandra"
      ssh maifi7 "cd /home/nqn12001/setup/hadcass/apache-cassandra-2.0.5; ./start.sh"
      echo "start sleeping 60s"
      sleep 60
      echo "end sleeping 60s - call command to run checking program from maifi3"
      ./check.sh
      echo "start sleeping 45s"	
      sleep 45
      echo "end sleeping 45s - call command to check dropped rate of all clients on maifi3"
      echo "maifi3" >> result/$sp1-$sp2.out
      python checkstatistics.py 0 4 4000 lcheck2.out >> result/$sp1-$sp2.out
      echo "start sleeping 10s"
      sleep 10
      for host in $(cat hosts.txt);
       do ssh "$host" "cd /home/nqn12001/cassandra/perf/"$folder"; ./check.sh"
       echo "end sleeping 10s - call command to run checking program from "$host
        #echo $sp1-$sp2: >> result/result$host.txt 
        echo $host >> result/$sp1-$sp2.out
        echo "start sleeping 45s"
        sleep 45
        echo "end sleeping 45s - call command to check dropped rate of all clients on "$host
        ssh "$host" "cd /home/nqn12001/cassandra/perf/"$folder"; python checkstatistics.py 0 4 4000 lcheck2.out" >> result/$sp1-$sp2.out
        echo "start sleeping 10s"
        sleep 10
       done
      echo "=====" >> result/$sp1-$sp2.out
      fi
     done    
   done
 done
