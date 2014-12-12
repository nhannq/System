#Nhan Nguyen
#This script parses run the program to check the dropped rate per minute on the server

server="m7"
server2="m11"
server3="m12"
folder="sm7_3"
cassandraFolder="/home/nq/setup/sparkcass/apache-cassandra-2.0.7"
experimentTime=3
ssh $server "cd $cassandraFolder/conf; cp metrics-reporter-config-sample.yaml metrics-reporter-config.yaml"
ssh $server2 "cp $cassandraFolder/conf/metrics-reporter-config-sample.yaml $cassandraFolder/conf/metrics-reporter-config.yaml"
ssh $server3 "cp $cassandraFolder/conf/metrics-reporter-config-sample.yaml $cassandraFolder/conf/metrics-reporter-config.yaml"

for count in 300  #301 302 303 304 305 306 307 308 # 03 304 305 306 307 308 309 310 311 #282 283 284 285 286 287 288 289 290 #245 246
do
#for startFunction in "org.apache.cassandra.db.Column" "org.apache.cassandra.db.ColumnFamily" "org.apache.cassandra.service.CassandraDaemon" 
#do
for sp1 in 14000 16000 18000 20000 22000  #12000 #14000 16000 18000 20000 #12000 #10000 15000 #17000 18000 19000 20000 21000 22000 #9000 10000 11000 15000 16000 17000 #18000 19000 20000 #21000 22000 
 do
  for sp2 in 14000 16000 18000 20000 22000 #12000 #14000 16000 18000 20000 #10000 15000 #17000 18000 19000 20000 21000 22000 #9000 10000 11000 15000 16000 17000 #17000 18000 19000 20000 #21000 22000 #26000 27000 28000 29000 30000
   do
    if [ "$sp1" -le "$sp2" ] 
    then
     echo $sp1-$sp2
     python creatrun.py $sp1 $sp2 $count
      echo "start sleeping 30s"
      sleep 15

      ssh $server "sed -i 's/metrics.out/metrics$sp1-$sp2-$count-$server.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"

      ssh $server2 "sed -i 's/metrics.out/metrics$sp1-$sp2-$count-$server2.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"

      ssh $server3 "sed -i 's/metrics.out/metrics$sp1-$sp2-$count-$server3.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"

      echo "end sleeping 30s - call command to stop Cassandra"  

      ssh $server "cd $cassandraFolder; ./halt.sh"    

      ssh $server "sudo rm /var/log/cassandra/gc.log" 
      ssh $server2 "sudo rm /var/log/cassandra/gc.log" 
      ssh $server3 "sudo rm /var/log/cassandra/gc.log" 

      ssh $server "cp $cassandraFolder/bk/org-cassandra-env-$count.sh $cassandraFolder/conf/cassandra-env.sh" 
      ssh $server2 "cp $cassandraFolder/bk/org-cassandra-env-$count.sh $cassandraFolder/conf/cassandra-env.sh"
      ssh $server3 "cp $cassandraFolder/bk/org-cassandra-env-$count.sh $cassandraFolder/conf/cassandra-env.sh"

#      ssh $server "sed -i 's/smple/jvm-$sp1-$sp2-$count/g' /home/nq/setup/javashot/javashot.properties"

      echo "start sleeping 30s"
      sleep 15
      echo "end sleeping 30s - call command to start Cassandra"
      ssh $server "cd $cassandraFolder; ./start.sh"
      echo "start sleeping 120s"
      sleep 45
      echo "end sleeping 120s - call command to clear DB"
      ssh $server "export CQLSH_HOST=10.12.2.220; cd $cassandraFolder; ./clearDB2.sh"
      echo "start sleeping 120s"
      sleep 60
      echo "end sleeping 120s - call command to put data from clients"
      scp run.sh "m4:cassandra/perf/"$folder
      scp run.sh "m5:cassandra/perf/"$folder
      scp run.sh "m6:cassandra/perf/"$folder
      scp run.sh "m2:cassandra/perf/"$folder
      scp check.sh "m4:cassandra/perf/"$folder      
      scp check.sh "m5:cassandra/perf/"$folder      
      scp check.sh "m6:cassandra/perf/"$folder
      scp check.sh "m2:cassandra/perf/"$folder
      ./run.sh
      #echo "sleep 3"
      #sleep 3
      #echo "end sleep 3"
      command="cd /home/nq/cassandra/perf/$folder; ./run.sh"
      for host in $(cat hosts.txt)
       do 
		ssh "$host" "$command"
##		sleep 3
       done
      sleep 20
      ssh $server "cd $cassandraFolder; ./runiotop.sh"
      ssh $server2 "cd $cassandraFolder; ./runiotop.sh"
      ssh $server3 "cd $cassandraFolder; ./runiotop.sh"

      ssh $server "cd $cassandraFolder; ./runtop.sh"
      ssh $server2 "cd $cassandraFolder; ./runtop.sh"
      ssh $server3 "cd $cassandraFolder; ./runtop.sh"

      ssh $server "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l" >> result/$sp1-$sp2-$count-$server.out
      ssh $server2 "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l" >> result/$sp1-$sp2-$count-$server2.out
      ssh $server3 "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l" >> result/$sp1-$sp2-$count-$server3.out
      sleep 10
      nbThreads=$(ssh $server "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l")
      echo "NumberThreads:"$nbThreads >> result/$sp1-$sp2-$count-$server.out
      nbThreads=$(ssh $server2 "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l")
      echo "NumberThreads:"$nbThreads >> result/$sp1-$sp2-$count-$server2.out
      nbThreads=$(ssh $server3 "cd $cassandraFolder; ps -eLf | grep Cassandra | wc -l")
      echo "NumberThreads:"$nbThreads >> result/$sp1-$sp2-$count-$server3.out

      for host in $(cat hostNames.txt)
      	do 
        	getNWCommand="cd $cassandraFolder; ./getNWCommand.sh $host"
		echo $getNWCommand
 		nbTCPConnections=$(ssh "$server" "$getNWCommand")       
        	echo "nbTCPConnections"$host":"$nbTCPConnections >> result/$sp1-$sp2-$count-$server.out
                nbTCPConnections=$(ssh "$server2" "$getNWCommand")
                echo "nbTCPConnections"$host":"$nbTCPConnections >> result/$sp1-$sp2-$count-$server2.out
                nbTCPConnections=$(ssh "$server3" "$getNWCommand")
                echo "nbTCPConnections"$host":"$nbTCPConnections >> result/$sp1-$sp2-$count-$server3.out
      	done

      echo "start sleeping 210s"
      sleep 210
      echo "end sleeping 210s - call command to get the average of disk statistics"

      echo $count-$1: >> result/$sp1-$sp2-$count-$server.out
      ssh $server "cd $cassandraFolder; ./avgDiskStat.sh" >> result/$sp1-$sp2-$count-$server.out 
      ssh $server2 "cd $cassandraFolder; ./avgDiskStat.sh" >> result/$sp1-$sp2-$count-$server2.out 
      ssh $server3 "cd $cassandraFolder; ./avgDiskStat.sh" >> result/$sp1-$sp2-$count-$server3.out 


      ssh $server "cd $cassandraFolder; cp logCPURAM.out logCPURAM/logCPURAM$sp1-$sp2-$count-$server.out"
      ssh $server2 "cd $cassandraFolder; cp logCPURAM.out logCPURAM/logCPURAM$sp1-$sp2-$count-$server2.out"
      ssh $server3 "cd $cassandraFolder; cp logCPURAM.out logCPURAM/logCPURAM$sp1-$sp2-$count-$server3.out"
      
#      echo "- call command to run checking program from m3"
#      ./check.sh
#      echo "start sleeping 60s" 
#      sleep 60
#      echo "end sleeping 60s - call command to check dropped rate of all clients on m3"
#      for host in $(cat hosts.txt);
#      	do
#        	echo "start sleeping 10s"
#        	sleep 10
#        	echo "end sleeping 10s - call command to run checking program from "$host
#        	ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; ./check.sh"
#		 #echo $sp1-$sp2: >> result/result$host.txt 
#        	echo "start sleeping 90s"
#        	sleep 90
#        	echo "end sleeping 90s - call command to check dropped rate of all clients on "$host
#         done
#      echo "m3" >> result/$sp1-$sp2-$count.out
#      python checkstatistics.py 0 4 $experimentTime "lcheck"$sp1-$sp2-$count".out" 1 >> result/$sp1-$sp2-$count.out
#      for host in $(cat hosts.txt);
#      	do
#		echo $host >> result/$sp1-$sp2-$count.out
#		ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; python checkstatistics.py 0 4 $experimentTime lcheck$sp1-$sp2-$count.out 1" >> result/$sp1-$sp2-$count.out	
#	done
      echo "=====" >> result/$sp1-$sp2-$count-$server.out
      echo "start sleeping 60s"
      sleep 30
      echo "end sleeping 60s - call command to stop Cassandra"
      sleep 10
      ssh $server "cd $cassandraFolder; ./killCass.sh" 

      ssh $server "cp /var/log/cassandra/system.log $cassandraFolder/runLogs/"$sp1-$sp2-$count-$server".log"
      ssh $server "sed -i 's/metrics$sp1-$sp2-$count-$server.out/metrics.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"
     # ssh $server "sed -i 's/jvm-$sp1-$sp2-$count/smple/g' /home/nq/setup/javashot/javashot.properties"
      ssh $server "cd $cassandraFolder; cp /var/log/cassandra/gc.log gclogs/gc-"$sp1-$sp2-$count-$server".log"
  #   ssh $server "cd $cassandraFolder; cp logs/functioncalls.log FClogs/functioncalls"$sp1-$sp2-$count-$server".log; sudo rm logs/functioncalls.log"

      ssh $server2 "cp /var/log/cassandra/system.log $cassandraFolder/runLogs/"$sp1-$sp2-$count-$server2".log"
      ssh $server2 "sed -i 's/metrics$sp1-$sp2-$count-$server2.out/metrics.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"
      ssh $server2 "cd $cassandraFolder; cp /var/log/cassandra/gc.log gclogs/gc-"$sp1-$sp2-$count-$server2".log"
 #    ssh $server2 "cd $cassandraFolder; cp logs/functioncalls.log FClogs/functioncalls"$sp1-$sp2-$count-$server2".log; sudo rm logs/functioncalls.log"

      ssh $server3 "cp /var/log/cassandra/system.log $cassandraFolder/runLogs/"$sp1-$sp2-$count-$server3".log"
      ssh $server3 "sed -i 's/metrics$sp1-$sp2-$count-$server3.out/metrics.out/g' $cassandraFolder/conf/metrics-reporter-config.yaml"
      ssh $server3 "cd $cassandraFolder; cp /var/log/cassandra/gc.log gclogs/gc-"$sp1-$sp2-$count-$server3".log"
#     ssh $server3 "cd $cassandraFolder; cp logs/functioncalls.log FClogs/functioncalls"$sp1-$sp2-$count-$server3".log; sudo rm logs/functioncalls.log"
      for systemID in 0 1 2 3
        do
          mv system"$systemID"/logs/cassandra-performance.log system"$systemID"/logs/"$sp1-$sp2-$count".cassandra-performance.log
        done

      for host in $(cat hosts.txt)
       do
        for systemID in 0 1 2 3
          do
                command="cd /home/nq/cassandra/perf/$folder/system$systemID/logs; mv cassandra-performance.log $sp1-$sp2-$count.cassandra-performance.log"
                echo $command
                ssh "$host" "$command"
##               sleep 3
          done
       done
       #restart Cassandra without running the Javashot
    #  ssh $server "cp $cassandraFolder/bk/org-cassandra-env.sh $cassandraFolder/conf/cassandra-env.sh" #change back to the original configuration of env
      ssh $server "cd $cassandraFolder; ./start2.sh" #start without metrics monitoring
      echo "start sleeping 120s"
      sleep 120

      echo "- call command to run checking program from m3"
      ./check.sh
      echo "start sleeping 60s" 
      sleep 60
      echo "end sleeping 60s - call command to check dropped rate of all clients on m3"
      for host in $(cat hosts.txt);
        do
                echo "start sleeping 10s"
                sleep 10
                echo "end sleeping 10s - call command to run checking program from "$host
                ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; ./check.sh"
                 #echo $sp1-$sp2: >> result/result$host.txt 
                echo "start sleeping 90s"
                sleep 60
                echo "end sleeping 90s - call command to check dropped rate of all clients on "$host
         done
      echo "m3" >> result/$sp1-$sp2-$count-$server.out
      python checkstatistics.py 0 4 $experimentTime "lcheck"$sp1-$sp2-$count".out" 1 >> result/$sp1-$sp2-$count-$server.out
      for host in $(cat hosts.txt);
        do
                echo $host >> result/$sp1-$sp2-$count-$server.out
                ssh "$host" "cd /home/nq/cassandra/perf/"$folder"; python checkstatistics.py 0 4 $experimentTime lcheck$sp1-$sp2-$count.out 1" >> result/$sp1-$sp2-$count-$server.out
        done
     #  ssh $server "cp $cassandraFolder/bk/cassandra-env.sh $cassandraFolder/conf/cassandra-env.sh"
       ssh $server "cd $cassandraFolder; ./killCass.sh"
     fi
     done
   done
 # done
 done
                            
