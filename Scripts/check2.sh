cd system0
sleep 20
nohup java -jar CassPerf.jar 1 log10000-10000-208.out > lcheck10000-10000-208.out &
cd ..
cd system1
sleep 20
nohup java -jar CassPerf.jar 1 log10000-10000-208.out > lcheck10000-10000-208.out &
cd ..
cd system2
sleep 20
nohup java -jar CassPerf.jar 1 log10000-10000-208.out > lcheck10000-10000-208.out &
cd ..
cd system3
sleep 20
nohup java -jar CassPerf.jar 1 log10000-10000-208.out > lcheck10000-10000-208.out &
cd ..
