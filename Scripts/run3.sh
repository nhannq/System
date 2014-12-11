cd system2
cp gendata6.properties gendata.properties
nohup java -jar CassPerf.jar 0 0 > log10000-10000-208.out &
cd ..
cd system3
cp gendata6.properties gendata.properties
nohup java -jar CassPerf.jar 0 0 > log10000-10000-208.out &
cd ..
