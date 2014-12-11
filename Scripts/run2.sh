cd system0
cp gendata6.properties gendata.properties
nohup java -jar CassPerf.jar 0 0 > log10000-10000-208.out &
cd ..
cd system1
cp gendata6.properties gendata.properties
nohup java -jar CassPerf.jar 0 0 > log10000-10000-208.out &
cd ..
