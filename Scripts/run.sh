python genIds6.py 0 2 1800000 10000
python makerun.py 0 2 run2.sh log10000-10000-208.out
chmod u+x run2.sh
./run2.sh
python genIds6.py 2 4 1800000 10000
python makerun.py 2 4 run3.sh log10000-10000-208.out
chmod u+x run3.sh
./run3.sh
