import os
import sys

unusedFuncs = {}
f = open('unusedFuncs.txt','r')
for line in f:
	funcs = line.split(":")
	unusedFuncs[funcs[0].strip()] = 1
	print funcs[0].strip()
directories = os.listdir(sys.argv[1])
print "-------"
f = open('CassandraClassNames.txt','w')
for dir in directories:
	if os.path.isdir(dir):
		classes = os.listdir(dir)
		count = 0
		f.write("------------" + dir + "-----------\n")
		for i in range(len(classes)-1):
			className = "org.apache.cassandra." + dir + "." + classes[i].strip(".java")
			if not className.lower() in unusedFuncs.keys():
				f.write(className + ", ")
				count += 1
		className = "org.apache.cassandra." + dir + "." + classes[len(classes)-1].strip(".java")
		print className.lower()
		if not className.lower() in unusedFuncs.keys():
			f.write(className + "\n")
			count += 1
		f.write("===========" + dir + " : " + str(count) + "============\n")
f.close()

