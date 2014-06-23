import os
import sys

directories = os.listdir(sys.argv[1])
f = open('CassandraClassNames.txt','w')
for dir in directories:
	if os.path.isdir(dir):
		f.write("------------" + dir + "-----------\n")
		classes = os.listdir(dir)
		for i in range(len(classes)-1):
			f.write("org.apache.cassandra." + dir + "." + classes[i].strip(".java") + ", ")
		f.write("org.apache.cassandra." + dir + "." + classes[len(classes)-1].strip(".java") + "\n")
f.close()

