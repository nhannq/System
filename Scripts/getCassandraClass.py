import os

directories = os.listdir(".")
f = open('CassandraClassNames.txt','w')
for dir in directories:
	if os.path.isdir(dir):
		f.write("------------" + dir + "-----------\n")
		classes = os.listdir(dir)
		for i in range(len(classes)-1):
			f.write("org.apache.cassandra" + dir + classes[i] + ", ")
		f.write(classes[len(classes)-1] + "\n")
f.close()

