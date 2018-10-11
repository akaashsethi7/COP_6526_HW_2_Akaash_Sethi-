# COP_6526_HW_2_Akaash_Sethi-
Python code for Homework 2 


In this module we learned to setup a spark cluster and create a Spark job which does word counts with permutations to combine like wise pars and provide the number of times it occurs by sentence. 


The code is based off of the original wordcount.py found in the Spark examples folder. The code was modified for the problem case of this class. The Licscnese is still intact at the top of the file. 


### Running the Code 

To run the code follow the below instructions: 

1. Logon onto a Spark Cluster. 

`Example: ssh -i "hadoop-cluster-akaash.pem" root@ec2-35-172-133-79.compute-1.amazonaws.com`

2. If using git then clone this repo else use SCP to copy over the file from local to onto the server: 

`Example: git clone https://github.com/akaashsethi7/COP_6526_HW_2_Akaash_Sethi-.git` 

3. Make sure Spark and Hadoop is up and running by doing a JPS. If not simpley start both: 

`Example: from the Hadoop home directory run ./sbin/start-all.sh` 

`Example: from the Spark home directory run ./sbin/start-all.sh` 

4.Also copy over any file you wish to test into the hdfs /input/ directory: 

`Example: hadoop fs -copyFromLocal  /home/ec2-user/input.txt /input/` 

5. Simply head to the spark home directory and run a spark submit command (note keep in mind where you loaded the python script as you will need to point to it) 

`Example: ./bin/spark-submit --master spark://ip-172-31-89-71.ec2.internal:7077 /home/ec2-user/wordcount.py /input/input.txt` 

6. You should see the output printed in the terminal window. 





