#!/usr/bin/env python

###
# This script sets up a Spark cluster on Google Compute Engine
# Sigmoidanalytics.com
###

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import subprocess
import sys
import tempfile
import time
import commands
import urllib2
from optparse import OptionParser
from sys import stderr
import shlex
import getpass
import threading
import json

###
# Make sure gcutil is installed and authenticated 
# Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name>
# Usage: spark_gce.py <project> <cluster-name> destroy
###

identity_file = ""
slave_no = ""
slave_type = ""
master_type = ""
zone = ""
cluster_name = ""
username = ""
project = ""


def read_args():

	global identity_file	
	global slave_no	
	global slave_type	
	global master_type
	global zone
	global cluster_name
	global username
	global project

	if len(sys.argv) == 8:
		project = sys.argv[1]
		slave_no = int(sys.argv[2])
		slave_type = sys.argv[3]
		master_type = sys.argv[4]
		identity_file = sys.argv[5]
		zone = sys.argv[6]
		cluster_name = sys.argv[7]
		username = getpass.getuser()

	elif len(sys.argv) == 4 and sys.argv[3].lower() == "destroy":

		print 'Destroying cluster ' + sys.argv[2]

		project = sys.argv[1]
		cluster_name = sys.argv[2]
		try:

			command = 'gcloud compute --project ' + project + ' instances list --format json'
			output = subprocess.check_output(command, shell=True)
			data = json.loads(output)
			master_nodes=[]
			slave_nodes=[]

			for instance in data:

				try:
					host_name = instance['name']		
					host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
					if host_name == cluster_name + '-master':
						command = 'gcloud compute instances delete ' + host_name + ' --project ' + project
						command = shlex.split(command)		
						subprocess.call(command)
					elif cluster_name + '-slave' in host_name:
						command = 'gcloud compute instances delete ' + host_name + ' --project ' + project
						command = shlex.split(command)		
						subprocess.call(command)

				except:
					pass				

		except:
			print "Failed to Delete instances"
			sys.exit(1)

		sys.exit(0)

	else:
		print '# Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name>'
		print '# Usage: spark_gce.py <project> <cluster-name> destroy'
		sys.exit(0)

		

def setup_network():

	print '[ Setting up Network & Firewall Entries ]'	

	try:
		command = 'gcloud compute --project=' + project + ' networks create "' + cluster_name + '-network" --range "10.240.0.0/16"'

		command = shlex.split(command)		
		subprocess.call(command)

		#Uncomment the above and comment the below section if you don't want to open all ports for public.
		command = 'gcloud compute firewall-rules delete internal --project '+ project
		command = 'gcloud compute firewall-rules create internal --network ' + cluster_name + '-network --allow tcp udp icmp --project '+ project
		command = shlex.split(command)		
		subprocess.call(command)
		
	except OSError:
		print "Failed to setup Network & Firewall. Exiting.."
		sys.exit(1)

			
def launch_master():

	print '[ Launching Master ]'
	command = 'gcloud compute --project "' + project + '" instances create "' + cluster_name + '-master" --zone "' + zone + '" --machine-type "' + master_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20141218" --boot-disk-type "pd-standard" --boot-disk-device-name "' + cluster_name + '-md"'

	command = shlex.split(command)		
	subprocess.call(command)


def launch_slaves():
	
	print '[ Launching Slaves ]'

	for s_id in range(1,slave_no+1):
		
		command = 'gcloud compute --project "' + project + '" instances create "' + cluster_name + '-slave' + str(s_id) + '" --zone "' + zone + '" --machine-type "' + slave_type + '" --network "' + cluster_name + '-network" --maintenance-policy "MIGRATE" --scopes "https://www.googleapis.com/auth/devstorage.read_only" --image "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20141218" --boot-disk-type "pd-standard" --boot-disk-device-name "' + cluster_name + '-s' + str(s_id) + 'd"'

		command = shlex.split(command)		
		subprocess.call(command)
		
def launch_cluster():
	
	print '[ Creating the Cluster ]'

	setup_network()	

	launch_master()

	launch_slaves()
		
def check_gcloud():
	
	myexec = "gcloud"
	print '[ Verifying gcloud ]'
	try:
		subprocess.call([myexec, 'info'])
		
	except OSError:
		print "%s executable not found. \n# Make sure gcloud is installed and authenticated\nPlease follow https://cloud.google.com/compute/docs/gcloud-compute/" % myexec
		sys.exit(1)

def get_cluster_ips():
		
	command = 'gcloud compute --project ' + project + ' instances list --format json'
	output = subprocess.check_output(command, shell=True)
	data = json.loads(output)
	master_nodes=[]
	slave_nodes=[]

	for instance in data:

		try:
			host_name = instance['name']		
			host_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
			if host_name == cluster_name + '-master':
				master_nodes.append(host_ip)
			elif cluster_name + '-slave' in host_name:
				slave_nodes.append(host_ip)

		except:
			pass
	
	# Return all the instances
	return (master_nodes, slave_nodes)

def enable_sudo(master,command):
	'''
	ssh_command(master,"echo \"import os\" > setuid.py ")
	ssh_command(master,"echo \"import sys\" >> setuid.py")
	ssh_command(master,"echo \"import commands\" >> setuid.py")
	ssh_command(master,"echo \"command=sys.argv[1]\" >> setuid.py")
	ssh_command(master,"echo \"os.setuid(os.geteuid())\" >> setuid.py")
	ssh_command(master,"echo \"print commands.getstatusoutput(\"command\")\" >> setuid.py")
	'''
	os.system("ssh -i " + identity_file + " -t -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + master + " '" + command + "'")

def ssh_thread(host,command):

	enable_sudo(host,command)
	
def install_java(master_nodes,slave_nodes):

	print '[ Installing Java and Development Tools ]'
	master = master_nodes[0]
	
	master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
	master_thread.start()
	
	#ssh_thread(master,"sudo yum install -y java-1.7.0-openjdk")
	for slave in slave_nodes:
		
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
		slave_thread.start()
		
		#ssh_thread(slave,"sudo yum install -y java-1.7.0-openjdk")
		
	slave_thread.join()
	master_thread.join()


def ssh_command(host,command):
	
	#print "ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'"
	commands.getstatusoutput("ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'" )
	
	
def deploy_keys(master_nodes,slave_nodes):

	print '[ Generating SSH Keys on Master ]'
	key_file = os.path.basename(identity_file)
	master = master_nodes[0]
	ssh_command(master,"ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa")
	ssh_command(master,"cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
	os.system("scp -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no -o 'StrictHostKeyChecking no' "+ identity_file + " " + username + "@" + master + ":")
	ssh_command(master,"chmod 600 " + key_file)
	ssh_command(master,"tar czf .ssh.tgz .ssh")
	
	ssh_command(master,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")
	ssh_command(master,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")
		
	print '[ Transfering SSH keys to slaves ]'
	for slave in slave_nodes:
		print commands.getstatusoutput("ssh -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no " + username + "@" + master + " 'scp -i " + key_file + " -oStrictHostKeyChecking=no .ssh.tgz " + username +"@" + slave  + ":'")
		ssh_command(slave,"tar xzf .ssh.tgz")
		ssh_command(master,"ssh-keyscan -H " + slave + " >> ~/.ssh/known_hosts")
		ssh_command(slave,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")
		ssh_command(slave,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")



def attach_drive(master_nodes,slave_nodes):

	print '[ Adding new 500GB drive on Master ]'
	master = master_nodes[0]
	
	command='gcloud compute --project="' + project + '" disks create "' + cluster_name + '-m-disk" --size 500GB --type "pd-standard" --zone ' + zone

	command = shlex.split(command)		
	subprocess.call(command)
	
	command = 'gcloud compute --project="' + project + '" instances attach-disk ' + cluster_name + '-master --device-name "' + cluster_name + '-m-disk" --disk ' + cluster_name + '-m-disk --zone ' + zone
	command = shlex.split(command)		
	subprocess.call(command)

	master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo mkfs.ext3 /dev/disk/by-id/google-"+ cluster_name + "-m-disk " + " -F < /dev/null"))
	master_thread.start()

	print '[ Adding new 500GB drive on Slaves ]'

	i = 1
	for slave in slave_nodes:

		master = slave
		
		command='gcloud compute --project="' + project + '" disks create "' + cluster_name + '-s' + str(i) + '-disk" --size 500GB --type "pd-standard" --zone ' + zone

		command = shlex.split(command)		
		subprocess.call(command)
			
		command = 'gcloud compute --project="' + project + '" instances attach-disk ' + cluster_name + '-slave' +  str(i) + ' --disk ' + cluster_name + '-s' + str(i) + '-disk --device-name "' + cluster_name + '-s' + str(i) + '-disk" --zone ' + zone
	
		command = shlex.split(command)		
		subprocess.call(command)
		slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo mkfs.ext3 /dev/disk/by-id/google-" + cluster_name + "-s" + str(i) + "-disk -F < /dev/null"))
		slave_thread.start()
		i=i+1

	slave_thread.join()
	master_thread.join()

	print '[ Mounting new Volume ]'
	enable_sudo(master_nodes[0],"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-disk /mnt")
	enable_sudo(master_nodes[0],"sudo chown " + username + ":" + username + " /mnt")
	i=1
	for slave in slave_nodes:			
		enable_sudo(slave,"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(i) +"-disk /mnt")
		enable_sudo(slave,"sudo chown " + username + ":" + username + " /mnt")
		i=i+1

	print '[ All volumns mounted, will be available at /mnt ]'

def setup_spark(master_nodes,slave_nodes):

	print '[ Downloading Binaries ]'
	
	master = master_nodes[0]
	
	ssh_command(master,"rm -fr sigmoid")
	ssh_command(master,"mkdir sigmoid")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/1.2.0/spark-1.2.0-bin-cdh4.tgz")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/scala.tgz")
	ssh_command(master,"cd sigmoid;tar zxf spark-1.2.0-bin-cdh4.tgz;rm spark-1.2.0-bin-cdh4.tgz")
	ssh_command(master,"cd sigmoid;tar zxf scala.tgz;rm scala.tgz")
	

	print '[ Updating Spark Configurations ]'
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;cp spark-env.sh.template spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SCALA_HOME=\"/home/`whoami`/sigmoid/scala\"' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_MEM=2454m' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.local.dir=/mnt/spark \\\"\" >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_JAVA_OPTS' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export SPARK_MASTER_IP=PUT_MASTER_IP_HERE' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export MASTER=spark://PUT_MASTER_IP_HERE:7077' >> spark-env.sh")
	ssh_command(master,"cd sigmoid;cd spark-1.2.0-bin-cdh4/conf;echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64' >> spark-env.sh")
	

	for slave in slave_nodes:
		ssh_command(master,"echo " + slave + " >> sigmoid/spark-1.2.0-bin-cdh4/conf/slaves")

	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/spark-1.2.0-bin-cdh4/conf/spark-env.sh")
	
	ssh_command(master,"chmod +x sigmoid/spark-1.2.0-bin-cdh4/conf/spark-env.sh")

	print '[ Rsyncing Spark to all slaves ]'

	#Change permissions
	enable_sudo(master,"sudo chown " + username + ":" + username + " /mnt")
	i=1
	for slave in slave_nodes:			
		enable_sudo(slave,"sudo chown " + username + ":" + username + " /mnt")


	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")
		ssh_command(slave,"mkdir /mnt/spark")

	ssh_command(master,"mkdir /mnt/spark")
	print '[ Starting Spark Cluster ]'
	ssh_command(master,"sigmoid/spark-1.2.0-bin-cdh4/sbin/start-all.sh")
	

	#setup_shark(master_nodes,slave_nodes)
	
	setup_hadoop(master_nodes,slave_nodes)


	print "\n\nSpark Master Started, WebUI available at : http://" + master + ":8080"

def setup_hadoop(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading hadoop ]'
	
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/sigmoidanalytics-builds/hadoop/hadoop-2.0.0-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;tar zxf hadoop-2.0.0-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;rm hadoop-2.0.0-cdh4.2.0.tar.gz")

	print '[ Configuring Hadoop ]'
	
	#Configure .bashrc
	ssh_command(master,"echo '#HADOOP_CONFS' >> .bashrc")
	ssh_command(master,"echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_INSTALL=/home/`whoami`/sigmoid/hadoop-2.0.0-cdh4.2.0' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/bin' >> .bashrc")
	ssh_command(master,"echo 'export PATH=$PATH:\$HADOOP_INSTALL/sbin' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_MAPRED_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_COMMON_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export HADOOP_HDFS_HOME=\$HADOOP_INSTALL' >> .bashrc")
	ssh_command(master,"echo 'export YARN_HOME=\$HADOOP_INSTALL' >> .bashrc")

	#Remove *-site.xmls
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/core-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/yarn-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0;rm etc/hadoop/hdfs-site.xml")
	#Download Our Confs
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/core-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/hdfs-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/mapred-site.xml")
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/yarn-site.xml")

	#Config Core-site
	ssh_command(master,"sed -i \"s/PUT-MASTER-IP/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/core-site.xml")
	
	#Create data/node dirs
	ssh_command(master,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
	#Config slaves
	ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;rm slaves")
	for slave in slave_nodes:
		ssh_command(master,"cd sigmoid/hadoop-2.0.0-cdh4.2.0/etc/hadoop/;echo " + slave + " >> slaves")

	print '[ Rsyncing with Slaves ]'
	#Rsync everything
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")
		ssh_command(slave,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
		ssh_command(master,"rsync -za /home/" + username + "/.bashrc " + slave + ":")

	print '[ Formating namenode ]'
	#Format namenode
	ssh_command(master,"sigmoid/hadoop-2.0.0-cdh4.2.0/bin/hdfs namenode -format")
	
	print '[ Starting DFS ]'
	#Start dfs
	ssh_command(master,"sigmoid/hadoop-2.0.0-cdh4.2.0/sbin/start-dfs.sh")

def setup_shark(master_nodes,slave_nodes):

	master = master_nodes[0]
	print '[ Downloading Shark binaries ]'
	
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/spark-ui/hive-0.11.0-bin.tgz")
	ssh_command(master,"cd sigmoid;wget https://s3.amazonaws.com/spark-ui/shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	ssh_command(master,"cd sigmoid;tar zxf hive-0.11.0-bin.tgz")
	ssh_command(master,"cd sigmoid;tar zxf shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	ssh_command(master,"rm sigmoid/hive-0.11.0-bin.tgz")
	ssh_command(master,"rm sigmoid/shark-0.9-hadoop-2.0.0-mr1-cdh4.2.0.tar.gz")
	
	print '[ Configuring Shark ]'
	ssh_command(master,"cd sigmoid/shark/;echo \"export SHARK_MASTER_MEM=1g\" > conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.kryoserializer.buffer.mb=10 \\\"\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_JAVA_OPTS\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export HIVE_HOME=/home/`whoami`/sigmoid/hive-0.11.0-bin\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_JAVA_OPTS\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export MASTER=spark://PUT_MASTER_IP_HERE:7077\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export SPARK_HOME=/home/`whoami`/sigmoid/spark-0.9.1-bin-cdh4\" >> conf/shark-env.sh")
	ssh_command(master,"mkdir /mnt/tachyon")
	ssh_command(master,"cd sigmoid/shark/;echo \"export TACHYON_MASTER=PUT_MASTER_IP_HERE:19998\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"export TACHYON_WAREHOUSE_PATH=/mnt/tachyon\" >> conf/shark-env.sh")
	ssh_command(master,"cd sigmoid/shark/;echo \"source /home/`whoami`/sigmoid/spark-0.9.1-bin-cdh4/conf/spark-env.sh\" >> conf/shark-env.sh")	
	ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" sigmoid/shark/conf/shark-env.sh")

	ssh_command(master,"chmod +x sigmoid/shark/conf/shark-env.sh")
	
	print '[ Rsyncing Shark on slaves ]'
	for slave in slave_nodes:
		ssh_command(master,"rsync -za /home/" + username + "/sigmoid " + slave + ":")

	print '[ Starting Shark Server ]'
	ssh_command(master,"cd sigmoid/shark/;./bin/shark --service sharkserver 10000 > log.txt 2>&1 &")

def show_banner():

	os.system("wget -qO- https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/banner")
	
def real_main():

	show_banner()
	print "[ Script Started ]"	
	#Read the arguments
	read_args()
	#Make sure gcloud is accessible.
	check_gcloud()

	#Launch the cluster
	launch_cluster()

	#Wait some time for machines to bootup
	print '[ Waiting 120 Seconds for Machines to start up ]'
	time.sleep(120)

	#Get Master/Slave IP Addresses
	(master_nodes, slave_nodes) = get_cluster_ips()

	#Install Java and build-essential
	install_java(master_nodes,slave_nodes)

	#Generate SSH keys and deploy
	deploy_keys(master_nodes,slave_nodes)

	#Attach a new empty drive and format it
	attach_drive(master_nodes,slave_nodes)

	#Set up Spark/Shark/Hadoop
	setup_spark(master_nodes,slave_nodes)

	


def main():
  try:
    real_main()
  except Exception as e:
    print >> stderr, "\nError:\n", e
    

if __name__ == "__main__":
  
  main()
