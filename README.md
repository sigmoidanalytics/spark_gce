Spark GCE
=========

Spark GCE is like Spark Ec2 but for those who run their cluster on Google Cloud.

  - Make Sure you have installed and authenticated gcutils where you are running this script.
  - Helps you launch a spark cluster in the Google Cloud
  - Attaches 500GB empty disk to all nodes in the cluster
  - Installs and Configures everything Automatically
  - Starts the Shark server Automatically

Spark GCE is a python script which will help you launch a spark cluster in the google cloud like the way spark_ec2 script does for AWS.

Usage
-----

> ***spark_gce.py project-name number-of-slaves slave-type master-type identity-file zone cluster-name***
>
>> 
>> - **project-id**: Project ID of the project where you are going to launch your spark cluster.
>> 
>> - **number-of-slave**: Number of slaves that you want to launch.
>>
>> - **slave-type**: Instance type for the slave machines.
>>
>> - **master-type**: Instance type for the master node.
>> 
>> - **identity-file**: Identity file to authenticate with your GCE instances, Usually resides at *~/.ssh/google_compute_engine* once you authenticate using gcutils.
>>
>> - **zone:** Specify the zone where you are going to launch the cluster.
>>
>> - **cluster-name**: Name the cluster that you are going to launch.
>>
>
> ***spark_gce.py project-name cluster-name destroy***
>
>> - **project-id:** Project id of the project where the spark cluster is at.
>> - **cluster-name:** Name of the cluster that you are going to destroy.


Installation
--------------

```sh
git clone https://github.com/sigmoidanalytics/spark_gce.git
cd spark_gce
python spark_gce.py
```


Need Help?
-------------
- Drop us an email: mayur@sigmoidanalytics.com
- Read Our Blog: http://www.sigmoidanalytics.com/spark-gce/
- Read Our Wiki: http://docs.sigmoidanalytics.com/index.php/SparkGCE
