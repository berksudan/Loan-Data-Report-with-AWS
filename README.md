# Loan Data Reporting with AWS — A Case Study

## Architecture Design Scheme
![Architecture Design Scheme](docs/architecture_design_scheme.png)

## Prerequisites
+ Make sure that a vanilla and default S3 bucket was created, for this case study a S3 bucket named ``loan-data-bucket-aws`` was created.
+ Make sure that you are using the right region, for this case study the region named ``us-east-1`` is used. Note that, if you use a different region, accordingly, you ought to change the region name in the code and links stated in this case study.

## Module 1: Kinesis Firehose Client

### Used Modules
+ Python - 3.6
+ boto3 (AWS S3 SDK for Python) -> for creating s3client and writing data to s3
+ wget -> for downloading csv data initially

### How to Run
+ Get Credentials: ***aws_access_key_id***, ***aws_secret_access_key***, ***aws_session_token***

+ Build Project.
	-  Go to folder: ``Loan-Data-Report-with-AWS/src/main/python/AWS-Kinesis-Firehose-Client/``.
	-  Run command using terminal:
		```bash
		./build.sh
		```
	-  Run command using terminal (NOTE: Make sure you used quote around parameters!):
		```bash
		venv/bin/python3 firehose_client.py "aws_access_key_id" "aws_secret_access_key" "aws_session_token"
		```

## Module 2: Fire URIs Retriever + Apache Spark Application

### Used Modules
+ Java 8 + Scala 2.11
+ Spark 2.10
+ Amazon S3 SDK for Java
+ Java 8

### How to Run
+ Get Credentials: ***aws_access_key_id***, ***aws_secret_access_key***, ***aws_session_token***

+ Create Cluster.
    -  Click: "AWS Console"
    -  Head to: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1
    -  Click: "Create Cluster"
    -  Cluster Configurations:
	```
	[Logging]: Enabled
	[Logging - S3 Folder]: s3://loan-data-bucket-aws/logs/
	[Launch mode]: Step execution
	[Step type]: Spark Application
	[Step type-Configure]:
		- [Spark-submit options]: --class loanprocessor.LoanProcessor
		- [Application location]:
		s3://loan-data-bucket-aws/Loan-Data-Report-with-AWS-1.0-SNAPSHOT-jar-with-dependencies.jar
		- [Arguments] (NOTE: Make sure you used quote around parameters!):
			  "aws_access_key_id"
			  "aws_secret_access_key"
			  "aws_session_token"
	[Software configuration]: emr-5.31.0
	[Hardware configuration]: m4.large, 3 instances
	[Security and access]: DEFAULT
	```

+ Check S3 Bucket.
	-  Wait until cluster is created.
	-  Head to: https://s3.console.aws.amazon.com/s3/buckets/loan-data-bucket-aws/?region=us-east-1
	-  You will see directories ``report_one/`` and ``report_two``.

## References
- http://constructedtruth.com/2020/03/07/sending-data-to-kinesis-firehose-using-python/
- https://medium.com/big-data-on-amazon-elastic-mapreduce/run-a-spark-job-within-amazon-emr-in-15-minutes-68b02af1ae16
