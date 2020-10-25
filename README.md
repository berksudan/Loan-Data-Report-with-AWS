# Loan Data Reporting with AWS — A Case Study

## About

It is intended to be delivered a system built on Amazon Web Services using Apache Spark, Java and Python. The system completes objectives defined in the *Objectives* section with given data to generate loan reports.

## Architecture Design Scheme
![Architecture Design Scheme](docs/architecture_design_scheme.png)

## Prerequisites
+ *Make sure that,* a vanilla and default ***S3 bucket*** was created, for this case study, a S3 bucket named ``loan-data-bucket-aws`` was created.
+ *Make sure that,* you are using the right ***region***, for this case study, the region named ``us-east-1`` is used. Note that, if you use a different region, accordingly, you ought to change the region name in the code and links stated in this case study.
+ *Make sure that,* ***maven*** is installed and updated in your computer. It will be used to compile java code and to build jar files.
+ *Make sure that,* a Kinesis Data Firehose Delivery Stream was created and has access to your S3 bucket. For this case study, a delivery stream named ``Loan-Data-Loader`` was created. Its S3 Compression is ``GZIP`` and its source is configured as ``Direct PUT or other sources``.

## Objectives
+ Using the LendingClub public loan data two reports are generated using Aws Kinesis Firehose, S3 and EMR.
+ CSV data is sent to Kinesis Firehose to be written to a S3 bucket in gzip format using a Firehose client application.
+ A Spark application is generated two reports described below in desired format in the same bucket under *report_one* and *report_two* directories.
+ The Spark application read data from S3 bucket and also application run on an EMR cluster, and the cluster is configured to auto terminate after Spark application finished.

## Reports
**1.** Given this yearly income ranges, <40k, 40-60k, 60-80k, 80-100k and >100k. Generate a report that contains average loan amount and average term of loan in months based on these 5 income ranges. Result file is like “income range, avg amount, avg term”

**2.** In loans which are fully funded and loan amount greater than $1000, what is the fully paid amount rate for every loan grade of the borrowers. Result file is like “credit grade,fully paid amount rate”, eg.“A,%95”

## Module 1: Kinesis Firehose Client

### Used Modules
+ Python 3.6
+ boto3 (AWS S3 SDK for Python), for creating s3client and writing data to s3
+ wget, for downloading csv data initially

### How to Build and Run
+ Get following credentials from your AWS Account Console:
	- ***aws_access_key_id***
	- ***aws_secret_access_key***
	- ***aws_session_token***
+ Enter your credentials to ``<PROJECT_FOLDER>/modules/AWS-Kinesis-Firehose-Client/credentials.json``.
+ Run command using terminal to build:
```bash
$ <PROJECT_FOLDER>/modules/AWS-Kinesis-Firehose-Client/build.sh
```
+ Run command using terminal to run:
```bash
$ <PROJECT_FOLDER>/modules/AWS-Kinesis-Firehose-Client/run.sh
```

## Module 2: Fire URIs Retriever + Apache Spark Application

### Used Modules
+ Java 8 + Scala 2.11
+ Spark 2.10
+ Amazon S3 SDK for Java
+ Java 8

### How to Compile
+ Go to folder: ``<PROJECT_FOLDER>/modules/Spark-Loan-Processing/``.
+ Run command using terminal to compile:
```bash
$ mvn install
```
+ You will see the ``<PROJECT_FOLDER>/modules/Spark-Loan-Processing/target/`` directory after compilation.
+ Once compilation is completed, you will see file ``<PROJECT_FOLDER>/modules/Spark-Loan-Processing/target/Loan-Data-Report-with-AWS-1.0-SNAPSHOT-jar-with-dependencies.jar``.
+ Simply, upload jar file named ``Loan-Data-Report-with-AWS-1.0-SNAPSHOT-jar-with-dependencies.jar`` to your S3 bucket.

### How to Run
+ Get following credentials from your AWS Account Console:
	- ***aws_access_key_id***
	- ***aws_secret_access_key***
	- ***aws_session_token***

+ Create Cluster:
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
		- [Spark-submit options]: --class loanprocessing.LoanProcessor
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

+ Check S3 Bucket:
	-  Wait until cluster is created.
	-  Head to: https://s3.console.aws.amazon.com/s3/buckets/loan-data-bucket-aws/?region=us-east-1
	-  You will see directories ``report_one/`` and ``report_two/``.

## Dataset
Dataset URL already defined in ``DATA_URL`` property in file ``<PROJECT_FOLDER>/modules/AWS-Kinesis-Firehose-Client/properties.json``. If this dataset URL is absent, you can find the dataset [here](https://www.lendingclub.com/info/download-data.action), the zip archive contains raw data in csv format and an excel dictionary file explaining fields of data.

## References
- http://constructedtruth.com/2020/03/07/sending-data-to-kinesis-firehose-using-python/
- https://medium.com/big-data-on-amazon-elastic-mapreduce/run-a-spark-job-within-amazon-emr-in-15-minutes-68b02af1ae16
