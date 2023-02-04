# Demo AWS CDC Project

This project creates a small demo of CDC on AWS with components:
- AWS RDS : CDC data source
- AWS DMS : CDC tool
- AWS S3 : CDC Target and Glue job Output
- AWS Lambda : Trigger to AWS Glue
- AWS Glue : process input data from S3 to Output using pyspark


## Data Pipeline

![image](https://user-images.githubusercontent.com/124248166/216770282-9e605aff-0f11-4503-bbad-c5ef5f3dff65.png)




## Step

### 1/ Create AWS RDS Instance (MySQL)
### 2/ Create AWS S3 Buckets
- Create S3 Target Bucket for DMS
- Create S3 Output Bucket for Glue Job
### 3/ Create AWS DMS task 
- Create source Endpoint to RDS
- Create target Endpoint to S3 Target Bucket, creaet IAM Role with AmazonS3FullAccess and grant to DMS
- Create Replication instance
- Create Database migration task with "Full load, ongoing replication" config
### 4/ Create AWS Lambda
- Create Lambda funtion with Python code (Code store in SourceCode folder)
- Add trigger with Event type: s3:ObjectCreated:* 
- Create IAM Role with AmazonS3FullAccess, CloudWatchFullAccess, AWSGlueConsoleFullAccess and grant to Lambda function
### 5/ Create AWS Glue job 
- Create AWS Glue job with Pyspark code (Code store in SourceCode folder)
- Create IAM Role with AmazonS3FullAccess, CloudWatchFullAccess and grant to Glue job
- Create Invoke from Lambda funtion to Glue job to run Glue job when DMS create new file to S3 Target Bucket and trigg Lambda funtion
### 6/ Test Data Pipeline
- Full load data: Create schema, table and Insert dump data (Dump file in DumpData folder). Run Database migration task, check S3 Target Bucket (DMS target Endpoint) and S3 Output Bucket (Glue job Output)
- CDC : excute change data script (Dump file in DumpData folder), check S3 Target Bucket (DMS target Endpoint) and S3 Output Bucket (Glue job Output)
