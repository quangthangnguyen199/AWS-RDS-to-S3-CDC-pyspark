from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()
inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://mycdc-ouput-pyspark/output"

if "LOAD" in fileName:
    fldf = spark.read.csv(inputFilePath)
    fldf = fldf.withColumnRenamed("_c0","PersonId").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    fldf.write.mode("overwrite").csv(finalFilePath)
else:
    udf = spark.read.csv(inputFilePath)
    udf = udf.withColumnRenamed("_c0","action").withColumnRenamed("_c1","PersonId").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    ffdf = spark.read.csv(finalFilePath)
    ffdf = ffdf.withColumnRenamed("_c0","PersonId").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    for row in udf.collect(): 
      if row["action"] == 'U':
        ffdf = ffdf.withColumn("FullName", when(ffdf["PersonId"] == row["PersonId"], row["FullName"]).otherwise(ffdf["FullName"]))      
        ffdf = ffdf.withColumn("City", when(ffdf["PersonId"] == row["PersonId"], row["City"]).otherwise(ffdf["City"]))
    
      if row["action"] == 'I':
        insertedRow = [list(row)[1:]]
        columns = ['PersonId', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        ffdf = ffdf.union(newdf)
    
      if row["action"] == 'D':
        ffdf = ffdf.filter(ffdf.PersonId != row["PersonId"])
        
    ffdf.write.mode("overwrite").csv(finalFilePath)   
