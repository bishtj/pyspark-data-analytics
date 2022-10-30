###Problem statement

Write a Spark application that will read security_raw and create a new table called security_normalised.
This table should be in csv format with header columns and it will be identical to security_raw except of the following changes:
Drop the columns: PrefixId, InsSrc, UpdSrc

Create a new column called: NormalisationDate -> this will be the timestamp of the insertion time of this record to your new table (UTC).

Create a new boolean column called: IsNew. If the InsDt column of the record is in the last 12 months then the value will be true, otherwise false.

Aggregate the data in the security_normalised table by Prefix, Product and issue year calculating the minimum, maximum and sum of the issue amount.

Store the aggregated data in a table called prefix_product_issue_year.

Generate a matrix of the sum of the Issue Amount column by Prefix and Product.
Use the values for Prefix as columns and the Product values as the rows in the matrix. Output the result to either a Hive table or text file
e.g.

Attempt | AF | AR | JM | RM | Total | 
--- | --- | --- | --- |--- |--- |
GNMIIARM | 128898922.00 | 269109541.40 | 0.00 | 0.00 | 398008463.40 
GNMII30MJM | 0.00 | 0.00 | 548253269.00 | 0.00 | 548253269.00 
GNMII15MJM | 0.00 | 0.00 | 2515477.00 | 0.00 | 2515477.00 
GNMIIREVMTG | 0.00 | 0.00 | 0.00 | 1120611.00 | 1120611.00 
Total | 128898922.00 | 269109541.40 | 550768746.00 | 1120611.00 | 949897820.40 

input data format:
http://embs.com/public/html/PostProcessedFileFmt.htm#Sec

###Local test run steps:

- install pyspark

- pip install pytest

- pip install chispa

- setup env variable "SPARK_HOME" 

  * export SPARK_HOME=/Users/actual path/spark-version-bin-hadoop version
  * example export SPARK_HOME=/Users/user_name/projects/spark-3.0.1-bin-hadoop2.7