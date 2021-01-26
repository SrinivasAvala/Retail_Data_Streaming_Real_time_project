from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


#Creation of Spark Session
spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

#Spark Session is active
	
#Connecting to Kafka Server and read data from real time project 
kafkaRealTime = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers"," Provide Server name")  \ ## Edited this for security purpose
    .option("startingOffsets","latest") \
	.option("subscribe","Topic Name")  \ ## Edited this for security purpose
	.load()

#Connected ot the Kafka from remote server

#Schema definition -- defining only required attributes
jsonSchema=StructType() \
    .add("invoice_no",LongType()) \
    .add("country",StringType()) \
    .add("timestamp",TimestampType()) \
    .add("type",StringType()) \
    .add("items",ArrayType(StructType([
 StructField("quantity",IntegerType()),
 StructField("unit_price",DoubleType()),
])))

#Defininig of Scmea to Json object is completed          

# Step 1:- Reading the sales data from the Kafka server
orderStream=kafkaRealTime.select(from_json(col("value").cast("string"),jsonSchema).alias("base_data")).select("base_data.*")

# Step 2:- Preprocessing the data to calculate additional derived columns such as total_cost etc            

IsorderUDF=udf(lambda x: 1 if x=='ORDER' else 0 ,IntegerType())
IsreturnUDF=udf(lambda x :1 if x=='RETURN' else 0 ,IntegerType())

expandedorderStream=orderStream.withColumn("Is_Order",IsorderUDF(orderStream.type)) 
Order_Intelligence_System1=expandedorderStream.withColumn("is_return",IsreturnUDF(orderStream.type))

def get_total_item_count(x):
    total=0
    for i in x:
        total=total+i[0]
    return total
      
   
add_total_item_count=udf(get_total_item_count,IntegerType())
Order_Intelligence_System2=Order_Intelligence_System1.withColumn("Total_Items",add_total_item_count(orderStream.items))


def get_total_cost(y,flag):
    total_cost=0
    for j in y:
        total_cost=total_cost+j[0]*j[1]
    if flag==1:
        total_cost=total_cost*-1
    return total_cost
get_total_cost_udf=udf(get_total_cost,DoubleType())

Order_Intelligence_System=Order_Intelligence_System2.withColumn("Total_Cost",round(get_total_cost_udf(orderStream.items,Order_Intelligence_System1.is_return),2))

query = Order_Intelligence_System  \
	.select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return") \
	.writeStream  \
	.outputMode("append")  \
	.format("console")  \
    .option("truncate","false") \
    .trigger(processingTime="1 minute") \
	.start()

#Step 3:- Calculating the time-based KPIs and time and country-based KPIs

#calculating time based KPI
aggStreambyTime=Order_Intelligence_System \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute", "1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
       count("invoice_no").alias("OPM"),
          avg("is_return").alias("Rate_of_return")) \
    .select("window",
            "OPM",
            "Rate_of_return",
            "total_volume_of_sales")

aggStreambyTime1=aggStreambyTime.withColumn("Average_transaction_size",aggStreambyTime.total_volume_of_sales/aggStreambyTime.OPM)

#calculating time and country based KPI

aggStreambyCountryandTime=Order_Intelligence_System \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute", "1 minute"),"country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
       count("invoice_no").alias("OPM"),
          avg("is_return").alias("Rate_of_return")) \
    .select("window",
            "country",
            "OPM",
            "Rate_of_return",
            "total_volume_of_sales")

# Step 4:- Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis
#writing the time based KPI output to json
queryByTime = aggStreambyTime1 \
             .writeStream \
             .format("json") \
             .outputMode("append") \
             .option("truncate","false") \
             .option("path","/tmp/output1") \
             .option("checkpointLocation","/tmp/coutput1") \
             .trigger(processingTime="1 minute") \
             .start()
#writing the time and country based KPI output to json
queryByCountryandTime =  aggStreambyCountryandTime.writeStream \
             .format("json") \
             .outputMode("append") \
             .option("truncate","false") \
             .option("path","/tmp/output2") \
             .option("checkpointLocation","/tmp/coutput2") \
             .trigger(processingTime="1 minute") \
             .start()
    	
query.awaitTermination()