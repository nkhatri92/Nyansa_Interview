import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

spark = SparkSession.builder.appName('nyansa').getOrCreate()

#Read text file in a data frame
df1 = spark.read.option("header", "false") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv(sys.argv[1])

#put data in appropriate columns
split_col = F.split(df1['_c0'], "\\|")
df1 = df1.withColumn('to_time_stamp', split_col.getItem(0))
df1 = df1.withColumn('url', split_col.getItem(1))

#convert to date
df1 = df1.withColumn('date', F.from_unixtime('to_time_stamp','MM/dd/yyyy').cast(StringType()))

#group by date and url
df_grouped= df1.groupby('date','url') \
               .agg(F.count('url')) \
               .orderBy(["date", "count(url)"], ascending=[1, 0])
               
df_grouped = df_grouped.withColumnRenamed('count(url)','counts')

#df1.select(F.rpad("date", 14, " GMT")).show() 

l  = df_grouped.collect()

#print as required
def print_result(list_data):    
    prev_date = 0
    for each_row_val in list_data:
        if prev_date != each_row_val.date:
            sys.stdout.write(each_row_val.date + " GMT"+ '\n')
        sys.stdout.write(each_row_val.url + " " + str(each_row_val.counts) + '\n')   
        prev_date = each_row_val.date

print_result(l)
spark.stop()