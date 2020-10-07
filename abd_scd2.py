from pyspark.sql.functions import lit, current_timestamp, max, col, row_number, coalesce
from pyspark.sql.window import *
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

#define schema of csv
#schema = StructType([
 #   StructField("IATACode", StringType()),
  #  StructField("PlaceName", StringType()),
   # StructField("CountryName", StringType()),
    #StructField("NumberCode", IntegerType())
#])

#read csv from blob storage
df_blob = spark.read.csv('dbfs:/mnt/sampleDirBlob/sampledir/IATAConversion.txt',inferSchema=True,header=True).\
  withColumnRenamed("IATACode","blobIATACode").\
  withColumnRenamed("PlaceName","blobPlaceName").\
  withColumnRenamed("CountryName","blobCountryName").\
  withColumnRenamed("NumberCode","blobNumberCode")

#Synapse connection jdbc setting  
jdbcHostname = "dialmcsqlserversbx01.database.windows.net"
jdbcDatabase = "dial-mc-sqlpool-sbx-01"
userName = 'dialmcadmin'
password = '@RonMeulenbroek'
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, userName, password)

#read from target  
df_syn = spark.read.jdbc(url=jdbcUrl, table='sample_schema.IATAConversionDim')
#maximum value of surrogate key in target
max_Sur = df_syn.agg(max("SurrogateId")).withColumnRenamed('max(SurrogateId)','maxSurr')

#process existing records
df_inner = df_blob.join(df_syn, df_blob.blobIATACode == df_syn.IATACode)
#dataframe for all inactive records in the target
df_inner_inactive = df_inner.filter("ActiveIndicator = 'N'").select(df_syn.SurrogateId, df_syn.IATACode, df_syn.PlaceName, df_syn.CountryName, df_syn.NumberCode, df_syn.EffectiveStartDate, df_syn.EffectiveEndDate, df_syn.ActiveIndicator)
#check for change in any column for active records
df_inner_change = df_inner.filter("ActiveIndicator = 'Y' and (blobPlaceName <> PlaceName or blobCountryName <> CountryName or blobNumberCode <> NumberCode)")
#capture unchanged active records
df_inner_unchanged = df_inner.filter("ActiveIndicator = 'Y' and blobPlaceName = PlaceName and blobCountryName = CountryName and blobNumberCode = NumberCode").\
                      select(df_syn.SurrogateId, df_syn.IATACode, df_syn.PlaceName, df_syn.CountryName, df_syn.NumberCode,\
                              df_syn.EffectiveStartDate,df_syn.EffectiveEndDate, df_syn.ActiveIndicator)
#Dataframe for all updates to convert active changed records to inactive
df_inner_active = df_inner_change.select(df_syn.SurrogateId, df_syn.IATACode, df_syn.PlaceName, df_syn.CountryName, df_syn.NumberCode, df_syn.EffectiveStartDate).withColumn("EffectiveEndDate",current_timestamp()).withColumn("ActiveIndicator",lit('N'))
# Dataframe for chnaged records to insert the updates
df_inner_new = df_inner_change.select(df_syn.SurrogateId, df_blob.blobIATACode, df_blob.blobPlaceName, df_blob.blobCountryName, df_blob.blobNumberCode).withColumn("blobEffectiveStartDate",current_timestamp()).withColumn("blobEffectiveEndDate",lit(('2040-12-31 23:59:59')).cast('timestamp')).withColumn("ActiveIndicator",lit('Y'))

#the new inserts
df_left_new = df_blob.join(df_syn, df_blob.blobIATACode == df_syn.IATACode, how = "left").filter('IATACode is null')
df_left_new = df_left_new.select(col('blobIATACode').alias('IATACode'),col('blobPlaceName').alias('PlaceName'),\
                                 col('blobNumberCode').alias('NumberCode'),col('blobCountryName').alias('CountryName')).\
                                  withColumn("EffectiveStartDate",current_timestamp()).\
                                  withColumn("EffectiveEndDate",lit(('2040-12-31 23:59:59')).cast('timestamp')).\
                                  withColumn("ActiveIndicator",lit('Y')).\
                                  withColumn('SurrogateId', row_number().over(Window.orderBy("NumberCode"))).\
                                  crossJoin(max_Sur)
df_left_new = df_left_new.withColumn('NewSurrogate',col('SurrogateId')+coalesce(col('maxSurr'),lit(0))).select(col('NewSurrogate').alias('SurrogateId'),'IATACode','PlaceName','CountryName','NumberCode','EffectiveStartDate','EffectiveEndDate','ActiveIndicator')

#final dataframe to write to the table
df_final = df_inner_active.union(df_inner_inactive).union(df_inner_new).union(df_left_new).union(df_inner_unchanged)

#df_final.write.jdbc(url=jdbcUrl, table='sample_schema.IATAConversionDim_cpy', mode = 'append')
df_final.write.option("truncate", "true").jdbc(url=jdbcUrl, table='sample_schema.IATAConversionDim', mode = 'overwrite')
