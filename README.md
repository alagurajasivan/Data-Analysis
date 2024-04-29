#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.sql.adaptive.skewJoin.enabled", "true").config("spark.driver.memory", "10g").config("spark.sql.shuffle.partitions", "20").appName('abc').getOrCreate()


# In[2]:


df_accounts=spark.read.option("header", "true").csv("./config/tcds/tcds_accounts.csv")


# In[3]:


accounts = df_accounts.rdd.map(lambda x: (x['Account-id'], x['AccountName'])).collectAsMap()

# Get list of account ids
account_ids = df_accounts.rdd.map(lambda x: x['Account-id']).collect()


# In[4]:


# Read bucketing.csv into a PySpark DataFrame
df_bucketing = spark.read.csv("./config/tcds/bucketing.csv", header=True)

# Extract unique tags from the DataFrame
unique_tags_in_bucketing = df_bucketing.select("ApplicationTag").distinct().rdd.map(lambda x: x[0]).collect()

# Create a dictionary from DataFrame columns ApplicationTag and Bucket
bucket_dict = df_bucketing.rdd.map(lambda x: (x["ApplicationTag"], x["Bucket"])).collectAsMap()

# Remove keys 'NA' and 'DEFAULT' from the dictionary
bucket_dict.pop('NA', None)
bucket_dict.pop('DEFAULT', None)


# In[5]:


from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define column names
APPLICATION_TAG = 'resource_tags_user_application'
ACCOUNT_ID = 'line_item_usage_account_id'
COST = 'tot_line_item_unblended_cost'
LINE_ITEM_TYPE = 'line_item_line_item_type'
NAME_TAG = 'resource_tags_user_name'
PRODUCT_ID = 'line_item_product_code'
PRODUCT = 'Product'
RESOURCE_ID = 'line_item_resource_id'
ADHOC_Request_ID = 'resource_tags_user_adhoc_request_id'
COST_CENTER = 'resource_tags_user_cost_center'
SAVINGS_PLAN_COST = 'savings_plan_savings_plan_effective_cost'


column_names = [
        APPLICATION_TAG, ACCOUNT_ID, LINE_ITEM_TYPE, COST, PRODUCT_ID,
        RESOURCE_ID, NAME_TAG, ADHOC_Request_ID, COST_CENTER,SAVINGS_PLAN_COST
    ]


# Read the CSV file into a DataFrame
#df_raw = spark.read.csv('./config/tcds/raw_4.csv', header=True, inferSchema=True).select(*column_names)#.limit(600000)
import json
schema_json = '{"fields":[{"metadata":{},"name":"savings_plan_savings_plan_effective_cost","nullable":true,"type":"double"},{"metadata":{},"name":"year","nullable":true,"type":"integer"},{"metadata":{},"name":"month","nullable":true,"type":"integer"},{"metadata":{},"name":"line_item_usage_start_date","nullable":true,"type":"string"},{"metadata":{},"name":"line_item_usage_end_date","nullable":true,"type":"string"},{"metadata":{},"name":"bill_payer_account_id","nullable":true,"type":"long"},{"metadata":{},"name":"line_item_usage_account_id","nullable":true,"type":"long"},{"metadata":{},"name":"line_item_resource_id","nullable":true,"type":"string"},{"metadata":{},"name":"line_item_line_item_type","nullable":true,"type":"string"},{"metadata":{},"name":"line_item_product_code","nullable":true,"type":"string"},{"metadata":{},"name":"product_location","nullable":true,"type":"string"},{"metadata":{},"name":"product_region","nullable":true,"type":"string"},{"metadata":{},"name":"line_item_currency_code","nullable":true,"type":"string"},{"metadata":{},"name":"line_item_unblended_rate","nullable":true,"type":"double"},{"metadata":{},"name":"tot_line_item_unblended_cost","nullable":true,"type":"double"},{"metadata":{},"name":"line_item_blended_rate","nullable":true,"type":"double"},{"metadata":{},"name":"tot_line_item_blended_cost","nullable":true,"type":"double"},{"metadata":{},"name":"resource_tags_aws_autoscaling_group_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_aws_cloudformation_logical_id","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_aws_cloudformation_stack_id","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_aws_cloudformation_stack_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_aws_created_by","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_jumpbox","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_cluster","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_m_o_m","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_mo_m","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_owner","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_p_m","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_project","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_adhoc_request_id","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_application","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_cost_center","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_data_governance","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_env","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_environment","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_hanzo_demo","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_hanzo_share","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_hours","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_k8s_io_etcd_events","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_k8s_io_etcd_main","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_k8s_io_role_master","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_io_cluster_useast1_kops_tcinno_net","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_io_created_for_pv_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_io_created_for_pvc_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_io_created_for_pvc_namespace","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_kubernetes_io_service_name","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_lambda_created_by","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_loc_code","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_product","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_service_level","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_source","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_storage","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_terraform","nullable":true,"type":"string"},{"metadata":{},"name":"resource_tags_user_test","nullable":true,"type":"boolean"},{"metadata":{},"name":"resource_tags_user_workload_type","nullable":true,"type":"string"}],"type":"struct"}'
schema = StructType.fromJson(json.loads(schema_json))
df_raw = spark.read.schema(schema).csv('./config/tcds/5bcbbd01-0fcf-4138-b079-5456c4d38245.csv', header=True).select(*column_names).cache()#.limit(600000)


# In[6]:


from pyspark.sql.functions import sum as spark_sum
# Select required columns from df_raw
df_raw_SPC = df_raw.select('savings_plan_savings_plan_effective_cost', 'line_item_usage_account_id')

# Rename 'line_item_usage_account_id' column to 'Account-id'
total_df_raw = df_raw_SPC.withColumnRenamed('line_item_usage_account_id', 'Account-id')

# Group by 'Account-id' and sum 'savings_plan_savings_plan_effective_cost'
total_df_raw_SPC = total_df_raw.groupBy('Account-id').agg(spark_sum('savings_plan_savings_plan_effective_cost').alias('Total_Savings_Plan_Cost'))

#perform join operation on 'Account-id' column
total_df_raw_SPC_merge = total_df_raw_SPC.join(df_accounts, on='Account-id', how='inner')


# In[7]:


from pyspark.sql.functions import col, lit,upper

df_raw = df_raw.filter(col('line_item_usage_account_id').isin(account_ids))

# Convert RESOURCE_ID and APPLICATION_TAG to uppercase if they exist
if RESOURCE_ID in df_raw.columns:
    df_raw = df_raw.withColumn(RESOURCE_ID, upper(col(RESOURCE_ID).cast("string")))


if APPLICATION_TAG in df_raw.columns:
    df_raw = df_raw.withColumn(APPLICATION_TAG, upper(col(APPLICATION_TAG).cast("string")))

# Get unique line item types from the DataFrame
unique_line_type = df_raw.select(LINE_ITEM_TYPE).distinct().rdd.map(lambda x: x[0]).collect()

# Read line_item_type_file into a DataFrame
line_item_type_df = spark.read.option("header", "true").csv('./config/tcds/line_item_type.csv')

# Get unique line item types from the file
unique_line_type_from_file = line_item_type_df.select("line_item_type").rdd.map(lambda x: x[0]).collect()

# Find new line item types not present in the file
new_line_item_type = list(set(unique_line_type) - set(unique_line_type_from_file))

if len(new_line_item_type) > 0:
    raise Exception('New line item type found: {}. Terminating the application. Please add the new line item type in the line_item_type.csv file with the same name as charge type. Also, add an entry in the common split percent file with 0 percentage.'.format(new_line_item_type))

# Remove 'Usage' from unique_line_type
if 'Usage' in unique_line_type:
    unique_line_type.remove('Usage')

# Filter out 'DEFAULT' and 'NA' from unique_tags_in_bucketing
new_tag_list = [i for i in unique_tags_in_bucketing if i not in ['DEFAULT', 'NA']]

# Select relevant columns from df_raw
df_input = df_raw.select(
    col('resource_tags_user_application'),
    col('line_item_usage_account_id'),
    col('line_item_line_item_type'),
    col('tot_line_item_unblended_cost'),
    col('resource_tags_user_adhoc_request_id'),
    col('line_item_resource_id'),
    col('line_item_product_code'),
    col('resource_tags_user_cost_center'),
    col('savings_plan_savings_plan_effective_cost')
)

# Add a new column 'Product' with default value 'NO MATCH'
df_input = df_input.withColumn('Product', lit('NO MATCH'))


# In[8]:


# Read CSV files into PySpark DataFrames
df_split_common = spark.read.csv('./config/tcds/common_split_percent.csv', header=True)
df_split_fanout = spark.read.csv('./config/tcds/fanout_split_services.csv', header=True)
df_split_eks = spark.read.csv('./config/tcds/eks_split_percent.csv', header=True)
rds_products_ds = spark.read.csv('./config/tcds/rds_products.csv', header=True)
account_based_common_split = spark.read.csv('./config/tcds/account_based_common_split.csv', header=True)
df_tag_cost_split = spark.read.csv('./config/tcds/tag_cost_split.csv', header=True)
line_item_type_df = spark.read.option("header", "true").csv('./config/tcds/line_item_type.csv')


# In[9]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

import boto3
import dfply as dp
import pandas as pd
from dfply import X
from datetime import date, timedelta

def cost_breakdown_raw(df_raw, month, year, unique_line_type,
                       unique_tags_in_bucketing, df_bucketing, df_input,
                       line_item_type_df,
                       adhoc=False):
    """
    Takes a list of tags for pattern matching and search in the"raw" dataframe.
    If match found then update"Product"column value with the corresponding
    "Bucket" value from the "bucketing" dataframe.If line item type is not
    "Usage"then update "Product" column with the corresponding line item
    type. Then calculates the sum of total cost based on acc_id, line item,type
    ,tags and products.And finally sorts the dataframe in alphabetical
    order(based on tags and blank spaces at the top)and saves into
    Cost_Breakdown_Raw dataframe.If in the input dataframe any new tags are
    found then the script will be terminated.
    """
    
    start_time = time.time()
    # Convert unique_line_type to uppercase
    unique_line_type_upper = [t.upper() for t in unique_line_type]
    # Get new tags from df_raw
    new_tag_list = [tag for tag in df_raw.select(APPLICATION_TAG).distinct().rdd.map(lambda x: x[0]).collect() if  tag is not None and tag.upper() not in unique_tags_in_bucketing and tag != '']
    if new_tag_list and False:
        new_tags_file = 'new_tags_NA.csv'
        df_new_bucket = df_raw.filter(col(APPLICATION_TAG).isin(new_tag_list))
        df_new_bucket.toPandas().to_csv(default_config['local_path'] + new_tags_file, index=False)
        if not adhoc:
            s3_client = boto3.client('s3')
            s3_client.upload_file(default_config['local_path'] + new_tags_file, default_config['bucket_name'], f'billing/{year}/{month}/input/{new_tags_file}')
        raise Exception('Few tags are missing in the bucketing file')
    # Update "Product" with line item type if its != "Usage"
    df_input = df_input.withColumn(APPLICATION_TAG, F.when(F.upper(col(LINE_ITEM_TYPE)).isin(unique_line_type_upper), col(LINE_ITEM_TYPE)).otherwise(col(APPLICATION_TAG)))
    # Update "Product" column with corresponding bucket based on pattern matching
    tag_bucket_map_df = df_bucketing.dropDuplicates().selectExpr('ApplicationTag as key','Bucket as val')
    df_input = df_input.join(tag_bucket_map_df,F.lower(df_input[APPLICATION_TAG]) == F.lower(tag_bucket_map_df['key']),"left").drop(tag_bucket_map_df['key']).withColumn(PRODUCT,F.when(tag_bucket_map_df['val'].isNotNull(), tag_bucket_map_df['val']).otherwise(col(PRODUCT)) ).drop(tag_bucket_map_df['val'])
    line_item_type_df = line_item_type_df.withColumn('charge_type', F.upper(col('charge_type')))
    line_item_type_df = line_item_type_df.filter(col('line_item_type') != 'Usage')
    line_item_type_df_dict = dict(line_item_type_df.rdd.map(lambda x: (x['line_item_type'], x['charge_type'])).collect())
    """for line_item_type, charge_type in line_item_type_df_dict.items():
        df_input = df_input.withColumn(PRODUCT, F.when(col('line_item_line_item_type') == line_item_type, charge_type).otherwise(col(PRODUCT)))
    """
    lineitem_chargetype_map_df = spark.sparkContext.parallelize([(k,)+(v,) for k,v in line_item_type_df_dict.items()]).toDF(['key','val'])
    df_input = df_input.join(F.broadcast(lineitem_chargetype_map_df),F.lower(df_input['line_item_line_item_type'])==F.lower(lineitem_chargetype_map_df['key']),"left").drop(lineitem_chargetype_map_df['key']).withColumn(PRODUCT,F.when(lineitem_chargetype_map_df['val'].isNotNull(), lineitem_chargetype_map_df['val']).otherwise(col(PRODUCT)) ).drop(lineitem_chargetype_map_df['val'])
    # Group by necessary columns and sum costs
    df_temp = df_input.select(
        'line_item_usage_account_id',
        'line_item_line_item_type',
        'resource_tags_user_application',
        'tot_line_item_unblended_cost',
        'Product',
        'line_item_product_code',
        'savings_plan_savings_plan_effective_cost'
    )
    df_aws_services = df_temp.groupBy(
        'line_item_usage_account_id',
        'Product',
        'line_item_product_code'
    ).agg(
        F.sum('tot_line_item_unblended_cost').alias(COST),
        F.sum('savings_plan_savings_plan_effective_cost').alias(SAVINGS_PLAN_COST)
    ).filter(col('Product') != 'NO MATCH').filter(col('Product') != 'USAGE')
    df_cost_breakdown = df_temp.groupBy(
        'line_item_usage_account_id',
        'line_item_line_item_type',
        'resource_tags_user_application',
        'Product'
    ).agg(
        F.sum('tot_line_item_unblended_cost').alias(COST),
        F.sum('savings_plan_savings_plan_effective_cost').alias(SAVINGS_PLAN_COST)
    ).orderBy('resource_tags_user_application', ascending=True, na_position='first')
    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'Cost breakdown completed in {end_time} minutes')

    return df_cost_breakdown, df_input, df_aws_services


# In[10]:


from datetime import datetime 
from dateutil.relativedelta import relativedelta
date_time = datetime.now() - relativedelta(months=1)
year = date_time.year
month = date_time.strftime('%m')
df_cost_breakdown, df_input_copy, df_aws_services = cost_breakdown_raw(
        df_raw, month, year, unique_line_type,
        unique_tags_in_bucketing, df_bucketing, df_input, line_item_type_df,
        False
    )


# In[11]:


# Convert df_cost_breakdown to df_tag_cost_split_input
df_tag_cost_split_input = df_cost_breakdown

# Join df_tag_cost_split_input and df_tag_cost_split on specified columns
df_usage = df_tag_cost_split_input.join(df_tag_cost_split, on=['line_item_usage_account_id', 'resource_tags_user_application'], how='left')

# Calculate split_total
df_usage = df_usage.withColumn('split_total', (col('tot_line_item_unblended_cost') * col('split_percentage') / 100))

# Filter out rows with non-null and non-empty AccountName
df_usage = df_usage.filter((col('AccountName').isNotNull()) & (col('AccountName') != ''))

# Select relevant columns and drop duplicates for df_split_remove
df_split_remove = df_usage.select('AccountName', 'line_item_usage_account_id', 'Product', 'tot_line_item_unblended_cost').dropDuplicates()

# Rename columns for df_split_remove
df_split_remove = df_split_remove.withColumnRenamed("AccountName", "Account").withColumnRenamed("tot_line_item_unblended_cost", "split_total")

# Select relevant columns and rename columns for df_split_add
df_split_add = df_usage.select('AccountName', 'line_item_usage_account_id', 'split_product', 'split_total') \
                       .withColumnRenamed("AccountName", "Account") \
                       .withColumnRenamed("split_product", "Product")

# Calculate sum of tot_line_item_unblended_cost for all_tag_cost
all_tag_cost = df_cost_breakdown.groupBy('line_item_usage_account_id', 'resource_tags_user_application', 'Product') \
                                .agg({'tot_line_item_unblended_cost': 'sum'})


# In[12]:


from pyspark.sql.functions import col

def cost_breakdown_resource_id(df_input_copy, bucket_dict, tag_list, df_aws_services):
    """
    Creates temporary dataframe with the records having no  application tag,
    adds a new column called "Product" with default value "NO MATCH".Then
    it replaces all NAN values to blank spaces.After that it searches in
    Resource ID column with the list of  tags and if matches found then
    update the corresponding "Product" and "Application tag" value. And
    finally aggregates the dataframe and calculate the  sum of total  cost
    grouped by Acc id, Line item type, tags and product and saves into
    Cost_Breakdown_ResID df.
    """
    
    

    # Filter rows with no application tag
    df_temp1 = df_input_copy.filter(col(APPLICATION_TAG).isNull())

    # Select relevant columns
    df_temp1 = df_temp1.select(APPLICATION_TAG, RESOURCE_ID, ACCOUNT_ID, LINE_ITEM_TYPE, COST, PRODUCT_ID, SAVINGS_PLAN_COST)

    # Add default 'Product' column
    df_temp1 = df_temp1.withColumn(PRODUCT, lit('NO MATCH'))
    df_temp1 = df_temp1.filter( df_temp1['resource_tags_user_application'].isNull()) 


    # Replace null values with empty strings
    df_temp1 = df_temp1.fillna('', subset=[RESOURCE_ID])

    # Update 'Product' and 'Application tag' based on Resource ID and tag list
    df_bucketing = spark.read.csv("./config/tcds/bucketing_resource.csv", header=True)
    tag_bucket_map_df = df_bucketing.selectExpr('ApplicationTag as key','Bucket as val','index1').dropDuplicates().filter(col('key').isin(new_tag_list)).sort(col("key").desc())
    df_temp1 = df_temp1.withColumn('index2', F.monotonically_increasing_id())
    df_temp1 = df_temp1.join(F.broadcast(tag_bucket_map_df),df_temp1[RESOURCE_ID].contains(tag_bucket_map_df['key']),"left").withColumn(PRODUCT,F.when(tag_bucket_map_df['val'].isNotNull(), tag_bucket_map_df['val']).otherwise(col(PRODUCT)) ).withColumn(APPLICATION_TAG,F.when(tag_bucket_map_df['key'].isNotNull(), tag_bucket_map_df['key']).otherwise(col(APPLICATION_TAG)) ).drop(tag_bucket_map_df['key']).drop(tag_bucket_map_df['val'])
    df_temp1 = df_temp1.withColumn('rowNum',F.row_number().over(Window.partitionBy("index2").orderBy(col("index1").desc()))).filter("rowNum = 1").drop('index1').drop('index2').drop('rowNum')

    
    # Copy the DataFrame
    df_res_copy = df_temp1.cache()

    # Group by necessary columns and sum costs
    df_aws_services1 = df_temp1.filter(col(PRODUCT) != 'NO MATCH').groupBy(ACCOUNT_ID, PRODUCT, PRODUCT_ID).agg(F.sum(COST).alias(COST), F.sum(SAVINGS_PLAN_COST).alias(SAVINGS_PLAN_COST))

    # Concatenate df_aws_services1 with df_aws_services
    df_aws_services = df_aws_services1.union(df_aws_services)

    # Group by necessary columns and sum costs
    df_temp1 = df_temp1.groupBy(ACCOUNT_ID, LINE_ITEM_TYPE, APPLICATION_TAG, PRODUCT).agg(F.sum(COST).alias(COST), F.sum(SAVINGS_PLAN_COST).alias(SAVINGS_PLAN_COST)).orderBy(col(APPLICATION_TAG).asc_nulls_first())

    # Select relevant columns and order by Application Tag
    df_cost_breakdown_res = df_temp1.select(ACCOUNT_ID, LINE_ITEM_TYPE, APPLICATION_TAG, COST, PRODUCT, SAVINGS_PLAN_COST).orderBy(col(APPLICATION_TAG).asc_nulls_first())

    return df_res_copy, df_cost_breakdown_res, df_aws_services


# In[13]:


df_resource_copy, df_cost_breakdown_resource, df_aws_services \
        = cost_breakdown_resource_id(
        df_input_copy, bucket_dict, new_tag_list, df_aws_services
    )


# In[14]:


from pyspark.sql.functions import col

def cost_breakdown_by_service(df_raw):
    start_time = time.time()
    
    # Filter rows for AmazonEKS
    eks_df = df_raw.filter((col(PRODUCT_ID) == 'AmazonEKS') & 
                           (col(LINE_ITEM_TYPE) == 'Usage') & 
                           (col(APPLICATION_TAG) == 'MOBILITY-COMMON'))
    
    # Filter rows for AmazonEC2 with NAME_TAG containing 'eks'
    eks_ec2_df = df_raw.filter((col(PRODUCT_ID) == 'AmazonEC2') & 
                                (col(LINE_ITEM_TYPE) == 'Usage') & 
                                (col(APPLICATION_TAG) == 'MOBILITY-COMMON') & 
                                (col(NAME_TAG).contains('eks')))
    
    # Union the two dataframes
    eks_df = eks_df.union(eks_ec2_df)
    
    # Group by necessary columns and sum costs
    eks_cost_breakdown = eks_df.groupBy(ACCOUNT_ID, APPLICATION_TAG, LINE_ITEM_TYPE, PRODUCT_ID).agg(F.sum(COST).alias(COST))
    
    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'cost breakdown by service completed in {end_time} minutes')
    
    return eks_cost_breakdown


# In[15]:


service_cost_breakdown_df = cost_breakdown_by_service(df_raw)


# In[16]:


from pyspark.sql.functions import col

def cost_breakdown_product_id(accounts, df_bucketing, df_cost_breakdown, df_resource_copy, df_split_eks, unique_tags_in_bucketing, service_cost_breakdown_df, df_aws_services, df_split_common):
    """
    Creates a dataframe "df_temp2" which contains only records having no
    tags associated after searching for tags in previous steps,
    group by records based on required fields and calculate sum of total
    cost and calls Cost_Breakdown_Final function.
    """
    start_time = time.time()

    # Filter rows where Product is 'NO MATCH'
    df_temp2 = df_resource_copy.filter(col(PRODUCT) == 'NO MATCH')

    # If all entries have tags associated
    if df_temp2.count() == 0:
        df_cost_breakdown = df_cost_breakdown.withColumn(PRODUCT, F.when(col(PRODUCT) == 'NO MATCH', 'COMMON').otherwise(col(PRODUCT)))

        # Group by necessary columns and sum costs
        df_final_temp = df_cost_breakdown.groupBy(ACCOUNT_ID, PRODUCT).agg(F.sum(COST).alias(COST)).orderBy(col(PRODUCT).asc())
        ## Group by Account ID and sum costs
        total_eks_cost_df = service_cost_breakdown_df.groupBy(ACCOUNT_ID).agg(F.sum(COST).alias(COST))
        ## Melt dataframe and rename columns
        account_alias_dict = dict((str(row[0]), accounts[str(row[0])]) for row in total_eks_cost_df.select(ACCOUNT_ID).collect())

        account_percentage_df = df_split_eks.select("Product", F.expr(f"""{"stack({}, {})".format(len(account_alias_dict), ", ".join(["'{}', `{}`".format(col, col) for col in account_alias_dict.values()]))} as (Account,Percentage)"""))
        account_map_df = spark.sparkContext.parallelize([(k,)+(v,) for k,v in account_alias_dict.items()]).toDF(['name','Account'])
        account_percentage_df = account_percentage_df.join(account_map_df,account_percentage_df['Account'] == account_map_df['Account'],"left").drop(account_map_df['Account']).withColumnRenamed('name',ACCOUNT_ID)
        actual_product_count_df = df_final_temp.join(account_percentage_df, on=[ACCOUNT_ID,PRODUCT], how='inner').groupBy(ACCOUNT_ID).count().withColumnRenamed("count", "actual_product_count")
        eks_split_product_count_df = account_percentage_df.filter(col("Percentage") != 0).groupBy(ACCOUNT_ID).count().withColumnRenamed("count", "product_count_in_split_file")
        final_percentage_df = df_final_temp.select(ACCOUNT_ID, PRODUCT).join(account_percentage_df, on=[ACCOUNT_ID, PRODUCT], how='inner').join(actual_product_count_df, on=ACCOUNT_ID, how='inner').join(eks_split_product_count_df, on=ACCOUNT_ID, how='inner')
        final_percentage_df = final_percentage_df.withColumn("Percentage", F.when((col("actual_product_count") < col("product_count_in_split_file")) & (col("actual_product_count") != 0) , 100 / col("actual_product_count")).otherwise(col("Percentage")))
        final_product_costs_df = total_eks_cost_df.join(final_percentage_df.select(ACCOUNT_ID, PRODUCT, 'Percentage'), on=ACCOUNT_ID, how='inner')
        ## Calculate costs
        final_product_costs_df = final_product_costs_df.withColumn(COST, (col(COST) * col("Percentage") / 100))
        df_final_temp = df_final_temp.withColumnRenamed(PRODUCT,'PRODUCT').withColumnRenamed(ACCOUNT_ID,'ACCOUNT_ID')
        final_product_costs_df = final_product_costs_df.withColumnRenamed(COST,'product_cost')
        total_eks_cost_df  = total_eks_cost_df.withColumnRenamed(COST,'eks_cost')
        df_final_temp1 = df_final_temp.join(final_product_costs_df, on= (df_final_temp['PRODUCT'] == final_product_costs_df[PRODUCT]) & (df_final_temp['ACCOUNT_ID'] == final_product_costs_df[ACCOUNT_ID]) , how='left').drop(final_product_costs_df[PRODUCT]).drop(final_product_costs_df[ACCOUNT_ID])
        df_final  = df_final_temp1.join(total_eks_cost_df, on=(df_final_temp1['PRODUCT'] == 'COMMON') & (df_final_temp1['ACCOUNT_ID'] == total_eks_cost_df[ACCOUNT_ID]) , how='left' ).drop(total_eks_cost_df[ACCOUNT_ID]) \
        .withColumnRenamed('ACCOUNT_ID',ACCOUNT_ID).withColumnRenamed('PRODUCT',PRODUCT).fillna( { 'product_cost':0, 'eks_cost':0 } ).withColumn('tot_line_item_unblended_cost', (col(COST) + col('product_cost')  - col('eks_cost') )).drop('product_cost').drop('eks_cost') 
        
        account_map_df = spark.sparkContext.parallelize([(k,)+(v,) for k,v in accounts.items()]).toDF(['name','Account'])
    
        df_final = df_final.join(account_map_df, df_final[ACCOUNT_ID] == account_map_df['name'],"left").drop(account_map_df['name']) 
        df_cost_breakdown_product = spark.createDataFrame([], schema=["empty"])
        
        return df_cost_breakdown_product, df_final

    else:
        # Iterate through unique tags and update Product column in df_temp2
        tag_bucket_map_df = df_bucketing.selectExpr("ApplicationTag as key","Bucket as val")
        df_temp2 = df_temp2.join(F.broadcast(tag_bucket_map_df),F.lower(df_temp2[PRODUCT_ID]) == F.lower(tag_bucket_map_df['key']),"left").drop(tag_bucket_map_df['key']).withColumn(PRODUCT,F.when(tag_bucket_map_df['val'].isNotNull(), tag_bucket_map_df['val']).otherwise(col(PRODUCT))).drop(tag_bucket_map_df['val'])
    
        # Group by necessary columns and sum costs
        df_aws_services2 = df_temp2.groupBy(ACCOUNT_ID, PRODUCT, PRODUCT_ID).agg(F.sum(COST).alias(COST), F.sum(SAVINGS_PLAN_COST).alias(SAVINGS_PLAN_COST)).orderBy(col(PRODUCT).asc())

        # Concatenate df_aws_services2 with df_aws_services
        df_aws_services = df_aws_services.union(df_aws_services2)

        # Replace 'NO MATCH' in PRODUCT column with 'COMMON'
        df_aws_services = df_aws_services.withColumn(PRODUCT, F.when(col(PRODUCT) == 'NO MATCH', 'COMMON').otherwise(col(PRODUCT)))

        # Group by necessary columns and sum costs
        df_cost_breakdown_product = df_temp2.groupBy(ACCOUNT_ID, LINE_ITEM_TYPE, PRODUCT_ID, PRODUCT, SAVINGS_PLAN_COST).agg(F.sum(COST).alias("tot_line_item_unblended_cost")).orderBy(col(PRODUCT).desc()).select(ACCOUNT_ID, LINE_ITEM_TYPE, PRODUCT_ID, COST, PRODUCT, SAVINGS_PLAN_COST)

    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'cost breakdown prod id completed in {end_time} minutes')

    return df_cost_breakdown_product, df_aws_services, None


# In[17]:


df_cost_breakdown_product, df_aws_services, df_final = \
        cost_breakdown_product_id(
            accounts, df_bucketing, df_cost_breakdown, df_resource_copy,
            df_split_eks, unique_tags_in_bucketing, service_cost_breakdown_df,
            df_aws_services, df_split_common
        )


# In[18]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum as spark_sum
from datetime import date, timedelta

def split_fanout_cost(df_final, spark):
    last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
    start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)
    start_day_of_prev_month = start_day_of_prev_month.strftime("%Y-%m-%d")

    fanout_split_file = 'fanout_split_services.csv'
    df_split_fanout = spark.read.csv('./config/tcds/fanout_split_services.csv', header=True)

    enrollment_sql = f"SELECT product, SUM(vincount) AS vincount FROM enrollment_metrics WHERE month='{start_day_of_prev_month}' AND region='NA' GROUP BY product"

    df_enrollment = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:8151/opsmetricsdb",driver = "com.mysql.jdbc.Driver",dbtable = "enrollment_metrics",user="ops_aurora_admin",password="N62fc5MHDVjJVKG2543rT").load()
    df_enrollment = df_enrollment.filter(col("month") == start_day_of_prev_month).filter(col("region") == "NA") \
        .groupBy("product").agg(spark_sum("vincount").alias("vincount"))

    # This is needed as all the service names from enrollment metrics table are not in sync with the billing output file
    df_enrollment = df_enrollment.withColumn("product", expr("""
        CASE 
            WHEN product = 'EVENT_NOTIFICATION' THEN 'ENS'
            WHEN product = 'TELEMETRY_STREAMING' THEN 'TELEMETRY-STREAMING'
            WHEN product = 'EXTERNAL_STREAMING' THEN 'MSFP EXTERNAL STREAMING'
            WHEN product = 'NON_AUTOMATIC_COLLISION_NOTIFICATION' THEN 'ACN'
            ELSE product
        END
    """))
    df_enrollment = df_enrollment[df_enrollment['product'].isin([i[0] for i in df_split_fanout.select('service').collect()])].cache()
    df_fanout = df_final.filter(col("Product") == "FANOUT")

    if df_fanout.count() > 0:
        fanout_list = df_fanout.collect()

        df_fanout_split = spark.createDataFrame([], df_final.select(ACCOUNT_ID, PRODUCT, COST, "Account", "percentage").schema)
        total_vin_count = df_enrollment.agg(F.sum(df_enrollment['vincount'])).collect()[0][0]
        
        for record in fanout_list:
            df_enrollment_temp = df_enrollment.withColumn(COST, col("vincount") * record[COST] / total_vin_count ) \
                .withColumn(COST, col(COST).cast("double"))

            total_cost = df_enrollment_temp.select(spark_sum(COST)).collect()[0][0]
            
            if total_cost != record[COST]:
                telem_cost = df_enrollment_temp.filter(col("product") == "TELEMETRY").select(COST).collect()[0][0]
                diff = record[COST] - total_cost
                df_enrollment_temp = df_enrollment_temp.withColumn(COST, 
                    expr(f"CASE WHEN product = 'TELEMETRY' THEN {COST} + " + str(diff) + f" ELSE {COST} END"))
            
            df_enrollment_temp = df_enrollment_temp.withColumn("Account", lit(record["Account"])) \
                .withColumn(ACCOUNT_ID, lit(record[ACCOUNT_ID])) \
                .withColumnRenamed("product", 'Product') \
                .withColumn("percentage", col(COST) * 100 / total_cost) \
                .select(ACCOUNT_ID, PRODUCT, COST, "Account", "percentage")

            df_fanout_split = df_fanout_split.union(df_enrollment_temp)

            df_final = df_final.select('Product','line_item_usage_account_id','Account','tot_line_item_unblended_cost').union(df_enrollment_temp.select('Product','line_item_usage_account_id','Account','tot_line_item_unblended_cost'))

        df_final = df_final.filter(col("Product") != "FANOUT") \
            .groupBy(PRODUCT, ACCOUNT_ID, "Account").agg(spark_sum(COST).alias(COST)).orderBy(PRODUCT)

    return df_final, df_fanout_split, df_fanout


# In[19]:


from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def cost_breakdown_final(accounts, df_cost_breakdown,
                         df_cost_breakdown_product, df_cost_breakdown_resource,
                         df_split_eks, service_cost_breakdown_df):
    """
    Takes three dataframes from the previous steps, after merging, calculate
    total cost grouped by acc_id, Product and saves in a dataframe
    "df_final". Then adds a new column "Account" and  from the accounts
    dictionary created from the accounts.csv file it searches for account
    type in "df_final" and fill up the Account column based on the match and
    finally calls a function "Save_MultipleDF_in_Excel".
    """

    # Create a new column for the total cost
    df_cost_breakdown_prod_sum = df_cost_breakdown_product.groupBy("line_item_usage_account_id", "Product") \
        .agg(F.sum("tot_line_item_unblended_cost").alias("tot_line_item_unblended_cost"),
             F.sum("savings_plan_savings_plan_effective_cost").alias("savings_plan_savings_plan_effective_cost"))
    cols = ['line_item_usage_account_id', 'line_item_line_item_type', 'resource_tags_user_application', 'tot_line_item_unblended_cost', 'Product', 'savings_plan_savings_plan_effective_cost']
    
    # Combine dataframes and filter
    df_merged_temp = df_cost_breakdown.select(*cols).union(df_cost_breakdown_resource.select(*cols)).filter(~F.col("resource_tags_user_application").isNull())

    # Concatenate dataframes
    df_final_temp = df_merged_temp.select(ACCOUNT_ID, PRODUCT, COST, SAVINGS_PLAN_COST) \
        .union(df_cost_breakdown_prod_sum.select(ACCOUNT_ID, PRODUCT, COST, SAVINGS_PLAN_COST)) 

    # Replace 'NO MATCH' with 'COMMON'
    df_final_temp = df_final_temp.withColumn(PRODUCT, F.when(F.col(PRODUCT) == "NO MATCH", "COMMON").otherwise(F.col(PRODUCT)))

    # Group by ACCOUNT_ID, PRODUCT and sum COST, SAVINGS_PLAN_COST
    df_final_temp = df_final_temp.groupBy(ACCOUNT_ID, PRODUCT) \
        .agg(F.sum(COST).alias(COST), F.sum(SAVINGS_PLAN_COST).alias(SAVINGS_PLAN_COST))

    # Group by ACCOUNT_ID and sum COST
    total_eks_cost_df = service_cost_breakdown_df.groupBy(ACCOUNT_ID).agg(F.sum(COST).alias(COST))

    
    
    account_alias_dict = dict((str(row[0]), accounts[str(row[0])]) for row in total_eks_cost_df.select(ACCOUNT_ID).collect())

    account_percentage_df = df_split_eks.select("Product", F.expr(f"""{"stack({}, {})".format(len(account_alias_dict), ", ".join(["'{}', `{}`".format(col, col) for col in account_alias_dict.values()]))} as (Account,Percentage)"""))
    account_map_df = spark.sparkContext.parallelize([(k,)+(v,) for k,v in account_alias_dict.items()]).toDF(['name','Account'])
    account_percentage_df = account_percentage_df.join(F.broadcast(account_map_df),account_percentage_df['Account'] == account_map_df['Account'],"left").drop(account_map_df['Account']).withColumnRenamed('name',ACCOUNT_ID)
    actual_product_count_df = df_final_temp.join(F.broadcast(account_percentage_df), on=[ACCOUNT_ID,PRODUCT], how='inner').groupBy(ACCOUNT_ID).count().withColumnRenamed("count", "actual_product_count")
    eks_split_product_count_df = account_percentage_df.filter(col("Percentage") != 0).groupBy(ACCOUNT_ID).count().withColumnRenamed("count", "product_count_in_split_file")
    final_percentage_df = df_final_temp.select(ACCOUNT_ID, PRODUCT).join(account_percentage_df, on=[ACCOUNT_ID, PRODUCT], how='inner').join(actual_product_count_df, on=ACCOUNT_ID, how='inner').join(eks_split_product_count_df, on=ACCOUNT_ID, how='inner')
    final_percentage_df = final_percentage_df.withColumn("Percentage", F.when((col("actual_product_count") < col("product_count_in_split_file")) & (col("actual_product_count") != 0) , 100 / col("actual_product_count")).otherwise(col("Percentage")))
    final_product_costs_df = total_eks_cost_df.join(final_percentage_df.select(ACCOUNT_ID, PRODUCT, 'Percentage'), on=ACCOUNT_ID, how='inner')
    df_final_savings = df_final_temp.select(ACCOUNT_ID, PRODUCT, SAVINGS_PLAN_COST)
    
    final_product_costs_df = final_product_costs_df.withColumn(COST, (col(COST) * col("Percentage") / 100))
    df_final_temp = df_final_temp.withColumnRenamed(PRODUCT,'PRODUCT').withColumnRenamed(ACCOUNT_ID,'ACCOUNT_ID')
    final_product_costs_df = final_product_costs_df.withColumnRenamed(COST,'product_cost')
    total_eks_cost_df  = total_eks_cost_df.withColumnRenamed(COST,'eks_cost')
    df_final_temp1 = df_final_temp.join(F.broadcast(final_product_costs_df), on= (df_final_temp['PRODUCT'] == final_product_costs_df[PRODUCT]) & (df_final_temp['ACCOUNT_ID'] == final_product_costs_df[ACCOUNT_ID]) , how='left').drop(final_product_costs_df[PRODUCT]).drop(final_product_costs_df[ACCOUNT_ID])
    df_final  = df_final_temp1.join(F.broadcast(total_eks_cost_df), on=(df_final_temp1['PRODUCT'] == 'COMMON') & (df_final_temp1['ACCOUNT_ID'] == total_eks_cost_df[ACCOUNT_ID]) , how='left' ).drop(total_eks_cost_df[ACCOUNT_ID]) \
        .withColumnRenamed('ACCOUNT_ID',ACCOUNT_ID).withColumnRenamed('PRODUCT',PRODUCT).fillna( { 'product_cost':0, 'eks_cost':0 } ).withColumn('tot_line_item_unblended_cost', (col(COST) + col('product_cost')  - col('eks_cost') )).drop('product_cost').drop('eks_cost') 
    
    account_map_df = spark.sparkContext.parallelize([(k,)+(v,) for k,v in accounts.items()]).toDF(['name','Account'])
    df_final = df_final.join(F.broadcast(account_map_df), df_final[ACCOUNT_ID] == account_map_df['name'],"left").drop(account_map_df['name'])  
            
    

    #
    # Split cost into df_fanout_split and df_fanout
    df_final, df_fanout_split, df_fanout = split_fanout_cost(df_final,spark)

    

    print("cost breakdown final completed")
    return df_final, df_fanout_split, df_fanout, df_final_savings


# In[20]:


df_final, df_fanout_split, df_fanout, df_final_savings = cost_breakdown_final(
        accounts, df_cost_breakdown, df_cost_breakdown_product,
        df_cost_breakdown_resource, df_split_eks, service_cost_breakdown_df
    )


# In[21]:


df_final.cache()
df_fanout_split.cache()
df_fanout.cache()
df_final.count()
df_fanout_split.count()
df_fanout.count()


# In[22]:


from pyspark.sql import functions as F

def generate_summary_report(df_split_remove, df_split_add, df_final):
    """
    Finds the unique Products and inserts into a list. Creates a pivot table
    dataframe "df_summary_table" from the "df_final" dataframe. Finally,
    calls Save_MultipleDF_in_Excel to generate an excel sheet.
    """

    # Merge df_split_remove with df_final and subtract split_total
    df_final = df_final.join(df_split_remove, ['Account', 'line_item_usage_account_id', 'Product'], "left_outer")
    df_final = df_final.withColumn("split_total", F.coalesce(df_final["split_total"], F.lit(0)))
    df_final = df_final.withColumn("tot_line_item_unblended_cost", F.col("tot_line_item_unblended_cost") - F.col("split_total"))
    df_final = df_final.drop("split_total")

    # Merge df_split_add with df_final and add split_total
    df_final = df_final.join(df_split_add, ['Account', 'line_item_usage_account_id', 'Product'], "left_outer")
    df_final = df_final.withColumn("split_total", F.coalesce(df_final["split_total"], F.lit(0)))
    df_final = df_final.withColumn("tot_line_item_unblended_cost", F.col("tot_line_item_unblended_cost") + F.col("split_total"))
    df_final = df_final.drop("split_total")

    # Select required columns
    df_final = df_final.select("Product", "line_item_usage_account_id", "Account", "tot_line_item_unblended_cost")

    # Start time measurement
    start_time = time.time()

    # Collect unique Products
    unique_products = df_final.select("Product").distinct().rdd.flatMap(lambda x: x).collect()

    # Round the cost to 6 decimal places
    df_final = df_final.withColumn("tot_line_item_unblended_cost", F.round("tot_line_item_unblended_cost", 6))

    # Create the summary table
    df_summary_table = df_final.fillna({"tot_line_item_unblended_cost": 0.0}).groupBy("Product").pivot("Account").sum("tot_line_item_unblended_cost")
    df_summary_table = df_summary_table.na.fill(0)
    df_summary_table = df_summary_table.withColumn("Grand Total", sum(  df_summary_table[col] for col in df_summary_table.columns if col != 'Product'))

    # End time measurement
    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'generate summary report completed in {end_time} minutes')

    return unique_products, df_summary_table, df_final


# In[23]:


unique_products, df_summary_table,df_dynamic_main_input = generate_summary_report(df_split_remove,df_split_add,df_final)


# In[24]:


account_based_common_split = spark.read.csv('./config/tcds/account_based_common_split.csv', header=True, inferSchema=True)


# In[25]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.sql import functions as F

def dynamic_split(spark, df_dynamic_main_input, df_bucketing, rds_products_ds, df_accounts, account_based_common_split):
    hardcoded_split_accounts = [row['AccountName'] for row in account_based_common_split.collect()]

    # Filter bucketing DataFrame
    values_to_exclude = ['COMMON', 'EDPDISCOUNT', 'SAVINGSPLANCOVEREDUSAGE', 'SAVINGSPLANNEGATION', 'BUNDLEDDISCOUNT']
    df_bucketing = df_bucketing.filter(~col('Bucket').isin(values_to_exclude)).dropDuplicates(['Bucket'])
    df_bucketing_list = df_bucketing.select('Bucket').rdd.flatMap(lambda x: x).collect()
    # Filter main input DataFrame
    df_dynamic_main_input = df_dynamic_main_input.filter(col('Product').isin(df_bucketing_list))
    df_dynamic_main_input = df_dynamic_main_input.select([col for col in df_dynamic_main_input.columns if not col.startswith('Unnamed')])
    # Compute total cost per account
    df_account_total = df_dynamic_main_input.groupBy('line_item_usage_account_id').agg(F.sum('tot_line_item_unblended_cost').alias('total_cost'))
    # Compute dynamic percentage
    df_dynamic_common_percent = df_dynamic_main_input.join(df_account_total, on='line_item_usage_account_id', how='inner')
    df_dynamic_common_percent = df_dynamic_common_percent.withColumn('dynamic_percentage', (col('tot_line_item_unblended_cost') / col('total_cost') * 100))
    # Rename columns and merge with bucketing DataFrame
    df_bucketing = df_bucketing.withColumnRenamed('Bucket', 'Product')
    df_dynamic_common_percent = df_dynamic_common_percent.join(F.broadcast(df_bucketing), on='Product', how='outer')
    df_dynamic_common_percent = df_dynamic_common_percent.join(F.broadcast(rds_products_ds), on='Product', how='left')
    df_dynamic_common_percent = df_dynamic_common_percent.fillna({'RDS-Product': 'NA'})
    # Select relevant columns and filter out hardcoded split accounts
    df_dynamic_common = df_dynamic_common_percent.select('Product', 'RDS-Product', 'Account', 'dynamic_percentage')
    df_dynamic_common = df_dynamic_common.filter(~col('Account').isin(hardcoded_split_accounts))
    # Rename columns and append fee product
    account_based_common_split = account_based_common_split.withColumn('RDS-Product',lit('NA'))
    df_account_based_split = account_based_common_split.withColumnRenamed("AccountName","Account").withColumnRenamed("Split","dynamic_percentage").select('Product', 'RDS-Product', 'Account', 'dynamic_percentage')
    df_dynamic_split_final = df_dynamic_common.union(df_account_based_split)
    df_dynamic_split_final = df_dynamic_split_final.fillna({'RDS-Product': 'NA'})
    df_pivot = df_dynamic_split_final.groupBy("Product").pivot("Account").agg(F.first("dynamic_percentage"))

    # Fill missing values with 0
    df_pivot = df_pivot.na.fill(0)
    df_pivot = df_pivot.join(rds_products_ds, on='Product', how='left')
    df_pivot = df_pivot.fillna({'RDS-Product': 'NA'})


    df_pivot.write.csv('test/common_split_percent/', mode='overwrite', header=True)

    return df_pivot


# In[26]:


df_split_common = dynamic_split(spark,df_dynamic_main_input, df_bucketing, rds_products_ds, df_accounts,
                                    account_based_common_split)


# In[27]:


from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import DataFrame

def dynamic_missing_accounts(df_dynamic_main_input: DataFrame, df_bucketing: DataFrame, account_based_common_split: DataFrame) -> DataFrame:
    hardcoded_split_accounts = [row['AccountName'] for row in account_based_common_split.collect()]
    df_bucketing = df_bucketing.filter(df_bucketing['Bucket'] != 'FANOUT')

    df_common_cost = df_dynamic_main_input.filter(df_dynamic_main_input['Product'] == 'COMMON')

    values_to_exclude = ['EDPDISCOUNT', 'SAVINGSPLANCOVEREDUSAGE', 'SAVINGSPLANNEGATION', 'BUNDLEDDISCOUNT']
    df_bucketing = df_bucketing.filter(~col('Bucket').isin(values_to_exclude)).dropDuplicates(subset=['Bucket'])
    df_bucketing_list = df_bucketing.select('Bucket').rdd.flatMap(lambda x: x).collect()

    df_dynamic_main_input = df_dynamic_main_input.filter(df_dynamic_main_input['Product'].isin(df_bucketing_list))
    df_dynamic_main_input = df_dynamic_main_input.select([col for col in df_dynamic_main_input.columns if not col.startswith('Unnamed')])

    df_account_total = df_dynamic_main_input.groupBy('line_item_usage_account_id').agg(spark_sum('tot_line_item_unblended_cost').alias('tot_line_item_unblended_cost_sum'))
    df_account_total = df_account_total.join(df_common_cost, on='line_item_usage_account_id')
    df_account_total = df_account_total.select([col for col in df_account_total.columns if not col.startswith('Unnamed')])

    df_account_total = df_account_total.withColumn('common_only_accounts', (col('tot_line_item_unblended_cost_sum') - col('tot_line_item_unblended_cost') == 0))
    df_common_only = df_account_total.filter(col('common_only_accounts') == True)

    dynamic_missing_accounts_df = df_common_only.filter(~col('Account').isin(hardcoded_split_accounts))
    dynamic_missing_accounts_df = dynamic_missing_accounts_df.select('line_item_usage_account_id', 'Account')

    dynamic_missing_accounts_df.write.csv('./test/dynamic_missing_accounts', mode='overwrite', header=True)

    return dynamic_missing_accounts_df


# In[28]:


df_dynamic_missing_accounts = dynamic_missing_accounts(df_dynamic_main_input, df_bucketing, account_based_common_split)


# In[29]:


df_final_savings_sp = df_final_savings
df_accounts_SP1 = df_accounts
total_df_raw = df_final_savings_sp.withColumnRenamed('line_item_usage_account_id', 'Account-id')
df_final_SPC = total_df_raw.join(df_accounts_SP1, on='Account-id')
df_final_SPC = df_final_SPC.withColumnRenamed('AccountName', 'Account')
#df_final_SPC = df_final_SPC.replace('SAVINGSPLANCOVEREDUSAGE', 'NetSavingsPlanCost')
df_final_SPC = df_final_SPC.withColumn('Product',F.when(df_final_SPC['Product'] == lit('SAVINGSPLANCOVEREDUSAGE'),lit('NetSavingsPlanCost')).otherwise(df_final_SPC['Product']))
#df_final_SPC.show(100)


# In[30]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def rds_enrollment(month, year):
    

    enrollment_date = f'{year}-{month}-01'
    ingest_date = int(str(year) + str(month))
    
    sql = f"select month, customer, product, vincount from " \
          f"enrollment_metrics WHERE month='{enrollment_date}' and region='NA' UNION ALL " \
          f"select '{enrollment_date}' as month,'TCDS' as customer,'INGEST' " \
          f"as product, vehicleCount as vincount from " \
          f"can_vehicle_metrics_monthly where code='AVG' and yearmonth=" \
          f" {ingest_date}"
    print(sql)

    enrollment_metrics_df = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:8151/opsmetricsdb",driver = "com.mysql.jdbc.Driver",query = sql,user="ops_aurora_admin",password="N62fc5MHDVjJVKG2543rT").load()
    

    return enrollment_metrics_df


# In[31]:


df_rds_product = rds_enrollment('03', '2024').cache()
df_rds_product.count()


# In[32]:


def generate_invoice(df_rds_product, df_split_common,
                     df_final,df_final_SPC, unique_products,
                     adhoc):
    """
    Creates a dataframe from split percentage csv file, based on the
    percentage and  it splits the  cost for "COMMON" across
    various regional accounts, saves the dataframe and calls
    "Save_MultipleDF_in_Excel" to save all dataframes into excel file
    """

    savings_plan_credit =df_final_SPC[df_final_SPC['Product']=='NetSavingsPlanCost']
    savings_plan_cost_total = df_final_SPC['savings_plan_savings_plan_effective_cost'].sum()
    savings_plan_credit['total']=savings_plan_cost_total
    savings_plan_credit['percentage']= (df_final_SPC['savings_plan_savings_plan_effective_cost']/savings_plan_credit['total']*100)
    savings_plan_credit['credit_amount_split'] = (SAVINGS_PLAN_CREDITS*savings_plan_credit['percentage']/100)

    savings_plan_credit = savings_plan_credit[['Account-id','Product','Account','credit_amount_split']]
    savings_plan_credit['credit_amount_split']=(0-savings_plan_credit['credit_amount_split'])
    savings_plan_credit = savings_plan_credit.rename(columns={"credit_amount_split": "savings_plan_savings_plan_effective_cost"})
    savings_plan_credit['Product']= 'NetSavingsPlanCost Credits'

    savings_plan_credit = pd.pivot_table(
        savings_plan_credit, values=SAVINGS_PLAN_COST, index=PRODUCT, columns='Account', fill_value=0
    )

    start_time = time.time()

    refund = None
    credit = None
    discount = None
    bundled_discount = None
    savingsplancoveredusage = None

    rds_product_dict = {}
    if not adhoc:
        rds_product_dict = dict(zip(
            df_split_common[PRODUCT].tolist(),
            df_split_common['RDS-Product'].tolist()
        ))

        #df_rds_product = rds_enrollment(month, year)
        rds_enrollment_dict = df_rds_product.groupby(
            ['product']
        )['vincount'].agg('sum').to_dict()

        for key in rds_product_dict:
            if rds_product_dict.get(key) in rds_enrollment_dict:
                rds_product_dict[key] = rds_enrollment_dict.get(
                    rds_product_dict.get(key))
            else:
                rds_product_dict[key] = 0

    df_split_common = df_split_common.drop(['RDS-Product'], axis=1)
    all_products = df_split_common[PRODUCT].tolist()
    df_split_common = df_split_common.set_index(PRODUCT)

    df_summary_table_copy = pd.pivot_table(
        df_final, values=COST, index=PRODUCT, columns='Account', fill_value=0
    )

    df_summary_table_copy1 = pd.pivot_table(
        df_final_SPC, values=SAVINGS_PLAN_COST, index=PRODUCT, columns='Account', fill_value=0
    )

    existing_products = list(df_summary_table_copy.index.values)
    missing_product = list(set(all_products).difference(existing_products))

    if missing_product:
        for product in missing_product:
            df_summary_table_copy.loc[product] = 0

    if 'CREDIT' in unique_products:
        credit_amount = df_final.query('Product == "CREDIT"')
        credit = credit_amount.set_index('Account').to_dict()[COST]
        df_summary_table_copy.drop(index='CREDIT', inplace=True)

    if 'REFUND' in unique_products:
        refund_price_df = df_final.query('Product == "REFUND"')
        refund = refund_price_df.set_index('Account').to_dict()[COST]
        df_summary_table_copy.drop(index='REFUND', inplace=True)

    if 'EDPDISCOUNT' in unique_products:
        discount_price_df = df_final.query('Product == "EDPDISCOUNT"')
        discount = discount_price_df.set_index('Account').to_dict()[COST]
        df_summary_table_copy.drop(index='EDPDISCOUNT', inplace=True)

    if 'BUNDLEDDISCOUNT' in unique_products:
        bundled_discount_price_df = df_final.query(
            'Product == "BUNDLEDDISCOUNT"')
        bundled_discount = \
            bundled_discount_price_df.set_index('Account').to_dict()[
                COST]
        df_summary_table_copy.drop(index='BUNDLEDDISCOUNT', inplace=True)

    if 'SAVINGSPLANCOVEREDUSAGE' in unique_products:
        SAVINGSPLANCOVEREDUSAGE_df = df_final_SPC.query('Product == "NetSavingsPlanCost"')
        SAVINGSPLANCOVEREDUSAGE = SAVINGSPLANCOVEREDUSAGE_df.set_index('Account').to_dict()[SAVINGS_PLAN_COST]
        df_summary_table_copy1.drop(index='NetSavingsPlanCost')
        df_summary_table_copy1 = df_summary_table_copy1[df_summary_table_copy1.index == 'NetSavingsPlanCost']

    all_data = df_summary_table_copy.append(df_summary_table_copy1)

    df_summary_table_copy = df_summary_table_copy.add(
        df_split_common.mul(df_summary_table_copy.loc['COMMON']).div(100)
    ).fillna(0)

    df_summary_table_copy = pd.concat(
        [df_summary_table_copy,
         pd.DataFrame(
             df_summary_table_copy.sum(axis=0),
             columns=['Grand Total w/o Refund and Credit and Discount']
         ).T]
    )

    has_refund_and_credit = all(
        x in unique_products for x in ['REFUND', 'CREDIT']
    )
    has_refund_and_discount = all(
        x in unique_products for x in ['REFUND', 'EDPDISCOUNT']
    )
    has_credit_and_discount = all(
        x in unique_products for x in ['CREDIT', 'EDPDISCOUNT']
    )
    has_credit_and_bundled_discount = all(
        x in unique_products for x in ['CREDIT', 'BUNDLEDDISCOUNT']
    )
    has_refund_and_bundled_discount = all(
        x in unique_products for x in ['REFUND', 'BUNDLEDDISCOUNT']
    )
    has_edpdiscount_and_bundled_discount = all(
        x in unique_products for x in ['EDPDISCOUNT', 'BUNDLEDDISCOUNT']
    )

    if has_refund_and_credit or has_refund_and_discount or \
            has_credit_and_discount or has_credit_and_bundled_discount or \
            has_refund_and_bundled_discount or \
            has_edpdiscount_and_bundled_discount:
        df_summary_table_copy.loc['REFUND'] = 0
        df_summary_table_copy.loc['CREDIT'] = 0
        df_summary_table_copy.loc['EDPDISCOUNT'] = 0
        df_summary_table_copy.loc['BUNDLEDDISCOUNT'] = 0

        if 'REFUND' in unique_products:
            for amount in refund:
                df_summary_table_copy.loc['REFUND', amount] = refund[amount]

        if 'CREDIT' in unique_products:
            for amount in credit:
                df_summary_table_copy.loc['CREDIT', amount] = credit[amount]

        if 'EDPDISCOUNT' in unique_products:
            for amount in discount:
                df_summary_table_copy.loc['EDPDISCOUNT', amount] = discount[
                    amount
                ]

        if 'BUNDLEDDISCOUNT' in unique_products:
            for amount in bundled_discount:
                df_summary_table_copy.loc['BUNDLEDDISCOUNT', amount] = \
                    bundled_discount[amount]

        df_summary_table_copy.loc[
            'Grand Total With Refund/Credit/Discount'
        ] = df_summary_table_copy.loc[
            'Grand Total w/o Refund and Credit and Discount'
        ].add(df_summary_table_copy.loc['REFUND'].add(
            df_summary_table_copy.loc['CREDIT']).add(
            df_summary_table_copy.loc['EDPDISCOUNT']).add(
            df_summary_table_copy.loc['BUNDLEDDISCOUNT']), fill_value=0
        )

        # df_summary_table_copy = df_summary_table_copy.append(df_summary_table_copy1)
        df_summary_table_copy = pd.concat([df_summary_table_copy,savings_plan_credit,df_summary_table_copy1])

        df_summary_table_copy.loc[
            'Net Final Amount'
        ] =df_summary_table_copy.loc[
            'Grand Total With Refund/Credit/Discount'
        ].add(df_summary_table_copy.loc['NetSavingsPlanCost'].add(
            df_summary_table_copy.loc['NetSavingsPlanCost Credits']), fill_value=0
        )

    elif 'CREDIT' in unique_products:
        df_summary_table_copy.loc['CREDIT'] = 0
        for amount in credit:
            df_summary_table_copy.loc['CREDIT', amount] = credit[amount]
            df_summary_table_copy.loc[
                'Grand Total With Refund/Credit/Discount'
            ] = df_summary_table_copy.loc[
                'Grand Total w/o Refund and Credit and Discount'
            ].add(df_summary_table_copy.loc['CREDIT'], fill_value=0)

    elif 'REFUND' in unique_products:
        df_summary_table_copy.loc['REFUND'] = 0
        for amount in refund:
            df_summary_table_copy.loc['REFUND', amount] = refund[amount]
            df_summary_table_copy.loc[
                'Grand Total With Refund/Credit/Discount'
            ] = df_summary_table_copy.loc[
                'Grand Total w/o Refund and Credit and Discount'
            ].add(df_summary_table_copy.loc['REFUND'], fill_value=0)

    elif 'EDPDISCOUNT' in unique_products:
        df_summary_table_copy.loc['EDPDISCOUNT'] = 0
        for amount in discount:
            df_summary_table_copy.loc['EDPDISCOUNT', amount] = discount[
                amount]
            df_summary_table_copy.loc[
                'Grand Total With Refund/Credit/Discount'
            ] = df_summary_table_copy.loc[
                'Grand Total w/o Refund and Credit and Discount'
            ].add(df_summary_table_copy.loc['EDPDISCOUNT'], fill_value=0)

    elif 'BUNDLEDDISCOUNT' in unique_products:
        df_summary_table_copy.loc['BUNDLEDDISCOUNT'] = 0
        for amount in bundled_discount:
            df_summary_table_copy.loc['BUNDLEDDISCOUNT', amount] = \
                bundled_discount[
                    amount]
            df_summary_table_copy.loc[
                'Grand Total With Refund/Credit/Discount'
            ] = df_summary_table_copy.loc[
                'Grand Total w/o Refund and Credit and Discount'
            ].add(df_summary_table_copy.loc['BUNDLEDDISCOUNT'], fill_value=0)

    df_summary_table_copy = pd.concat(
        [df_summary_table_copy, pd.DataFrame(
            df_summary_table_copy.sum(axis=1),
            columns=['Grand Total'])], axis=1
    )

    df_summary_table_copy = df_summary_table_copy.round(7)

    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'generated invoice in {end_time} minutes')

    return df_summary_table_copy, df_split_common, rds_product_dict



# In[33]:


PROD = 501174749948
NON_PROD = 391837235661

SAVINGS_PLAN_CREDITS = 46931.199259
df_summary_table_copy, df_split_common, rds_product_dict = \
        generate_invoice(
            df_rds_product.toPandas(), df_split_common.toPandas(), df_final.toPandas(), df_final_SPC.toPandas(),
            unique_products, False
        )


# In[34]:


import operator
import time

import numpy as np
import pandas as pd
def generate_invoice_category(date_time, df_summary_table_copy,
                              unique_products, rds_product_dict):
    """
    Generate a dataframe that includes cost for Prod, Prod-adhoc and
    Non-prod accounts and finally populates total cost across axis 0 and 1.
    """

    SP_Total = df_summary_table_copy['Grand Total'].iloc[-1]
    ProdNum_check = df_summary_table_copy[['NA-PRD','NA-AD-HOC',]].copy()
    ProdNum_check.drop(index='REFUND', inplace=True)
    ProdNum_check.drop(index='CREDIT', inplace=True)
    ProdNum_check.drop(index='EDPDISCOUNT', inplace=True)
    ProdNum_check.drop(index='BUNDLEDDISCOUNT', inplace=True)
    ProdNum_check.drop(
        index='Grand Total With Refund/Credit/Discount',
        inplace=True
    )
    ProdNum_check.drop(index='NetSavingsPlanCost', inplace=True)
    ProdNum_check.drop(index='NetSavingsPlanCost Credits', inplace=True)
    ProdNum_check.drop(index='Net Final Amount', inplace=True)

    start_time = time.time()

    month_text = date_time.strftime('%B')[0:3]
    year = date_time.strftime('%Y')
    column_name = f'Month of {month_text}-{str(year)}'

    product_list = list(df_summary_table_copy.index.values)
    no_of_prod = len(product_list)
    temp_list1 = [0] * no_of_prod
    temp_list2 = [0] * no_of_prod
    temp_list3 = [0] * no_of_prod

    df1 = pd.DataFrame(
        list(zip(product_list, temp_list1, temp_list2, temp_list3)),
        columns=[column_name, 'Prod', 'Prod - Ad-hoc', 'Non-Prod']
    )

    df1['Prod'] = df1['Prod'].astype(object)
    df1['Prod - Ad-hoc'] = df1['Prod - Ad-hoc'].astype(object)
    df1['Non-Prod'] = df1['Non-Prod'].astype(object)

    prod_values = df_summary_table_copy['NA-PRD'].tolist()
    prod_values_diag = df_summary_table_copy['tcna-ingest-use-1-aws-prd'].tolist()
    prod_values_total=map(operator.add, prod_values, prod_values_diag)
    prod_values_total=list(prod_values_total)
    # prod_values_total= prod_values + prod_values_diag
    tot_values = df_summary_table_copy['Grand Total'].tolist()
    prod_adhoc_values = df_summary_table_copy['NA-AD-HOC'].tolist()
    all_prd_values = map(operator.add, prod_values, prod_adhoc_values)
    all_prd_values=list(all_prd_values)
    map_object = map(operator.sub, tot_values, all_prd_values)
    map_object=list(map_object)
    map_object_diag = map(operator.sub, map_object, prod_values_diag)
    non_prd_values = list(map_object_diag)

    df1.at[:, 'Prod'] = prod_values_total
    df1.at[:, 'Prod - Ad-hoc'] = prod_adhoc_values
    df1.at[:, 'Non-Prod'] = non_prd_values
    df1 = df1.set_index(column_name)

    has_refund_and_credit = all(
        x in unique_products for x in ['REFUND', 'CREDIT']
    )
    has_refund_and_discount = all(
        x in unique_products for x in ['REFUND', 'EDPDISCOUNT']
    )
    has_credit_and_discount = all(
        x in unique_products for x in ['CREDIT', 'EDPDISCOUNT']
    )
    has_credit_and_bundled_discount = all(
        x in unique_products for x in ['CREDIT', 'BUNDLEDDISCOUNT']
    )
    has_refund_and_bundled_discount = all(
        x in unique_products for x in ['REFUND', 'BUNDLEDDISCOUNT']
    )
    has_edpdiscount_and_bundled_discount = all(
        x in unique_products for x in ['EDPDISCOUNT', 'BUNDLEDDISCOUNT']
    )

    if has_refund_and_credit or has_refund_and_discount or \
            has_credit_and_discount or has_credit_and_bundled_discount or \
            has_refund_and_bundled_discount or \
            has_edpdiscount_and_bundled_discount:
        df1.drop(index='REFUND', inplace=True)
        df1.drop(index='CREDIT', inplace=True)
        df1.drop(index='EDPDISCOUNT', inplace=True)
        df1.drop(index='BUNDLEDDISCOUNT', inplace=True)
        df1.drop(index='Grand Total With Refund/Credit/Discount', inplace=True)
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )
        df1.drop(index='NetSavingsPlanCost', inplace=True)
        df1.drop(index='NetSavingsPlanCost Credits', inplace=True)
        df1.drop(index='Net Final Amount', inplace=True)

    elif 'REFUND' in unique_products:
        df1.drop(index='REFUND', inplace=True)
        df1.drop(index='Grand Total With Refund/Credit/Discount', inplace=True)
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )

    elif 'CREDIT' in unique_products:
        df1.drop(index='CREDIT', inplace=True)
        df1.drop(index='Grand Total With Refund/Credit/Discount', inplace=True)
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )

    elif 'EDPDISCOUNT' in unique_products:
        df1.drop(index='EDPDISCOUNT', inplace=True)
        df1.drop(index='Grand Total With Refund/Credit/Discount', inplace=True)
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )

    elif 'BUNDLEDDISCOUNT' in unique_products:
        df1.drop(index='BUNDLEDDISCOUNT', inplace=True)
        df1.drop(index='Grand Total With Refund/Credit/Discount', inplace=True)
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )

    else:
        df1.drop(
            index='Grand Total w/o Refund and Credit and Discount',
            inplace=True
        )

    prod_list = df1['Prod'].tolist()
    prod_adhoc_list = df1['Prod - Ad-hoc'].tolist()
    non_prd_values = df1['Non-Prod'].tolist()

    df2 = pd.concat(
        [df1, pd.DataFrame(df1.sum(axis=0), columns=['Grand Total']).T]
    )
    curr_total = df2['Prod'].iloc[-1] + df2['Prod - Ad-hoc'].iloc[-1] + df2['Non-Prod'].iloc[-1]

    # Curr percentage total
    prod_per = df2['Prod'].iloc[-1] / curr_total
    adhoc_per = df2['Prod - Ad-hoc'].iloc[-1] / curr_total
    nonprod_per = df2['Non-Prod'].iloc[-1] / curr_total

    prod_per = prod_per * SP_Total
    adhoc_per = adhoc_per * SP_Total
    nonprod_per = nonprod_per * SP_Total

    my_temp_list_prod = [i / df2['Prod'].iloc[-1] for i in prod_list]
    my_new_list_prod = [i * prod_per for i in my_temp_list_prod]

    my_temp_list_adhoc = [i / df2['Prod - Ad-hoc'].iloc[-1] for i in prod_adhoc_list]
    my_new_list_adhoc = [i * adhoc_per for i in my_temp_list_adhoc]

    my_temp_list_nonprod = [i / df2['Non-Prod'].iloc[-1] for i in non_prd_values]
    my_new_list_nonprod = [i * nonprod_per for i in my_temp_list_nonprod]

    df1['Prod'] = my_new_list_prod
    df1['Prod - Ad-hoc'] = my_new_list_adhoc
    df1['Non-Prod'] = my_new_list_nonprod


    adhoc_non_prd_values = np.add(df1['Prod - Ad-hoc'], df1['Non-Prod'])
    vin_count_values = [rds_product_dict.get(x, 0) for x in
                        df1.index.tolist()]
    prod_cost_vin = [round(p / float(v), 6) if v else 0 for p, v in
                     zip(prod_values, vin_count_values)]

    df1 = pd.concat(
        [df1, pd.DataFrame(df1.sum(axis=1), columns=['Grand Total'])],
        axis=1
    )

    df1['Prod Ad-Hoc + Non-Prod'] = adhoc_non_prd_values
    df1['VIN Count'] = vin_count_values
    df1['Prod Cost/VIN'] = prod_cost_vin

    all_cost_vin = [
        round(p / float(v), 6) if v else 0 for p, v in
        zip(df1["Grand Total"].tolist(), vin_count_values)
    ]

    df1['All Env Cost/VIN'] = all_cost_vin
    df1 = pd.concat(
        [df1, pd.DataFrame(df1.sum(axis = 0), columns=['Grand Total']).T]
    )
    df1.index.names = [column_name]

    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'generated invoice category in {end_time} minutes')

    return df1


# In[35]:


df1 = generate_invoice_category(
        date_time, df_summary_table_copy, unique_products,
        rds_product_dict
    )


# In[36]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window

def business_service_cost(accounts, df_aws_services, df_split_common):

    
    accounts_df = spark.createDataFrame([(int(k), v) for k, v in accounts.items()], ['Account_ID', 'Account_Alias'])
    df_aws_services = df_aws_services.join(F.broadcast(accounts_df), df_aws_services[ACCOUNT_ID] == accounts_df['Account_ID'], 'left_outer') \
        .withColumn('Account', col('Account_Alias')).drop('Account_ID', 'Account_Alias')
    # Filter and group df_aws_services for COMMON product
    df_common = df_aws_services.filter(col('Product') == 'COMMON') \
        .groupBy(ACCOUNT_ID, 'Account', 'Product', PRODUCT_ID) \
        .agg(spark_sum(COST).alias('Total_Cost'))
    # Calculate percentage for each account
    window_spec = Window.partitionBy('Account').orderBy('Account')
    df_common = df_common.withColumn('Total_Account_Cost', spark_sum('Total_Cost').over(window_spec))
    df_common = df_common.withColumn('Percentage', col('Total_Cost') * 100 / col('Total_Account_Cost'))
    # Group df_common by Account and Product, summing up costs
    df_common_grouped = df_common.groupBy('Account', 'Product').agg(spark_sum('Total_Cost').alias(COST))
    df_split_common_spark = spark.createDataFrame(df_split_common.reset_index())
    unpivot_Expr = f"""{"stack({}, {})".format(len(df_split_common_spark.columns)-1, ", ".join(["'{}', `{}`".format(col, col) for col in df_split_common_spark.columns if col != 'Product']))} as (Account,value)"""
    df_split_common_stack = df_split_common_spark.select("Product", expr(unpivot_Expr)).withColumnRenamed('Product','variable')
    df_split_common_stack = df_split_common_stack.join(df_common_grouped, "Account")
    df_split_common_stack = df_split_common_stack.withColumn(COST, col(COST) * col("value") / 100)
    df_split_common_stack = df_split_common_stack.orderBy("Account").drop("Product", "Value").withColumnRenamed('variable','Product')
    df_split_common_stack = df_split_common_stack.filter(col(COST) != 0.0).na.drop()
    df_split_common_stack = df_split_common_stack.withColumn("Product", expr("CONCAT('COMMON-', Product)"))
    df_merged = df_common.drop(COST).drop('Total_Cost').drop('Total_Account_Cost').drop('Product').join(df_split_common_stack, "Account", "left")
    df_merged = df_merged.withColumn(COST ,col(COST) * col("Percentage") / 100).drop("Percentage")

    df_aws_services1 = df_aws_services.filter(col("Product") != "COMMON").select(ACCOUNT_ID, 'Account', 'Product',PRODUCT_ID, COST)
    df_merged = df_merged.select(ACCOUNT_ID, 'Account', 'Product',PRODUCT_ID, COST)
    # Concatenate DataFrames
    df_merged_final = df_merged.union(df_aws_services1)

    # Group by 'Account', 'Product', and 'Product ID' to calculate total cost
    df_business_service_cost = df_merged_final.groupBy("Account", "Product", PRODUCT_ID).agg(spark_sum(COST).alias(COST))

    df_business_service_cost = df_business_service_cost.groupBy("Account", "Product") \
        .pivot(PRODUCT_ID) \
        .sum(COST) \
        .fillna(0)
    df_business_service_cost = df_business_service_cost.withColumn("Grand Total", F.expr('+'.join([col for col in df_business_service_cost.columns if col not in  ['Product','Account']])))
    
    return df_business_service_cost


# In[37]:


df_business_service_cost = business_service_cost(accounts, df_aws_services, df_split_common)


# In[38]:


from pyspark.sql.functions import sum as spark_sum, when, col
from pyspark.sql import functions as F

def adhoc_cost(df1, df_raw, df_bucketing):

    df_adhoc = df1.loc['ADHOC'].tolist()
    df_rnd = df1.loc['TMNA-RND-DATA'].tolist()
    adhoc_total_cost = df_adhoc[0] + df_adhoc[2] + df_rnd[0] + df_rnd[1] + df_rnd[2]
    
    

    # List of accounts
    list_of_accounts = [501174749948, 391837235661]

    # Filter raw data based on account list and usage type
    df_raw_adhocacc = df_raw.filter(col("line_item_usage_account_id").isin(list_of_accounts)) \
                                  .filter(col("line_item_line_item_type") == "Usage")
    
    # Rename column in df_bucketing_spark
    df_bucketing = df_bucketing.withColumnRenamed("ApplicationTag", "resource_tags_user_application")

    # Create an empty DataFrame
    schema = StructType([
        StructField("resource_tags_user_application", StringType(), True),
        StructField("Bucket", StringType(), True)
    ])

    # Create an empty DataFrame with the specified schema
    empty_df = spark.createDataFrame([(None, 'NA')], schema)
    df_bucketing = df_bucketing.select('resource_tags_user_application', 'Bucket').union(empty_df)

    # Join raw data with bucketing data
    df_usage = df_raw_adhocacc.join(F.broadcast(df_bucketing), df_raw_adhocacc["resource_tags_user_application"].eqNullSafe(df_bucketing["resource_tags_user_application"]), "left").drop(df_bucketing["resource_tags_user_application"])
    # Filter df_usage
    df_usage = df_usage.filter((col("Bucket").isin(["TMNA-RND-DATA", "COMMON", '', "NA"])) &
                                   ((~F.col("resource_tags_user_application").contains("HACKATHON")) | F.col("resource_tags_user_application").isNull()))
    df_usage = df_usage.withColumn("resource_tags_user_application", F.regexp_replace(col('resource_tags_user_application'), "^\\s*$", "MOBILITY-RND-INFRA"))
    df_usage = df_usage.withColumn("resource_tags_user_application", 
                                        when(F.col("resource_tags_user_application").isNull(), "MOBILITY-RND-INFRA")
                                        .otherwise(F.col("resource_tags_user_application")))
        # Group by and sum the costs
    total_prod = df_usage.groupBy("resource_tags_user_adhoc_request_id", 
                                       "resource_tags_user_application", "Bucket") \
                             .agg(spark_sum("tot_line_item_unblended_cost").alias("tot_line_item_unblended_cost"))

    # Calculate total cost and percentage discounts
    total = total_prod.groupBy().agg(spark_sum("tot_line_item_unblended_cost").alias("total_cost")).head()[0]
    total_prod = total_prod.withColumn("tot_line_item_unblended_cost1", (F.col("tot_line_item_unblended_cost") * lit(adhoc_total_cost)) / lit(total))

    print(total)
    
    return total_prod


# In[39]:


df_adhoc_cost_prod = adhoc_cost(df1, df_raw, df_bucketing)


# In[40]:


import pandas as pd
import numpy as np
from constants import *
import boto3
import time

default_args = {
    "region": "us-east-1",
    "workgroup": "Billing",
    'bucket_name_prefix': f'us-east-1-ops-metrics-',
    'script_bucket_prefix': f'us-east-1-ops-scripts-'
}

def tagaws_service(month, year, df_adhoc_cost_prod):

    crop = df_adhoc_cost_prod.filter(col('resource_tags_user_adhoc_request_id').isNull()).select('resource_tags_user_application').limit(5).toPandas()
    crop2 = crop['resource_tags_user_application']
    print(crop2)
    crop3 = crop2.tolist()
    crop4 = (','.join("'{0}'".format(x) for x in crop3))
    df_lineitem_bucketing = run_query(month, year,crop4)
    return df_lineitem_bucketing

def run_query(month, year, crop4):
    query_month = str(month)
    query_year = str(year)
    months = month.lstrip('0')
    month_for_query = "'" + str(months) + "'"
    year_for_query = "'" + str(year) + "'"
    account_id = boto3.client('sts').get_caller_identity()['Account']
    s3_client = boto3.client('s3', region_name=default_args['region'])

    athena_output_path = 's3://us-east-1-ops-metrics-709639782912/billing/'+query_year+'/'+query_month+'/output/'
    athena_result_path = f'billing/{year}/{month}/output/'

    bucket_name = default_args.pop('bucket_name_prefix') + account_id
    script_bucket = default_args.pop('script_bucket_prefix') + account_id

    select_query = "select distinct year, month, line_item_usage_account_id,line_item_product_code,\
        UPPER(resource_tags_user_application) from billing.billing_parquet_billing \
        where year = " + year_for_query + " and month =" + month_for_query + "and line_item_usage_account_id in ('501174749948', '391837235661')\
        and line_item_line_item_type = 'Usage' and UPPER(resource_tags_user_application) in ("+crop4+")"
    athena_client = boto3.client('athena', region_name=default_args['region'])
    query_response = athena_client.start_query_execution(
        QueryString=select_query,
        ResultConfiguration={'OutputLocation': athena_output_path},
        WorkGroup=default_args["workgroup"]
    )
    line_item_type_file = query_response['QueryExecutionId']+".csv".strip()
    time.sleep(60)

    s3_client.download_file(bucket_name,athena_result_path+line_item_type_file,line_item_type_file)
    df_lineitem_tag = pd.read_csv(
        line_item_type_file, dtype=str,
        keep_default_na=False
    )
    df_lineitem = df_lineitem_tag[['line_item_product_code','_col4']].head()
    df_lineitem_bucketing = df_lineitem.rename(columns={'_col4': 'Tag'})

    return df_lineitem_bucketing


# In[41]:


df_tagaws_service = tagaws_service(month, year, df_adhoc_cost_prod)


# In[42]:


import time

import boto3
from pandas import ExcelWriter




def save_dataframes_to_excel(month, year, df1, df_cost_breakdown, df_accounts,
                             df_bucketing, df_cost_breakdown_resource,
                             df_cost_breakdown_product, df_final,df_tag_cost_split,
                             df_split_common, df_split_eks, df_summary_table,
                             df_summary_table_copy, file_id,
                             service_cost_breakdown_df, df_fanout_split,
                             df_fanout, df_business_service_cost, df_adhoc_cost_prod, df_tagaws_service, adhoc=False):
    """
    Saves all the dataframes into output Excel file with different sheets.
    """

    start_time = time.time()

    output_filename = f'CostBreakdown_{file_id}.xlsx'

    writer = ExcelWriter('./' + output_filename)
    df_accounts.to_excel(writer, 'Accounts', index=False)
    df_bucketing.to_excel(writer, 'Bucketing', index=False)
    df_tag_cost_split.to_excel(writer, 'Tag_Split_Percent', index=True)
    df_split_common.to_excel(writer, 'Common_Split_Percent', index=True)
    df_split_eks.to_excel(writer, 'EKS_Split_Percent', index=True)
    df_cost_breakdown.to_excel(writer, 'Cost_Breakdown', index=False)
    df_cost_breakdown_resource.to_excel(
        writer, 'Cost_Breakdown_ResID', index=False
    )
    df_cost_breakdown_product.to_excel(
        writer, 'Cost_Breakdown_Prod_ID', index=False
    )
    service_cost_breakdown_df.to_excel(
        writer, 'Cost_Breakdown_Services', index=False
    )
    df_fanout.to_excel(writer, sheet_name='Fanout cost split', index=False)
    df_fanout_split.to_excel(writer, sheet_name='Fanout cost split',
                             startrow=10, index=False)
    df_final.to_excel(writer, 'Cost_Breakdown_Final', index=False)
    df_business_service_cost.to_excel(writer, 'Business_service_cost',
                                      index=True)
    df_adhoc_cost_prod.to_excel(writer, 'Cost Prod Non Prod',
                                       index=True)
    df_tagaws_service.to_excel(writer, 'Tag AWS Service',
                               index=True)
    df_summary_table.to_excel(writer, 'Summary_Report')
    df_summary_table_copy.to_excel(writer, 'Invoice', index=True)
    df1.to_excel(writer, sheet_name='Invoice', startrow=70, index=True)

    writer.save()

    if not adhoc:
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            default_config['local_path'] + output_filename,
            default_config['bucket_name'],
            f'billing/{year}/{month}/output/v2/{output_filename}'
        )

    end_time = round(((time.time() - start_time) / 60), 4)
    print(f'saved output to excel in {end_time} minutes')

    print('============ Success ============')
    print(f'The output file name is: {output_filename}')


# In[43]:


file_id = '5bcbbd01-0fcf-4138-b079-5456c4d38245_latest.csv'
file_id_without_extension = file_id.replace('.csv', '')
save_dataframes_to_excel(
        month, year, df1, df_cost_breakdown.toPandas(), df_accounts.toPandas(), df_bucketing.toPandas(),
        df_cost_breakdown_resource.toPandas(), df_cost_breakdown_product.toPandas(), df_final.toPandas(),
        df_tag_cost_split.toPandas(),df_split_common, df_split_eks.toPandas(), df_summary_table.toPandas(), df_summary_table_copy,
        file_id_without_extension, service_cost_breakdown_df.toPandas(),
        df_fanout_split.toPandas(), df_fanout.toPandas(), df_business_service_cost.toPandas(), df_adhoc_cost_prod.toPandas(), df_tagaws_service, True
    )


# In[ ]:




