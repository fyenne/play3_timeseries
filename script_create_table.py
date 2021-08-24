
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

"""
三个部分, 第一部分创建表.
"""

names = ['ods_cn_bose'
,'ods_cn_apple_sz'
,'ods_cn_apple_sh'
,'ods_cn_costacoffee'
,'ods_cn_diadora'
,'ods_cn_ferrero'
,'ods_cn_fuji'
,'ods_cn_hd'
,'ods_cn_hp_ljb'
,'ods_cn_hpi'
,'ods_cn_hualiancosta'
,'ods_cn_jiq'
,'ods_cn_kone'
,'ods_cn_michelin'
,'ods_cn_razer'
,'ods_cn_squibb'
,'ods_cn_vzug'
,'ods_cn_zebra'
,'ods_dbo'
,'ods_hk_abbott'
,'ods_hk_revlon'
,'ods_hk_fredperry']

cc = ['CREATE EXTERNAL TABLE `' + i +
"""
.receipt_header_df`(
`internal_receipt_num` double COMMENT '4',
`warehouse` string COMMENT '4',
`company` string COMMENT '4',
`receipt_id` string COMMENT '4',
`receipt_id_type` string COMMENT '4',
`receipt_type` string COMMENT '4',
`receipt_date` string COMMENT '4',
`close_date` string COMMENT '4',
`source_id` string COMMENT '4',
`source_name` string COMMENT '4',
`source_address1` string COMMENT '4',
`source_address2` string COMMENT '4',
`source_address3` string COMMENT '4',
`source_city` string COMMENT '4',
`source_state` string COMMENT '4',
`source_postal_code` string COMMENT '4',
`source_country` string COMMENT '4',
`source_attention_to` string COMMENT '4',
`source_phone_num` string COMMENT '4',
`source_fax_num` string COMMENT '4',
`source_email_address` string COMMENT '4',
`priority` double COMMENT '4',
`carrier` string COMMENT '4',
`carrier_service` string COMMENT '4',
`erp_order_num` string COMMENT '4',
`erp_order_type` string COMMENT '4',
`bol_num_alpha` string COMMENT '4',
`license_plate_id` string COMMENT '4',
`packing_list_id` string COMMENT '4',
`pro_num_alpha` string COMMENT '4',
`trailer_id` string COMMENT '4',
`seal_id` string COMMENT '4',
`total_containers` double COMMENT '4',
`total_lines` double COMMENT '4',
`total_qty` double COMMENT '4',
`quantity_um` string COMMENT '4',
`total_weight` double COMMENT '4',
`weight_um` string COMMENT '4',
`total_volume` double COMMENT '4',
`volume_um` string COMMENT '4',
`total_value` double COMMENT '4',
`leading_sts` double COMMENT '4',
`leading_sts_date` string COMMENT '4',
`leading_sts_failed` string COMMENT '4',
`trailing_sts` double COMMENT '4',
`trailing_sts_date` string COMMENT '4',
`trailing_sts_failed` string COMMENT '4',
`user_def1` string COMMENT '4',
`user_def2` string COMMENT '4',
`user_def3` string COMMENT '4',
`user_def4` string COMMENT '4',
`user_def5` string COMMENT '4',
`user_def6` string COMMENT '4',
`user_def7` double COMMENT '4',
`user_def8` double COMMENT '4',
`user_stamp` string COMMENT '4',
`process_stamp` string COMMENT '4',
`date_time_stamp` string COMMENT '4',
`manually_entered` string COMMENT '4',
`ship_from` string COMMENT '4',
`ship_from_address1` string COMMENT '4',
`ship_from_address2` string COMMENT '4',
`ship_from_address3` string COMMENT '4',
`ship_from_city` string COMMENT '4',
`ship_from_state` string COMMENT '4',
`ship_from_country` string COMMENT '4',
`ship_from_postal_code` string COMMENT '4',
`ship_from_name` string COMMENT '4',
`ship_from_attention_to` string COMMENT '4',
`ship_from_email_address` string COMMENT '4',
`ship_from_phone_num` string COMMENT '4',
`ship_from_fax_num` string COMMENT '4',
`scheduled_date_time` string COMMENT '4',
`arrived_date_time` string COMMENT '4',
`start_unitize_date_time` string COMMENT '4',
`end_unitize_date_time` string COMMENT '4',
`interface_record_id` string COMMENT '4',
`creation_process_stamp` string COMMENT '4',
`creation_date_time_stamp` string COMMENT '4',
`trailer_yard_status_id` double COMMENT '4',
`upload_interface_batch` string COMMENT '4',
`in_pre_checkin_ctr_creation` string COMMENT '4',
`data_source` string COMMENT '数据源',
`is_business_valid` bigint,
`src_inc_day` string comment '数据来源的inc_day')
COMMENT 'RECEIPT_HEADER'
PARTITIONED BY (
`inc_day` string COMMENT '增量日期')
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
"""
for i in names]

cc2 = [cc[i].replace('\n', '') for i in cc]

[spark.sql(i) for i in cc2]

"""
第二部分, 将archive 数据写入ods层的--分区
"""
['insert overwrite table ' + i + 
"""
.receipt_header
partition (inc_day = '19950529')
Select 
internal_receipt_num
,warehouse
,company
,receipt_id
,receipt_id_type
,receipt_type
,receipt_date
,close_date
,source_id
,source_name
,source_address1
,source_address2
,source_address3
,source_city
,source_state
,source_postal_code
,source_country
,source_attention_to
,source_phone_num
,source_fax_num
,source_email_address
,priority
,carrier
,carrier_service
,erp_order_num
,erp_order_type
,bol_num_alpha
,license_plate_id
,packing_list_id
,pro_num_alpha
,trailer_id
,seal_id
,total_containers
,total_lines
,total_qty
,quantity_um
,total_weight
,weight_um
,total_volume
,volume_um
,total_value
,leading_sts
,leading_sts_date
,leading_sts_failed
,trailing_sts
,trailing_sts_date
,trailing_sts_failed
,user_def1
,user_def2
,user_def3
,user_def4
,user_def5
,user_def6
,user_def7
,user_def8
,user_stamp
,process_stamp
,date_time_stamp
,manually_entered
,ship_from
,ship_from_address1
,ship_from_address2
,ship_from_address3
,ship_from_city
,ship_from_state
,ship_from_country
,ship_from_postal_code
,ship_from_name
,ship_from_attention_to
,ship_from_email_address
,ship_from_phone_num
,ship_from_fax_num
,scheduled_date_time
,arrived_date_time
,start_unitize_date_time
,end_unitize_date_time
,interface_record_id
,creation_process_stamp
,creation_date_time_stamp
,trailer_yard_status_id
,upload_interface_batch
,in_pre_checkin_ctr_creation
,data_source
,is_business_valid
,inc_day

from
"""
 + i + '.ar_receipt_header;'





"""
第三部分,  drop duplicates and insert overwrite. 
"""
dd = ['insert overwrite table ' + i + 
"""
.receipt_header_df
partition (inc_day = '20210823')

Select 
internal_receipt_num
,warehouse
,company
,receipt_id
,receipt_id_type
,receipt_type
,receipt_date
,close_date
,source_id
,source_name
,source_address1
,source_address2
,source_address3
,source_city
,source_state
,source_postal_code
,source_country
,source_attention_to
,source_phone_num
,source_fax_num
,source_email_address
,priority
,carrier
,carrier_service
,erp_order_num
,erp_order_type
,bol_num_alpha
,license_plate_id
,packing_list_id
,pro_num_alpha
,trailer_id
,seal_id
,total_containers
,total_lines
,total_qty
,quantity_um
,total_weight
,weight_um
,total_volume
,volume_um
,total_value
,leading_sts
,leading_sts_date
,leading_sts_failed
,trailing_sts
,trailing_sts_date
,trailing_sts_failed
,user_def1
,user_def2
,user_def3
,user_def4
,user_def5
,user_def6
,user_def7
,user_def8
,user_stamp
,process_stamp
,date_time_stamp
,manually_entered
,ship_from
,ship_from_address1
,ship_from_address2
,ship_from_address3
,ship_from_city
,ship_from_state
,ship_from_country
,ship_from_postal_code
,ship_from_name
,ship_from_attention_to
,ship_from_email_address
,ship_from_phone_num
,ship_from_fax_num
,scheduled_date_time
,arrived_date_time
,start_unitize_date_time
,end_unitize_date_time
,interface_record_id
,creation_process_stamp
,creation_date_time_stamp
,trailer_yard_status_id
,upload_interface_batch
,in_pre_checkin_ctr_creation
,data_source
,is_business_valid
,inc_day as src_inc_day
from 
(
Select 
internal_receipt_num
,warehouse
,company
,receipt_id
,receipt_id_type
,receipt_type
,receipt_date
,close_date
,source_id
,source_name
,source_address1
,source_address2
,source_address3
,source_city
,source_state
,source_postal_code
,source_country
,source_attention_to
,source_phone_num
,source_fax_num
,source_email_address
,priority
,carrier
,carrier_service
,erp_order_num
,erp_order_type
,bol_num_alpha
,license_plate_id
,packing_list_id
,pro_num_alpha
,trailer_id
,seal_id
,total_containers
,total_lines
,total_qty
,quantity_um
,total_weight
,weight_um
,total_volume
,volume_um
,total_value
,leading_sts
,leading_sts_date
,leading_sts_failed
,trailing_sts
,trailing_sts_date
,trailing_sts_failed
,user_def1
,user_def2
,user_def3
,user_def4
,user_def5
,user_def6
,user_def7
,user_def8
,user_stamp
,process_stamp
,date_time_stamp
,manually_entered
,ship_from
,ship_from_address1
,ship_from_address2
,ship_from_address3
,ship_from_city
,ship_from_state
,ship_from_country
,ship_from_postal_code
,ship_from_name
,ship_from_attention_to
,ship_from_email_address
,ship_from_phone_num
,ship_from_fax_num
,scheduled_date_time
,arrived_date_time
,start_unitize_date_time
,end_unitize_date_time
,interface_record_id
,creation_process_stamp
,creation_date_time_stamp
,trailer_yard_status_id
,upload_interface_batch
,in_pre_checkin_ctr_creation
,data_source
,is_business_valid
,inc_day
,row_number() over(partition by receipt_id, internal_receipt_num order by inc_day desc) as rn
from
"""
 + i + '.receipt_header_df) as a where rn = 1;'

]


dd2 = [dd[i].replace('\n', '') for i in dd]