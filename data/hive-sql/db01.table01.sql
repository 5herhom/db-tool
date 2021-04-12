Create table `db01.table01`(
  `id` string COMMENT '备注id',
    `age` decimal(13,2) COMMENT '年龄',
    `num` bigint,
    `name01` string,`name03` string ,`mappp` map<string,string> comment '映射4',`date_aa` timestamp)
partitioned by (y string comment "123123",m string ,d string ,type string)
COMMENT "我是表01"
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
LOCATION
    'asdasdasd'