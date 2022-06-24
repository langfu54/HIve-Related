scope:本文档为流量数据指标的MR离线实现到改为flink实现的说明

#### **1.需求指标**

![img](file:///C:/Users/fulang/Documents/WXWork/1688854756239296/Cache/Image/2021-11/企业微信截图_16370472811366.png)



改进离线流量数据的计算为实时，主要包括以下指标

~~~sql
1.UV    

```sql
sqluv = '''
select mch_id,`date`,'uv-spark' as type,count(distinct cookieid) as cnt from (
select cookieid,`date`, otherkvmap['mch_id'] as mch_id  from ylog.tbl_flow 
where `date`='{{ ds }}' and topic='shop2cn' and action_type = 'show') as t
where mch_id >0 group by mch_id,`date`
'''

2.PV

```sql
select mch_id,`date`,'pv-spark' as type,count(cookieid) as cnt from (
select cookieid,`date`, otherkvmap['mch_id'] as mch_id  from ylog.tbl_flow  
where `date`='2021-11-24'  and topic='shop2cn'  and action_type = 'show' ) as t
where mch_id >0 group by mch_id,`date`
```

3.CART_ADD  

```sql
select mch_id,`date`,'addcart-spark' as type,count(distinct cookieid) as cnt from (
select cookieid,`date`, otherkvmap['mch_id'] as mch_id  from ylog.tbl_flow 
where `date`='2021-11-24' and topic='shop2cn' and page_type ='product' and sub_module_name = 'add_to_shoppingcart' and action_type = 'click') as t
where mch_id >0 group by mch_id,`date`
```

4.PUV

```sql
select mch_id,`date`,'puv-spark' as type,count(distinct cookieid) as cnt from (
select cookieid,`date`, otherkvmap['mch_id'] as mch_id  from ylog.tbl_flow
where `date`='2021-11-24' and topic='shop2cn' and page_type = 'product' and action_type = 'show') as t
where mch_id >0 group by mch_id,`date`
```
5.新老用户校验


~~~

#### 2.离线处理

 客户端上报->流量站点->kafka->flume定时落地文件>-MR任务清洗落地Hive表

![image-20211026173654208](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211026173654208.png)



```
source code:  git@172.16.100.22:dw/mr-jobkit.git

prj: mr-jobkit 

account:  fulang 87654321
```



##### 2.1 离线处理主题资源信息

###### 2.1.1 Kafka 集群信息

​	kafka 版本：**kafka_2.11-1.1.9**

​	流量数据主机，主题： 

```shell
[root@linux-10-12-32-47 bin]# /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka
ATLAS_ENTITIES
ATLAS_HOOK
__consumer_offsets
abtest
buyerapp
default
example
flink-kafka
monitormetrics
redis_monitor
redis_monitor_1
search_rec
sellerapp
shop2cn
test
yid-category-trigger
yid-trigger
yidserver
```



###### 2.1.2 离线 流量数据格式，内容

![image-20211026182542345](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211026182542345.png)

```
flume日志数据：

[http://hue.ymatou.cn/filebrowser/download=/data/rawdata/flume/events/shop2cn/202](http://hue.ymatou.cn/filebrowser/download=/data/rawdata/flume/events/shop2cn/2021/10/29/14/event.1635487200398.log)


http://hue.ymatou.cn/filebrowser/view=/data/rawdata/flume/events/shop2cn/2021/12/11/23/event.1639236606938.log
```

离线表ylog.table_flow字段：

| 0    | time              | string             | 时间戳                                                       |
| ---- | ----------------- | ------------------ | ------------------------------------------------------------ |
| 1    | cookieid          | string             | cookieid                                                     |
| 2    | osid              | string             | 系统id：ios=1，android=2，pc=3                               |
| 3    | userid            | string             | 用户id（若登录）                                             |
| 4    | useragent         | string             | useragent                                                    |
| 5    | os                | string             | 操作系统                                                     |
| 6    | appid             | string             | appid：买家=1，买手=2                                        |
| 7    | appversion        | string             | app版本号                                                    |
| 8    | url               | string             | 页面url （网页http://)                                       |
| 9    | pageviewid        | string             | 页面id                                                       |
| 10   | refferpvid        | string             | 上一页面的id                                                 |
| 11   | module_name       | string             | 模块名称                                                     |
| 12   | sub_module_name   | string             | 子模块名称                                                   |
| 13   | module_index      | string             | 模块位置--    部分有                                         |
| 14   | module_page       | string             | 模块所在页--没有，表中为空字段                               |
| 15   | ip                | string             | 设备的公网ip地址                                             |
| 16   | **page_type**     | string             | 页面类型 -- **有，pagetype**                                 |
| 17   | longitude         | string             | 经度 （暂无）-- 没有，表中为空字段                           |
| 18   | latitude          | string             | 纬度 （暂无） ****没有**，**表中为空字段****                 |
| 19   | wifiid            | string             | wifiid   ****没有**，**表中为空字段****                      |
| 20   | cellid            | string             | 手机基站id   没有，表中为空字段                              |
| 21   | reffer            | string             | 上一页面的url 没有，表中部分有值                             |
| 22   | schema            | string             | --   没有，表中为空字段                                      |
| 23   | source            | string             | -- **********没有**，表中为空字段                            |
| 24   | imei              | string             | android设备号 ****没有****，表中为空字段                     |
| 25   | mac               | string             | mac地址 没有，表中为空字段                                   |
| 26   | idfa              | string             | iphone设备号                                                 |
| 27   | sellerid          | string             | 卖家id 没有，表中为空字段                                    |
| 28   | liveid            | string             | 直播id                                                       |
| 29   | productid         | string             | 商品id（int，暂时没有） ****表中为空****                     |
| 30   | sproductid        | string             | 商品id（string）                                             |
| 31   | showid            | string             | --  表中为空                                                 |
| 32   | orderid           | string             | 订单id 表中为空                                              |
| 33   | payid             | string             | --表中为空                                                   |
| 34   | imgid             | string             | --                                                           |
| 35   | msgid             | string             | --                                                           |
| 36   | show_commid       | string             | --                                                           |
| 37   | content_commid    | string             | --                                                           |
| 39   | banner_id         | string             | --                                                           |
| 38   | product_commid    | string             | --                                                           |
| 40   | topic_id          | string             | --                                                           |
| 41   | topic_commid      | string             | --11                                                         |
| 42   | **categoryid**    | string             | --  部分有值                                                 |
| 43   | **brandid**       | string             | ----  部分有值                                               |
| 44   | **action_type**   | string             | **操作类型（show、click...)  -- action**                     |
| 45   | action_param      | string             | 操作参数（target:..)会逐渐废弃                               |
| 46   | target            | string             | 操作对象                                                     |
| 47   | keyword           | string             | 搜索关键词                                                   |
| 48   | area              | string             | --                                                           |
| 49   | module_end        | string             | --                                                           |
| 50   | module_end_index  | string             | --                                                           |
| 51   | mode              | string             |                                                              |
| 52   | origin            | string             | log来源（front/server)                                       |
| 53   | yid               | string             | 用户唯一标识id （tbl_flow暂时不可用）                        |
| 54   | **ydeviceid**     | string             | 设备id（ios=idfa,android=imei)   **无字段，表有**            |
| 55   | **sessionid**     | string             | 会话id（按45min切分）  **无字段，表有**                      |
| 56   | **isnewcustomer** | string             | 新客标记 (tbl_session)  **查REDIS**                          |
| 57   | **datetime**      | string             | **log时间（从time字段转化来）**                              |
| 58   | logtype           | string             | log类型（logtype=event/page/wifi， server=null/''） 平时分析主要用event |
| 59   | mobile_operator   | string             | 手机网络（移动/联通)                                         |
| 60   | os_version        | string             | 操作系统版本                                                 |
| 61   | pagestatus        | string             | 页面状态（pagestart/pageend)                                 |
| 62   | pagestarttime     | string             | 页面开始时间                                                 |
| 63   | **pageendtime**   | string             | 页面结束时间  **为0表示未结束，ts为结束时间**                |
| 64   | device_model      | string             | 设备                                                         |
| 65   | refurl            | string             | 上一页面的url（暂不用）                                      |
| 66   | refpagetype       | string             | 上一页面的页面类型                                           |
| 67   | **otherkvmap**    | map<string,string> | 额外字段： otherkvmap['key']  **那些字段**                   |
| 68   | note_id           | string             | 笔记id                                                       |
| 69   | note_user_id      | string             | 笔记用户id                                                   |
| 70   | tag_id            | string             | 笔记tagid                                                    |
| 71   | tag_type          | string             | 笔记tag类型                                                  |
| 72   | sys_push_message  | string             | 系统推送消息                                                 |
| 73   | date              | string             | 分区列：日期（tbl_session=yyyymmdd，tbl_flow=yyyy-mm-dd      |
| 74   | hour              | string             | 分区列：小时（tbl_flow=HH)                                   |
| 75   | topic             | string             | 分区列：打点来源（buyerapp=买家app/default=其他（主要是pc端流量） |

raw data格式：

```
kafkatopic=shop2cnip=117.136.127.200forwardedip=117.136.127.200, 127.0.0.1pagetype=searchrefpagetype=home_pagemch_eeid=0pagestarttime=1625068798209device_model=iPhone 11<iPhone12,1>packageid=wechatuseragent=70iOS====14.3==d9bd05b0-5884-111e-0b3d-466c47c524a3============================6.2.38wechat==========pagestatus=pagestartosid=1hostversion=8.0.7mch_id=500007068userid=500385670module_index=12logtype=eventsdkversion=2.18.0appname=shop2cnyid=d62a2505-fed0-46ac-9735-6302d8655f91cookieid=210f0d5448d6636523ec97b0aa27050daction=scrollkeyword=资生堂cell_type=4grootpage=0pageviewid=9E2565BE-F897-F486-F28F-4746010C8B53os=iOSos_version=14.3idfa=210f0d5448d6636523ec97b0aa27050dpackagename=shop2cn.miniappappversion=6.2.38url=searchrecentpagelist=start_app:CF43EABA-5D50-1949-0670-2153486DCFC2,home_page:C2672F12-F191-A136-6D45-23217BEB2D39,search:9E2565BE-F897-F486-F28F-4746010C8B53user_id=500385670sub_module_name=productappid=70pageendtime=0module_name=productlistsproductid=p6238575refurl=home_pagetime=1625068798248mch_sid=500385670refferpvid=C2672F12-F191-A136-6D45-23217BEB2D39
```

```
kafkatopic=shop2cn
ip=117.136.127.200
forwardedip=117.136.127.200, 127.0.0.1
pagetype=search
refpagetype=home_page
mch_eeid=0
pagestarttime=1625068798209
device_model=iPhone 11<iPhone12,1>
packageid=wechat
useragent=70iOS====14.3==d9bd05b0-5884-111e-0b3d-466c47c524a3============================6.2.38wechat==========
pagestatus=pagestart
osid=1
hostversion=8.0.7
mch_id=500007068
userid=500385670
module_index=16
logtype=event
sdkversion=2.18.0
appname=shop2cn
yid=d62a2505-fed0-46ac-9735-6302d8655f91
cookieid=210f0d5448d6636523ec97b0aa27050d
action=scroll
keyword=资生堂
cell_type=4g
rootpage=0
pageviewid=9E2565BE-F897-F486-F28F-4746010C8B53
os=iOS
os_version=14.3
idfa=210f0d5448d6636523ec97b
```

**数据补充逻辑**

**isnewcustomer    yid    dayofYear < now.dayofYear   old**

```java
    public boolean isNewCustomer(String yid, String dateString) {
        String dateStr = getFromRedis(yid);
        if (StringUtils.isEmpty(dateStr)) {
            return true;
        }
        try {
            DateTime redisDate = DateTime.parse(dateStr, DateTimeFormat.forPattern(Utils.DATE_FORMAT));
            DateTime now = DateTime.parse(dateString, DateTimeFormat.forPattern(Utils.DATE_FORMAT));
            if (redisDate.getYear() < now.getYear() || redisDate.getDayOfYear() < now.getDayOfYear()) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
//        Date today =
        return true;
    }
```

**sessionid    UUID.randomUUID().toString();   --**

```
 public static class SessionReducer
            extends Reducer<DoubleKeyPair, Text, Text, NullWritable> {
        String sessionId = "";
        String timestr;
        Text result = new Text();
        long lastTimeStamp = 0;
        LogEntity entity;
        private static final long sessionDelta = 45 * 60 * 1000;

        public void reduce(DoubleKeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            sessionId = "";
            lastTimeStamp = 0;

            while (iter.hasNext()) {
                entity = LogEntity.fromTableString(iter.next().toString());
                if (entity == null) {
                    continue;
                }
                timestr = entity.getTime();
                try {
                    long timeStamp = Double.valueOf(timestr).longValue();
                    if (Utils.isEmpty(sessionId) || timeStamp - lastTimeStamp > sessionDelta) {
                        sessionId = UUID.randomUUID().toString();
                    }
                    entity.setSessionId(sessionId);
                    result.set(entity.toString());
                    lastTimeStamp = timeStamp;
                    context.write(result, NullWritable.get());

                } catch (Exception e) {
                    //
                }
            }
        }
    }
```

ydeviceid     idfa 字段

datetime      time 字段    2021-10-28 00:13:11.150

redis info

```
example key: customer_detected_05f0d933-3793-47d4-982d-d2e426762d71

```

-----

------

#### 3.实时处理

![image-20220105164603253](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20220105164603253.png)



##### 3.1 新、新用户判断逻辑-redis

```java
判断数据的day(数据时间）> day(key的记录时间) 
0 - 老用户   1 - 新用户

boolean cookieTimeCompare = TimeUtil.toYMD(newDate()).equals(redisCookieTime.substring(0, 10));
boolean redisUserIdCompare =TimeUtil.toYMD(newDate()).equals(redisUserIdTime.substring(0, 10));
```

---

##### 3.2 去重实现 - flink 状态 

```java
if (!dateTime.equals(state)){
     valueState.update(dateTime);
     return true;
 }

```

##### 3.3 结果统计

```java
10s的事件时间窗口，并设置clickhouse提交
 .withBatchIntervalMs(30000)  //
 .withBatchSize(100)
```



-----

-----

#### 4.测试环境 

##### 4.1 clickhouse 测试集群

![img](file:///C:/Users/fulang/Documents/WXWork/1688854756239296/Cache/Image/2021-11/企业微信截图_16364453496089.png)

```
登录：clickhouse-client --password 密码：123456
目录：/etc/clickhouse-server

用户： default
```

常用命令

```
清除数据：
ALTER TABLE ylogVisitStats_distributed on cluster 'perftest_3shards_1replicas' DELETE WHERE 1=1;
```



##### 4.2 zookeeper测试集群

```
clickhouse5 2181 2182 2183
```

![img](file:///C:/Users/fulang/Documents/WXWork/1688854756239296/Cache/Image/2021-11/企业微信截图_16365137025855.png)

##### 4.3 kafka测试集群

![image-20220105165909207](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20220105165909207.png)

常用命令

```shell
kafka-server-start.sh -daemon  ./server.properties

kafka-console-consumer.sh --bootstrap-server 172.16.7.27:9092 --from-beginning --topic shop2cn 

kafka-console-producer.sh --broker-list 172.16.7.27:9092 --topic shop2cn
```

##### 4.4 hadoop测试集群

```
172.16.7.39
172.16.7.40
172.16.7.41

账号：root@1qaz@WSX12!

```

##### 4.5 flink

```
172.16.7.39

```

##### 4.6 redis

```
主从，单节点，哨兵

172.16.7.27 clickhouse-1
```

![image-20211224142431353](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211224142431353.png)

```
./redis-trib.rb check 127.0.0.1:7002    -- 查看节点主从关系
```



#### 5.上线越洋

#####  5.1**初始化redis COOKIE  /  USERID**   

​    （业务要求两种判断新用户，离线的userID /cookie 数据）

​    	godmch_mchid_cookie_firsttime_daily   全量刷写到REDIS

 	   dim_userinfo_df.user_id                             最新分区

操作命令

com.ymatou.prj.RedisInitCookie

com.ymatou.prj.RedisInitUserid



```
hdfs dfs -put /usr/local/flink-1.13.2/examples/myex/cookie.csv /user/airflowuser/tmpdata
```

```
flink run -m yarn-cluster -d -yqu default  -ynm initCookie  -yjm 1024  -ytm 2048 -c com.ymatou.prj.RedisInitCookie  /usr/local/flink-1.13.2/examples/myex/RedisInit-1.0-SNAPSHOT-jar-with-dependencies.jar 
```

```
flink run -m yarn-cluster -d -yqu default  -ynm initUserid  -yjm 1024  -ytm 2048 -c com.ymatou.prj.RedisInitUserid  /usr/local/flink-1.13.2/examples/myex/RedisInit-1.0-SNAPSHOT-jar-with-dependencies.jar
```

com.ymatou.prj.RedisInitMap

```
flink run -m yarn-cluster -d -yqu default  -ynm initUserid  -yjm 1024  -ytm 2048 -c com.ymatou.prj.RedisInitMap  /usr/local/flink-1.13.2/examples/myex/RedisInit-1.0-SNAPSHOT-jar-with-dependencies.jar
```



```
"hdfs://hadoop-manager:8020/user/airflowuser/tmpdata/userid.csv"
```

```
"hdfs://hadoop-manager:8020/user/airflowuser/tmpdata/cookie1.csv"
```

##### 5.2 flink 打包执行

1. job全类名

```
com.ymatou.metrics.dwm.UniqueVisit

com.ymatou.metrics.dwm.PageUniqueVisit

com.ymatou.metrics.dwm.AddCart

com.ymatou.metrics.dwd.Shop2cnBaseLog

com.ymatou.metrics.ads.Shop2cnVisitStats
```



2.创建KAFKA 主题，定义分区数5

```shell
1.数据源  shop2cn

dwd_shop2cn_eventlog

--kafka-topics.sh --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka --create --topic dwd_shop2cn_eventlog --partitions 5 --replication-factor 2
```

```shell
dwd_shop2cn_pagelog

--kafka-topics.sh --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka --create --topic dwd_shop2cn_pagelog --partitions 5 --replication-factor 2
```

```shell
dwd_shop2cn_wifilog

--kafka-topics.sh --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka --create --topic dwd_shop2cn_wifilog --partitions 5 --replication-factor 2
```

```shell
dwm_shop2cn_uvlog

--kafka-topics.sh --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka --create --topic dwm_shop2cn_uvlog --partitions 5 --replication-factor 2
```

```shell
dwm_shop2cn_puvlog
--kafka-topics.sh --zookeeper 10.12.32.52:2181,10.12.32.53:2181,10.12.32.55:2181,10.12.33.37:2181,10.12.33.46:2181/bigdata-kafka --create --topic dwm_shop2cn_puvlog --partitions 5 --replication-factor 2
```

3.创建clickhouse表

```sql
SELECT COUNT(1) from ylogVisitStats_distributed yvsd where wst > '2021-12-24 00:00:00' 

show create table ylogVisitStats_distributed

drop table ylogVisitStats_distributed on  cluster 'cluster_3shards_2replicas'

show create table ylogVisitStats

drop table ylogVisitStats on  cluster 'cluster_3shards_2replicas'



CREATE TABLE default.ylogVisitStats on  cluster 'cluster_3shards_2replicas'
(
    `wst` DateTime,
    `wed` DateTime,
    `ts` String,
    `mch_id` String,
    `module_name` String,
    `app_version` String,
    `action_type` String,
    `is_New` UInt8,
    `uv_cnt` UInt32,
    `puv_cnt` UInt32,
    `pv_cnt` UInt32,
    `add_cart` UInt32,
    `mch_relation_uv` UInt32
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ylogVisitStats', '{replica}')
PARTITION BY toYYYYMMDD(wst)
ORDER BY (wst, mch_id, module_name, app_version, action_type,is_New)
SETTINGS index_granularity = 8192

CREATE TABLE default.ylogVisitStats_distributed on  cluster 'cluster_3shards_2replicas'
(
    `wst` DateTime,
    `wed` DateTime,
    `ts` String,
    `mch_id` String,
    `module_name` String,
    `app_version` String,
    `action_type` String,
     `is_New` UInt8,
    `uv_cnt` UInt32,
    `puv_cnt` UInt32,
    `pv_cnt` UInt32,
    `add_cart` UInt32,
    `mch_relation_uv` UInt32
)
ENGINE = Distributed('cluster_3shards_2replicas', 'default', 'ylogVisitStats', hiveHash(action_type))

```

4. 启动程序 

   **dwd:**

   com.ymatou.metrics.dwd.Shop2cnBaseLog

```shell

./flink run -m yarn-cluster -d -yqu default  -ynm shop2cn_dwd_Shop2cnBaseLog  -yjm 1024  -ytm 2048  -DJAVA_HOME=/usr/java/jdk1.8.0_112 -c com.ymatou.metrics.dwd.Shop2cnBaseLog  /usr/local/flink-1.13.2/examples/myex/yamtouRealtime-1.0-SNAPSHOT-jar-with-dependencies.jar --env prod
```



**dwm:**

​     com.ymatou.metrics.dwm.UniqueVisit

```sh

./flink run -m yarn-cluster -d -yqu default  -ynm shop2cn_dwm_UniqueVisit -yjm 1024  -ytm 2048 -DJAVA_HOME=/usr/java/jdk1.8.0_112 -c com.ymatou.metrics.dwm.UniqueVisit  /usr/local/flink-1.13.2/examples/myex/yamtouRealtime-1.0-SNAPSHOT-jar-with-dependencies.jar --env prod
```

​	com.ymatou.metrics.dwm.PageUniqueVisit

```sh

./flink run -m yarn-cluster -d -yqu default  -ynm shop2cn_dwm_PageUniqueVisit  -yjm 1024  -ytm 2048  -DJAVA_HOME=/usr/java/jdk1.8.0_112 -c com.ymatou.metrics.dwm.PageUniqueVisit  /usr/local/flink-1.13.2/examples/myex/yamtouRealtime-1.0-SNAPSHOT-jar-with-dependencies.jar --env prod
```

​	com.ymatou.metrics.dwm.AddCart

```sh

./flink run -m yarn-cluster -d -yqu default  -ynm shop2cn_dwm_AddCart  -yjm 1024  -ytm 2048  -DJAVA_HOME=/usr/java/jdk1.8.0_112 -c com.ymatou.metrics.dwm.AddCart  /usr/local/flink-1.13.2/examples/myex/yamtouRealtime-1.0-SNAPSHOT-jar-with-dependencies.jar --env prod
```

**ads:**

​    com.ymatou.metrics.ads.Shop2cnVisitStats

```sh

./flink run -m yarn-cluster -d -yqu default  -ynm shop2cn_ads_Shop2cnVisitStats  -yjm 1024  -ytm 2048  -DJAVA_HOME=/usr/java/jdk1.8.0_112 -c  com.ymatou.metrics.ads.Shop2cnVisitStats  /usr/local/flink-1.13.2/examples/myex/yamtouRealtime-1.0-SNAPSHOT-jar-with-dependencies.jar --env prod
```

5.停止程序

```
flink stop 
flink savepoint jobId
flink run -s :savepointPath
```

#### 6.其他

**CDH 资源情况**

CORE

![image-20211122151452889](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211122151452889.png)

![image-20211122151614125](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211122151614125.png)

![image-20211122151854992](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211122151854992.png)



MEM

![image-20211123111527354](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211123111527354.png)

![image-20211123111435071](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211123111435071.png)

![image-20211123111407736](C:\Users\fulang\AppData\Roaming\Typora\typora-user-images\image-20211123111407736.png)



**KAFKA分区数**

shop2cn - 5

![img](file:///C:/Users/fulang/Documents/WXWork/1688854756239296/Cache/Image/2021-11/企业微信截图_16375661031421.png)

![img](file:///C:/Users/fulang/Documents/WXWork/1688854756239296/Cache/Image/2021-11/企业微信截图_16375661377011.png)



测试环境配置

```shell
#clickhouse config

CLICKHOUSE_DRIVER=ru.yandex.clickhouse.ClickHouseDriver
CLICKHOUSE_SERVER=jdbc:clickhouse://172.16.7.27:8123/default
CLICKHOUSE_USR=default
CLICKHOUSE_PWD=123456

#redis sentinels config
SENTINELS = 172.16.7.27:26379
REDISHOST=172.16.7.27:6000
NODES=172.16.7.27:7001,172.16.7.27:7002,172.16.7.27:7003,172.16.7.27:7004,172.16.7.27:7005,172.16.7.27:7006
#kafka config
BROKERLIST_SHOP2CN=172.16.7.27:9092,172.16.7.28:9092,172.16.7.29:9092
BROKERLIST=172.16.7.27:9092,172.16.7.28:9092,172.16.7.29:9092

#hadoop config    3.x hadoop 8020
HADOOP=hdfs://172.16.7.39:9000/ymatouRealtime/checkpoints
```



-----

分层命名规范：

**分层_topic_指标**   

**ods:**

**dwd:**

​	dwd_shop2cn_baselog

​    com.ymatou.metrics.dwd.Shop2cnBaseLog



**dwm:**

​	dwm_shop2cn_uvlog

​	com.ymatou.metrics.dwm.UniqueVisit



​	com.ymatou.metrics.dwm.PageUniqueVisit



**ads:**

​	ods_shop2cn_orderdetail

​    com.ymatou.metrics.ads.Shop2cnVisitStats



**RTEDIS 连接信息**

**10.12.102.20:5268** master

```
10.12.102.23:5268 slave 7
10.12.102.10:5268 myself,
10.12.102.13:5268 slave 9
10.12.102.22:5268 slave 7
10.12.102.12:5268 master 
10.12.102.11:5268 master 
10.12.102.20:5268 master 
10.12.102.21:5268 master 

```

