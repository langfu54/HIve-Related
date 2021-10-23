
常用日期函数
unix_timestamp:返回当前或指定时间的时间戳	
from_unixtime：将时间戳转为日期格式
current_date：当前日期
current_timestamp：当前的日期加时间
to_date：抽取日期部分
year：获取年
month：获取月
day：获取日
hour：获取时
minute：获取分
second：获取秒
weekofyear：当前时间是一年中的第几周
dayofmonth：当前时间是一个月中的第几天
months_between： 两个日期间的月份
add_months：日期加减月
datediff：两个日期相差的天数
date_add：日期加天数
date_sub：日期减天数
last_day：日期的当月的最后一天
date_format : 按指定格式返回日期

常用取整函数
round： 四舍五入
ceil：  向上取整
floor： 向下取整

常用字符串操作函数
upper： 转大写
lower： 转小写
length： 长度
trim：  前后去空格
lpad： 向左补齐，到指定长度
rpad：  向右补齐，到指定长度
regexp_replace： SELECT regexp_replace('100-200', '(\\d+)', 'num') ；
	使用正则表达式匹配目标字符串，匹配成功后替换！

集合操作
size： 集合中元素的个数
map_keys： 返回map中的key
map_values: 返回map中的value
array_contains: 判断array中是否包含某个元素
sort_array： 将array中的元素排序
-----------------------------------------

create table gulivideo_orc(
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>)
stored as orc
tblproperties("orc.compress"="SNAPPY");

create table gulivideo_user_orc(
    uploader string,
    videos int,
    friends int)
row format delimited 
fields terminated by "\t" 
stored as orc
tblproperties("orc.compress"="SNAPPY");

----------------------------------------
11.4.1 统计视频观看数Top10

select
  videoId,
  uploader,
  views
from gulivideo_orc
order by views desc limit 10

|   videoid    |     uploader     |   views   |
+--------------+------------------+-----------+
| dMH0bHeiRNg  | judsonlaipply    | 42513417  |
| 0XxI-hvPRRA  | smosh            | 20282464  |
| 1dmVU08zVpA  | NBC              | 16087899  |
| RB-wUgnyGv0  | ChrisInScotland  | 15712924  |
| QjA5faZF1A8  | guitar90         | 15256922  |
| -_CSo1gOd48  | tasha            | 13199833  |
| 49IDp76kjPw  | TexMachina       | 11970018  |
| tYnn51C3X_w  | CowSayingMoo     | 11823701  |
| pv5zWaTEVkI  | OkGo             | 11672017  |
| D2kJZOfq7zk  | mrWoot           | 11184051  |
+--------------+------------------+-----------+
-----------------------------------------
    统计视频类别热度Top10
select
 category_name,
 count(*) c_c
from gulivideo_orc
lateral view  explode(category) tmp as category_name
group by category_name
order by c_c desc limit 10
+----------------+---------+
| category_name  |   c_c   |
+----------------+---------+
| Music          | 179049  |
| Entertainment  | 127674  |
| Comedy         | 87818   |
| Animation      | 73293   |
| Film           | 73293   |
| Sports         | 67329   |
| Games          | 59817   |
| Gadgets        | 59817   |
| People         | 48890   |
| Blogs          | 48890   |
+----------------+---------+
select
  videoId,
  category_name
from gulivideo_orc
lateral view  explode(category) tmp as category_name t1

select
 t1.category_name,
 count(*) c_c
from (
      select
        videoId,
        category_name
      from gulivideo_orc
      lateral view  explode(category) tmp as category_name
)t1
group by t1.category_name
order by c_c desc limit 10


11.4.3 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
select
 videoid,
 views,
 category
from gulivideo_orc
order by views desc limit 20  t1

select
 category_name,
 count(*)
from (
      select
       videoid,
       views,
       category
      from gulivideo_orc
      order by views desc limit 20 
)t1 
lateral view explode(t1.category) tmp as category_name
group by category_name

| category_name  | _c1  |
+----------------+------+
| Blogs          | 2    |
| Comedy         | 6    |
| Entertainment  | 6    |
| Music          | 5    |
| People         | 2    |
| UNA            | 1    |
+----------------+------+
11.4.4 统计视频观看数Top50所关联视频的所属类别排序

1.统计视频观看数Top50所关联视频
select
 videoid,
 views,
 relatedId
from gulivideo_orc
order by views desc limit 50 t1

+--------------+-----------+----------------------------------------------------+
|   videoid    |   views   |                     relatedid                      |
+--------------+-----------+----------------------------------------------------+
| dMH0bHeiRNg  | 42513417  | ["OxBtqwlTMJQ","1hX1LxXwdl8","NvVbuVGtGSE","Ft6fC6RI4Ms","plv1e3MvxFw","1VL-ShAEjmg","y8k5QbVz3SE","weRfgj_349Q","_MFpPziLP9o","0M-xqfP1ibo","n4Pr_iCxxGU","UrWnNAMec98","QoREX_TLtZo","I-cm3GF-jX0","doIQXfJvydY","6hD3gGg9jMk","Hfbzju1FluI","vVN_pLl5ngg","3PnoFu027hc","7nrpwEDvusY"] |
| 0XxI-hvPRRA  | 20282464  | ["ut5fFyTkKv4","cYmeG712dD0","aDiNeF5dqnA","lNFFR1uwPGo","5Iyw4y6QR14","N1NO0iLbEt0","YtmGrR0tR7E","GZltV9lWQL4","qUDLSsSrrRA","wpQ1llsQ7qo","u9w2z-xtmqY","txVJgU3n72g","M6KcfOAckmw","orkbRVgRys0","HSuSo9hG_RI","3H3kKJLQgPs","46EsU9PmPyk","nn4XzrI1LLk","VTpKh6jFS7M","xH4b9ydgaHk"] |

2.关联视频的所属类别
select
 relatedId_name
from(
     select
      videoid,
      views,
      relatedId
     from gulivideo_orc
     order by views desc limit 50
) t1
lateral view explode(t1.relatedId) tmp as relatedId_name  t2



select
 t3.videoId,
 t3.category
from (
      select
       relatedId_name
      from(
           select
            videoid,
            views,
            relatedId
           from gulivideo_orc
           order by views desc limit 50
      ) t1
      lateral view explode(t1.relatedId) tmp as relatedId_name
)t2 join gulivideo_orc t3
on t2.relatedId_name=t3.videoId  t4


| rlNzm18WNjw  | ["Entertainment"]     |
| IMiTJhiWx-k  | ["Entertainment"]     |
| 8DnZEc4yQVU  | ["Entertainment"]     |
| 6EzIHNcnz64  | ["Entertainment"]     |
| z_BCoHhxxYY  | ["Comedy"]            |
| MKUVC1jHYME  | ["Comedy"]            |
| xa_b4abHtKc  | ["Comedy"]            |
| mCP_Mkf6T4s  | ["Comedy"]            |
| SDmM_aaX9wg  | ["Comedy"]            |
| xKBD1x3UOsk  | ["Entertainment"]     |
| HVSBP3OpKBM  | ["People","Blogs"]    |
| EWzURxmuQUQ  | ["Entertainment"]     |
| Gw63atzHGyw  | ["Entertainment"]     |
| eJUc9UJkj1Y  | ["People","Blogs"]    |
3. 排序
select
 category_name,
 count(*) c_c
from (
         select
          t3.videoId,
          t3.category
         from (
               select
                relatedId_name
               from(
                    select
                     videoid,
                     views,
                     relatedId
                    from gulivideo_orc
                    order by views desc limit 50
               ) t1
               lateral view explode(t1.relatedId) tmp as relatedId_name
         )t2 join gulivideo_orc t3
         on t2.relatedId_name=t3.videoId
         )t4
lateral view explode(t4.category) tmp as category_name
group by category_name  t5





select
 t5.category_name,
 t5.c_c,
 rank()over(order by t5.c_c desc) r_k
from (
     select
      category_name,
      count(*) c_c
     from (
              select
               t3.videoId,
               t3.category
              from (
                    select
                     relatedId_name
                    from(
                         select
                          videoid,
                          views,
                          relatedId
                         from gulivideo_orc
                         order by views desc limit 50
                    ) t1
                    lateral view explode(t1.relatedId) tmp as relatedId_name
              )t2 join gulivideo_orc t3
              on t2.relatedId_name=t3.videoId
              )t4
     lateral view explode(t4.category) tmp as category_name
     group by category_name
) t5


+-------------------+---------+------+
| t5.category_name  | t5.c_c  | r_k  |
+-------------------+---------+------+
| Comedy            | 237     | 1    |
| Entertainment     | 216     | 2    |
| Music             | 195     | 3    |
| Blogs             | 51      | 4    |
| People            | 51      | 4    |
| Film              | 47      | 6    |
| Animation         | 47      | 6    |
| News              | 24      | 8    |
| Politics          | 24      | 8    |
| Games             | 22      | 10   |
| Gadgets           | 22      | 10   |
| Sports            | 19      | 12   |
| Howto             | 14      | 13   |
| DIY               | 14      | 13   |
| UNA               | 13      | 15   |
| Places            | 12      | 16   |
| Travel            | 12      | 16   |
| Animals           | 11      | 18   |
| Pets              | 11      | 18   |
| Autos             | 4       | 20   |
| Vehicles          | 4       | 20   |
+-------------------+---------+------+
11.4.5 统计每个类别中的视频热度（视频观看数）Top10，以Music为例

1.先炸开 
select
 videoid,
 views,
 category_name
from gulivideo_orc
lateral view explode(category) tmp as category_name
where category_name='Music'
order by views desc limit 10


select
 videoid,
 views,
 category_name
from gulivideo_orc
lateral view explode(category) tmp as category_name t1

select
 t1.videoid,
 t1.views,
 t1.category_name
from (
      select
       videoid,
       views,
       category_name
      from gulivideo_orc
      lateral view explode(category) tmp as category_name
)t1 
where t1.category_name='Music'
 order by t1.views desc  limit 10

 |  t1.videoid  | t1.views  | t1.category_name  |
+--------------+-----------+-------------------+
| QjA5faZF1A8  | 15256922  | Music             |
| tYnn51C3X_w  | 11823701  | Music             |
| pv5zWaTEVkI  | 11672017  | Music             |
| 8bbTtPL1jRs  | 9579911   | Music             |
| UMf40daefsI  | 7533070   | Music             |
| -xEzGIuY7kw  | 6946033   | Music             |
| d6C0bNDqf3Y  | 6935578   | Music             |
| HSoVKUVOnfQ  | 6193057   | Music             |
| 3URfWTEPmtE  | 5581171   | Music             |
| thtmaZnxk_0  | 5142238   | Music             |
+--------------+-----------+-------------------+
11.4.6 统计每个类别视频观看数Top10


      select
       videoid,
       views,
       category_name
      from gulivideo_orc
      lateral view explode(category) tmp as category_name limit 20 t1

select
 t1.videoid,
 t1.views,
 t1.category_name,
 rank()over(partition by t1.category_name order by t1.views desc) r_k
from (
      select
       videoid,
       views,
       category_name
      from gulivideo_orc
      lateral view explode(category) tmp as category_name
)t1   t2


select
 t2.videoid,
 t2.views,
 t2.category_name,
 t2.r_k
from (
      select
       t1.videoid,
       t1.views,
       t1.category_name,
       rank()over(partition by t1.category_name order by t1.views desc) r_k
      from (
            select
             videoid,
             views,
             category_name
            from gulivideo_orc
            lateral view explode(category) tmp as category_name
      )t1 
)t2
where t2.r_k<=10

--------------+-----------+-------------------+---------+
|  t2.videoid  | t2.views  | t2.category_name  | t2.r_k  |
+--------------+-----------+-------------------+---------+
| 2GWPOPSXGYI  | 3660009   | Animals           | 1       |
| xmsV9R8FsDA  | 3164582   | Animals           | 2       |
| 12PsUW-8ge4  | 3133523   | Animals           | 3       |
| OeNggIGSKH8  | 2457750   | Animals           | 4       |
| WofFb_eOxxA  | 2075728   | Animals           | 5       |
| AgEmZ39EtFk  | 1999469   | Animals           | 6       |
| a-gW3RbJd8U  | 1836870   | Animals           | 7       |
| 8CL2hetqpfg  | 1646808   | Animals           | 8       |
| QmroaYVD_so  | 1645984   | Animals           | 9       |
| Sg9x5mUjbH8  | 1527238   | Animals           | 10      |
| sdUUx5FdySs  | 5840839   | Animation         | 1       |
| 6B26asyGKDo  | 5147533   | Animation         | 2       |
| H20dhY01Xjk  | 3772116   | Animation         | 3       |
| 55YYaJIrmzo  | 3356163   | Animation         | 4       |
| JzqumbhfxRo  | 3230774   | Animation         | 5       |
| eAhfZUZiwSE  | 3114215   | Animation         | 6       |
| h7svw0m-wO0  | 2866490   | Animation         | 7       |
| tAq3hWBlalU  | 2830024   | Animation         | 8       |
| AJzU3NjDikY  | 2569611   | Animation         | 9       |
| ElrldD02if0  | 2337238   | Animation         | 10      |


约定1：取Top10中所有人上传的视频的观看次数前20

select
 uploader,
 videos
from gulivideo_user_orc
 order by videos desc limit 10 t1

 +---------------------+---------+
|      uploader       | videos  |
+---------------------+---------+
| expertvillage       | 86228   |
| TourFactory         | 49078   |
| myHotelVideo        | 33506   |
| AlexanderRodchenko  | 24315   |
| VHTStudios          | 20230   |
| ephemeral8          | 19498   |
| HSN                 | 15371   |
| rattanakorn         | 12637   |
| Ruchaneewan         | 10059   |
| futifu              | 9668    |
+---------------------+---------+


select
 t2.uploader,
 t2.views
from (
     select
      uploader,
      videos
     from gulivideo_user_orc
      order by videos desc limit 10
)t1 join gulivideo_orc t2
on t1.uploader=t2.uploader
 order by t2.views desc limit 20

 |  t2.uploader   | t2.views  |
+----------------+-----------+
| expertvillage  | 39059     |
| expertvillage  | 29975     |
| expertvillage  | 26270     |
| expertvillage  | 25511     |
| expertvillage  | 25366     |
| expertvillage  | 24659     |
| expertvillage  | 22593     |
| expertvillage  | 18822     |
| expertvillage  | 16304     |
| expertvillage  | 13576     |
| expertvillage  | 13450     |
| expertvillage  | 11639     |
| expertvillage  | 11553     |
| expertvillage  | 11452     |
| expertvillage  | 10915     |
| expertvillage  | 10817     |
| expertvillage  | 10597     |
| expertvillage  | 10402     |
| expertvillage  | 9422      |
| expertvillage  | 7123      |



约定2：取Top10中每个人上传的视频的观看次数前20

select
 t2.uploader,
 t2.views,
 rank()over(partition by t2.uploader order by t2.views desc ) r_k
from ( 
     select
      uploader,
      videos
     from gulivideo_user_orc
      order by videos desc limit 10
)t1 join gulivideo_orc t2
on t1.uploader=t2.uploader   t3

select
 t3.uploader,
 t3.views,
 t3.r_k
from (
      select
       t2.uploader,
       t2.views,
       rank()over(partition by t2.uploader order by t2.views desc ) r_k
      from ( 
           select
            uploader,
            videos
           from gulivideo_user_orc
            order by videos desc limit 10
      )t1 join gulivideo_orc t2
      on t1.uploader=t2.uploader
)t3
where t3.r_k <=20

----------------+-----------+---------+
|  t3.uploader   | t3.views  | t3.r_k  |
+----------------+-----------+---------+
| Ruchaneewan    | 3132      | 1       |
| Ruchaneewan    | 1086      | 2       |
| Ruchaneewan    | 549       | 3       |
| Ruchaneewan    | 453       | 4       |
| Ruchaneewan    | 441       | 5       |
| Ruchaneewan    | 426       | 6       |
| Ruchaneewan    | 420       | 7       |
| Ruchaneewan    | 420       | 7       |
| Ruchaneewan    | 418       | 9       |
| Ruchaneewan    | 395       | 10      |
| Ruchaneewan    | 389       | 11      |
| Ruchaneewan    | 344       | 12      |
| Ruchaneewan    | 271       | 13      |
| Ruchaneewan    | 242       | 14      |
| Ruchaneewan    | 231       | 15      |
| Ruchaneewan    | 227       | 16      |
| Ruchaneewan    | 226       | 17      |
| Ruchaneewan    | 213       | 18      |
| Ruchaneewan    | 209       | 19      |
| Ruchaneewan    | 206       | 20      |
--------------------------------------------

约定3：Top10用户上传的所有视频，有哪些视频是在视频观看次数前20的视频


观看数前20的视频
select
  videoId,
  uploader,
  views
from gulivideo_orc
order by views desc limit 20  t1

top10的用户
select
 uploader,
 videos
from gulivideo_user_orc
 order by videos desc limit 10  t2

select
  *
 from (
        select
         videoId,
         uploader,
         views
       from gulivideo_orc
       order by views desc limit 20
 ) t1 join (
             select
              uploader,
              videos
             from gulivideo_user_orc
              order by videos desc limit 10
 )t2
 on t1.uploader=t2.uploader

--------------------------------课后练习第一题

create table action
(userId string,
visitDate string,
visitCount int) 
row format delimited fields terminated by "\t";



1.替换日期格式

select
 userId,
 date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') visitDate,
 visitCount
from action t1


select
 t1.userId,
 t1.visitDate,
 sum(t1.visitCount) s_c
from (
      select
       userId,
       date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') visitDate,
       visitCount
      from action
)t1
group by t1.userId,t1.visitDate  t2

select
 t2.userId,
 t2.visitDate,
 t2.s_c,
 sum(t2.s_c)over(partition by t2.userId order by t2.visitDate)
from (
      select
       t1.userId,
       t1.visitDate,
       sum(t1.visitCount) s_c
      from (
            select
             userId,
             date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') visitDate,
             visitCount
            from action
      )t1
      group by t1.userId,t1.visitDate
)t2
+------------+---------------+---------+---------------+
| t2.userid  | t2.visitdate  | t2.s_c  | sum_window_0  |
+------------+---------------+---------+---------------+
| u01        | 2017-01       | 11      | 11            |
| u01        | 2017-02       | 12      | 23            |
| u02        | 2017-01       | 12      | 12            |
| u03        | 2017-01       | 8       | 8             |
| u04        | 2017-01       | 3       | 3             |
+------------+---------------+---------+---------------+

create table user_low_carbon(user_id String,data_dt String,low_carbon int) row format delimited fields terminated by '\t';
create table plant_carbon(plant_id string,plant_name String,low_carbon int) row format delimited fields terminated by '\t';
----题目
1.蚂蚁森林植物申领统计
问题：假设2017年1月1日开始记录低碳数据（user_low_carbon），假设2017年10月1日之前满足申领条件的用户都申领了一颗p004-胡杨，
剩余的能量全部用来领取“p002-沙柳” 。
统计在10月1日累计申领“p002-沙柳” 排名前10的用户信息；以及他比后一名多领了几颗沙柳。
得到的统计结果如下表样式：
user_id  plant_count less_count(比后一名多领了几颗沙柳)
u_101    1000         100
u_088    900          400
u_103    500          …

select
 user_id,
 regexp_replace(data_dt,'/','-') data_dt,
 low_carbon
from user_low_carbon
where regexp_replace(data_dt,'/','-') <'2017-10-01' t1


select
  t1.user_id,
  sum(t1.low_carbon)-(select low_carbon from plant_carbon where plant_name='胡杨') sum_low_carbon
from (
     select
      user_id,
      regexp_replace(data_dt,'/','-') data_dt,
      low_carbon
     from user_low_carbon
     where regexp_replace(data_dt,'/','-') <'2017-10-01'
)t1
group by user_id  t2

select
 t2.user_id,
 floor(t2.sum_low_carbon/(select low_carbon from plant_carbon where plant_name='沙柳')) plant_count
from (
      select
        t1.user_id,
        sum(t1.low_carbon)-(select low_carbon from plant_carbon where plant_name='胡杨') sum_low_carbon
      from (
           select
            user_id,
            regexp_replace(data_dt,'/','-') data_dt,
            low_carbon
           from user_low_carbon
           where regexp_replace(data_dt,'/','-') <'2017-10-01'
      )t1
      group by user_id
) t2
where sum_low_carbon >0  t3



select
 t3.user_id,
 t3.plant_count
from (
      select
       t2.user_id,
       floor(t2.sum_low_carbon/(select low_carbon from plant_carbon where plant_name='沙柳')) plant_count
      from (
            select
              t1.user_id,
              sum(t1.low_carbon)-(select low_carbon from plant_carbon where plant_name='胡杨') sum_low_carbon
            from (
                 select
                  user_id,
                  regexp_replace(data_dt,'/','-') data_dt,
                  low_carbon
                 from user_low_carbon
                 where regexp_replace(data_dt,'/','-') <'2017-10-01'
            )t1
            group by user_id
      ) t2
      where sum_low_carbon >0 
)t3
 order by t3.plant_count desc limit 11  t4



select
 t4.user_id,
 t4.plant_count,
 t4.plant_count-lead(t4.plant_count,1,0)over(order by t4.plant_count desc) less_count
 from (
      select
      t3.user_id,
      t3.plant_count
      from (
           select
            t2.user_id,
            floor(t2.sum_low_carbon/(select low_carbon from plant_carbon where plant_name='沙柳')) plant_count
           from (
                 select
                   t1.user_id,
                   sum(t1.low_carbon)-(select low_carbon from plant_carbon where plant_name='胡杨') sum_low_carbon
                 from (
                      select
                       user_id,
                       regexp_replace(data_dt,'/','-') data_dt,
                       low_carbon
                      from user_low_carbon
                      where regexp_replace(data_dt,'/','-') <'2017-10-01'
                 )t1
                 group by user_id
           ) t2
           where sum_low_carbon >0 
      )t3
      order by t3.plant_count desc limit 11
 )t4 limit 10

+-------------+-----------------+-------------+
| t4.user_id  | t4.plant_count  | less_count  |
+-------------+-----------------+-------------+
| u_007       | 66              | 3           |
| u_013       | 63              | 10          |
| u_008       | 53              | 7           |
| u_005       | 46              | 1           |
| u_010       | 45              | 1           |
| u_014       | 44              | 5           |
| u_011       | 39              | 2           |
| u_009       | 37              | 5           |
| u_006       | 32              | 9           |
| u_002       | 23              | 1           |

2、蚂蚁森林低碳用户排名分析
问题：查询user_low_carbon表中每日流水记录，条件为：
用户在2017年，连续三天（或以上）的天数里，
每天减少碳排放（low_carbon）都超过100g的用户低碳流水。
需要查询返回满足以上条件的user_low_carbon表中的记录流水。
例如用户u_002符合条件的记录如下，因为2017/1/2~2017/1/5连续四天的碳排放量之和都大于等于100g：

拿到2017年数据
select
 user_id,
 regexp_replace(data_dt,'/','-') data_dt,
 sum(low_carbon) sum_low_carbon
from user_low_carbon
where year(regexp_replace(data_dt,'/','-')) ='2017'
 group by user_id,regexp_replace(data_dt,'/','-') t1

select
 t1.user_id,
 t1.data_dt,
 t1.sum_low_carbon
from (
     select
      user_id,
      regexp_replace(data_dt,'/','-') data_dt,
      sum(low_carbon) sum_low_carbon
     from user_low_carbon
     where year(regexp_replace(data_dt,'/','-')) ='2017'
      group by user_id,regexp_replace(data_dt,'/','-') 
)t1 
where t1.sum_low_carbon>=100  t2
-------------+-------------+--------------------+
| t1.user_id  | t1.data_dt  | t1.sum_low_carbon  |
+-------------+-------------+--------------------+
| u_001       | 2017-1-2    | 270                |
| u_001       | 2017-1-6    | 135                |
| u_002       | 2017-1-2    | 220                |
| u_002       | 2017-1-3    | 110                |
| u_002       | 2017-1-4    | 150                |
| u_002       | 2017-1-5    | 101                |
| u_003       | 2017-1-2    | 160                |
| u_003       | 2017-1-3    | 160                |
| u_003       | 2017-1-5    | 120                |
| u_003       | 2017-1-7    | 120                |
| u_004       | 2017-1-1    | 110                |
| u_004       | 2017-1-3    | 120                |
| u_004       | 2017-1-6    | 120                |
| u_004       | 2017-1-7    | 130                |
| u_005       | 2017-1-2    | 130                |
| u_005       | 2017-1-3    | 180                |
| u_005       | 2017-1-4    | 190                |
| u_005       | 2017-1-6    | 280                |
| u_005       | 2017-1-7    | 160                |

怎么算连续三天

2017-1-1  2017-1-2  2017-1-3

2017-1-2  2017-1-3  2017-1-4

2017-1-3  2017-1-4  2017-1-5

select
 t2.user_id,
 t2.sum_low_carbon,
 lag(t2.data_dt,2,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) qt,
 lag(t2.data_dt,1,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) zt,
 t2.data_dt,
 lead(t2.data_dt,1,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) mt,
 lead(t2.data_dt,2,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) ht
from (
      select
       t1.user_id,
       t1.data_dt,
       t1.sum_low_carbon
      from (
           select
            user_id,
            regexp_replace(data_dt,'/','-') data_dt,
            sum(low_carbon) sum_low_carbon
           from user_low_carbon
           where year(regexp_replace(data_dt,'/','-')) ='2017'
            group by user_id,regexp_replace(data_dt,'/','-') 
      )t1 
      where t1.sum_low_carbon>=100 
)t2  t3

select
 t3.user_id,
 t3.data_dt,
 t3.sum_low_carbon,
 datediff(data_dt,qt) jt_qt,
 datediff(data_dt,zt) jt_zt,
 datediff(data_dt,mt) jt_mt,
 datediff(data_dt,ht) jt_ht
from (
     select
      t2.user_id,
      t2.sum_low_carbon,
      lag(t2.data_dt,2,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) qt,
      lag(t2.data_dt,1,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) zt,
      t2.data_dt,
      lead(t2.data_dt,1,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) mt,
      lead(t2.data_dt,2,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) ht
     from (
           select
            t1.user_id,
            t1.data_dt,
            t1.sum_low_carbon
           from (
                select
                 user_id,
                 regexp_replace(data_dt,'/','-') data_dt,
                 sum(low_carbon) sum_low_carbon
                from user_low_carbon
                where year(regexp_replace(data_dt,'/','-')) ='2017'
                 group by user_id,regexp_replace(data_dt,'/','-') 
           )t1 
           where t1.sum_low_carbon>=100 
     )t2 
)t3  t4


select
 t4.user_id,
 t4.data_dt,
 t4.sum_low_carbon
from (
    select
     t3.user_id,
     t3.data_dt,
     t3.sum_low_carbon,
     datediff(data_dt,qt) jt_qt,
     datediff(data_dt,zt) jt_zt,
     datediff(data_dt,mt) jt_mt,
     datediff(data_dt,ht) jt_ht
    from (
         select
          t2.user_id,
          t2.sum_low_carbon,
          lag(t2.data_dt,2,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) qt,
          lag(t2.data_dt,1,'0000-00-00')over(partition by t2.user_id order by t2.data_dt) zt,
          t2.data_dt,
          lead(t2.data_dt,1,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) mt,
          lead(t2.data_dt,2,'9999-99-99')over(partition by t2.user_id order by t2.data_dt) ht
         from (
               select
                t1.user_id,
                t1.data_dt,
                t1.sum_low_carbon
               from (
                    select
                     user_id,
                     regexp_replace(data_dt,'/','-') data_dt,
                     sum(low_carbon) sum_low_carbon
                    from user_low_carbon
                    where year(regexp_replace(data_dt,'/','-')) ='2017'
                     group by user_id,regexp_replace(data_dt,'/','-') 
               )t1 
               where t1.sum_low_carbon>=100 
         )t2 
    )t3
    )t4 
where (t4.jt_qt=2 and t4.jt_zt=1) or(t4.jt_zt=1 and t4.jt_mt=-1) or(t4.jt_mt=-1 and t4.jt_ht=-2)

------------------------------第二种 
select
 t1.user_id,
 t1.data_dt,
 t1.sum_low_carbon
from (
     select
      user_id,
      regexp_replace(data_dt,'/','-') data_dt,
      sum(low_carbon) sum_low_carbon
     from user_low_carbon
     where year(regexp_replace(data_dt,'/','-')) ='2017'
      group by user_id,regexp_replace(data_dt,'/','-') 
)t1 
where t1.sum_low_carbon>=100   t2

利用等差数列

select
 t2.user_id,
 t2.data_dt,
 t2.sum_low_carbon,
 rank()over(partition by t2.user_id order by t2.data_dt) rk
from (
     select
      t1.user_id,
      t1.data_dt,
      t1.sum_low_carbon
     from (
          select
           user_id,
           regexp_replace(data_dt,'/','-') data_dt,
           sum(low_carbon) sum_low_carbon
          from user_low_carbon
          where year(regexp_replace(data_dt,'/','-')) ='2017'
           group by user_id,regexp_replace(data_dt,'/','-') 
     )t1 
     where t1.sum_low_carbon>=100
)t2  t3

select
 t3.user_id,
 t3.data_dt,
 t3.sum_low_carbon,
 date_sub(t3.data_dt,rk) data_dt_rk
from(
     select
      t2.user_id,
      t2.data_dt,
      t2.sum_low_carbon,
      rank()over(partition by t2.user_id order by t2.data_dt) rk
     from (
          select
           t1.user_id,
           t1.data_dt,
           t1.sum_low_carbon
          from (
               select
                user_id,
                regexp_replace(data_dt,'/','-') data_dt,
                sum(low_carbon) sum_low_carbon
               from user_low_carbon
               where year(regexp_replace(data_dt,'/','-')) ='2017'
                group by user_id,regexp_replace(data_dt,'/','-') 
          )t1 
          where t1.sum_low_carbon>=100
     )t2 
) t3  t4


select
 t4.user_id,
 t4.data_dt,
 t4.sum_low_carbon,
 count(*)over(partition by user_id) cc
from (
    select
     t3.user_id,
     t3.data_dt,
     t3.sum_low_carbon,
     date_sub(t3.data_dt,rk) data_dt_rk
    from(
         select
          t2.user_id,
          t2.data_dt,
          t2.sum_low_carbon,
          rank()over(partition by t2.user_id order by t2.data_dt) rk
         from (
              select
               t1.user_id,
               t1.data_dt,
               t1.sum_low_carbon
              from (
                   select
                    user_id,
                    regexp_replace(data_dt,'/','-') data_dt,
                    sum(low_carbon) sum_low_carbon
                   from user_low_carbon
                   where year(regexp_replace(data_dt,'/','-')) ='2017'
                    group by user_id,regexp_replace(data_dt,'/','-') 
              )t1 
              where t1.sum_low_carbon>=100
         )t2 
    ) t3
)t4  t5

select
t5.user_id,
t5.data_dt,
t5.sum_low_carbon,
t5.cc
from(
    select
     t4.user_id,
     t4.data_dt,
     t4.sum_low_carbon,
     count(*)over(partition by user_id) cc
    from (
        select
         t3.user_id,
         t3.data_dt,
         t3.sum_low_carbon,
         date_sub(t3.data_dt,rk) data_dt_rk
        from(
             select
              t2.user_id,
              t2.data_dt,
              t2.sum_low_carbon,
              rank()over(partition by t2.user_id order by t2.data_dt) rk
             from (
                  select
                   t1.user_id,
                   t1.data_dt,
                   t1.sum_low_carbon
                  from (
                       select
                        user_id,
                        regexp_replace(data_dt,'/','-') data_dt,
                        sum(low_carbon) sum_low_carbon
                       from user_low_carbon
                       where year(regexp_replace(data_dt,'/','-')) ='2017'
                        group by user_id,regexp_replace(data_dt,'/','-') 
                  )t1 
                  where t1.sum_low_carbon>=100
             )t2 
        ) t3
    )t4
) t5 where cc>=3

