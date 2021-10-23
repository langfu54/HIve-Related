--------------------------------会话划分-------------------------------------------
select
    mid_id,
    count(*)
from
(
    select
        mid_id,
        session_id
    from
    (
        select
            mid_id,
            last_page_id,
            page_id,
            during_time,
            ts,
            session_point,
            concat(mid_id,'-',last_value(session_point,true) over(partition by mid_id order by ts)) session_id
        from
        (
            select
                mid_id,
                last_page_id,
                page_id,
                during_time,
                ts,
                if(last_page_id is null,ts,null) session_point
            from dwd_page_log
            where dt='2020-06-14'
        ) t1
    ) t2
    group by mid_id,session_id
) t3
group by mid_id
having count(*) > 1;
---------------------------------------会话划分----------------------------------
select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    session_point,
    concat(mid_id,'-',last_value(session_point,true) over(partition by mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
from
(
    select
        mid_id,
        last_page_id,
        page_id,
        during_time,
        ts,
        if(last_page_id is null,ts,null) session_point
    from dwd_page_log
    where dt='2020-06-14'
) t1
where mid_id='mid_53480';
----------------文档需求--------------------
select
    mid_id,
    session_id,
    count(*) page_count,
    sum(during_time) during_time
from
(
    select
        mid_id,
        last_page_id,
        page_id,
        during_time,
        ts,
        session_point,
        concat(mid_id,'-',last_value(session_point,true) over(partition by mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
    from
    (
        select
            mid_id,
            last_page_id,
            page_id,
            during_time,
            ts,
            if(last_page_id is null,ts,null) session_point
        from dwd_page_log
        where dt='2020-06-14'
    ) t1
)t2
group by mid_id,session_id;

--访客统计简单版
CREATE EXTERNAL TABLE ads_visit_stats_tmp (
  `dt` STRING COMMENT '统计日期',
  `uv_count` BIGINT COMMENT '日活(访问人数)',
  `duration_sec` BIGINT COMMENT '页面停留总时长',
  `avg_duration_sec` BIGINT COMMENT '一次会话，页面停留平均时长,单位为秒',
  `page_count` BIGINT COMMENT '页面总浏览数',
  `avg_page_count` BIGINT COMMENT '一次会话，页面平均浏览数',
  `sv_count` BIGINT COMMENT '会话次数',
  `bounce_count` BIGINT COMMENT '跳出数',
  `bounce_rate` DECIMAL(16,2) COMMENT '跳出率'
) COMMENT '访客统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_visit_stats_tmp/';

--数据装载
insert overwrite table ads_visit_stats_tmp
select * from ads_visit_stats_tmp
union
select
    '2020-06-14',
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(sum(during_time)/1000 as bigint) duration_sec,
    cast(avg(during_time)/1000 as bigint) avg_duration_sec,
    cast(sum(page_count) as bigint) page_count,
    cast(avg(page_count) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count=1,1,0)) as bigint) bounce_count,
    cast(sum(if(page_count=1,1,0))/count(*)*100 as DECIMAL(16,2)) bounce_rate
from
(
    select
        mid_id,
        session_id,
        count(*) page_count,
        sum(during_time) during_time
    from
    (
        select
            mid_id,
            last_page_id,
            page_id,
            during_time,
            ts,
            session_point,
            concat(mid_id,'-',last_value(session_point,true) over(partition by mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
        from
        (
            select
                mid_id,
                last_page_id,
                page_id,
                during_time,
                ts,
                if(last_page_id is null,ts,null) session_point
            from dwd_page_log
            where dt='2020-06-14'
        ) t1
    )t2
    group by mid_id,session_id
)t3;

--访客统计 最终版
CREATE EXTERNAL TABLE ads_visit_stats (
  `dt` STRING COMMENT '统计日期',
  `is_new` STRING COMMENT '新老标识,1:新,0:老',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `channel` STRING COMMENT '渠道',
  `uv_count` BIGINT COMMENT '日活(访问人数)',
  `duration_sec` BIGINT COMMENT '页面停留总时长',
  `avg_duration_sec` BIGINT COMMENT '一次会话，页面停留平均时长,单位为秒',
  `page_count` BIGINT COMMENT '页面总浏览数',
  `avg_page_count` BIGINT COMMENT '一次会话，页面平均浏览数',
  `sv_count` BIGINT COMMENT '会话次数',
  `bounce_count` BIGINT COMMENT '跳出数',
  `bounce_rate` DECIMAL(16,2) COMMENT '跳出率'
) COMMENT '访客统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_visit_stats/';

--数据装载 最终版
insert overwrite table ads_visit_stats
select * from ads_visit_stats
union
select
    '2020-06-14',
    is_new,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(sum(during_time)/1000 as bigint) duration_sec,
    cast(avg(during_time)/1000 as bigint) avg_duration_sec,
    cast(sum(page_count) as bigint) page_count,
    cast(avg(page_count) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count=1,1,0)) as bigint) bounce_count,
    cast(sum(if(page_count=1,1,0))/count(*)*100 as DECIMAL(16,2)) bounce_rate
from
(
    select
        mid_id,
        is_new,
        recent_days,
        channel,
        session_id,
        count(*) page_count,
        sum(during_time) during_time
    from
    (
        select
            t1.mid_id,
            if(t0.visit_date_first>=date_add('2020-06-14',-recent_days+1),'1','0') is_new,
            recent_days,
            channel,
            last_page_id,
            page_id,
            during_time,
            ts,
            session_point,
            concat(t1.mid_id,'-',last_value(session_point,true) over(partition by recent_days,t1.mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
        from
        (
            select
                mid_id,
                recent_days,
                channel,
                last_page_id,
                page_id,
                during_time,
                ts,
                if(last_page_id is null,ts,null) session_point
            from dwd_page_log
            lateral view explode(array(1,7,30)) tmp as recent_days
            where dt>=date_add('2020-06-14',-29)
            and dt>=date_add('2020-06-14',-recent_days+1)
        ) t1
        left join
        (
            select
                mid_id,
                visit_date_first
            from dwt_visitor_topic
            where dt='2020-06-14'
        )t0
        on t1.mid_id=t0.mid_id
    )t2
    group by mid_id,is_new,recent_days,channel,session_id
)t3
group by recent_days,is_new,channel;

select count(*) from ods_order_detail;

--路径分析
CREATE EXTERNAL TABLE ads_page_path
(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source` STRING COMMENT '跳转起始页面ID',
    `target` STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
)  COMMENT '页面浏览路径'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_page_path/';

--最终版
insert into table ads_page_path
select
    '2020-06-14',
    recent_days,
    source,
    target,
    count(*) path_count
from
(
    select
        recent_days,
        concat('step-',rn,':',source) source,
        concat('step-',rn+1,':',target) target,
        session_id
    from
    (
        select
            mid_id,
            recent_days,
            page_id source,
            lead(page_id,1,null) over (partition by recent_days,session_id order by ts) target,
            session_id,
            row_number() over (partition by recent_days,session_id order by ts) rn
        from
        (
            select
                mid_id,
                recent_days,
                last_page_id,
                page_id,
                ts,
                concat(mid_id,'-',last_value(if(last_page_id is null,ts,null),true) over(partition by recent_days,mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
            from dwd_page_log
            lateral view explode(`array`(1,7,30)) tmp as recent_days
            where dt>=date_add('2020-06-14',-29)
            and dt>=date_add('2020-06-14',-recent_days+1)
        )t1
    )t2
)t3
group by recent_days,source,target;


--简化版
CREATE EXTERNAL TABLE ads_page_path
(
    `dt` STRING COMMENT '统计日期',
    `source` STRING COMMENT '跳转起始页面ID',
    `target` STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
)  COMMENT '页面浏览路径'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_page_path/';


select
    '2020-06-14',
    source,
    target,
    count(*) path_count
from
(
    select
        concat('step-',rn,':',source) source,
        concat('step-',rn+1,':',target) target,
        session_id
    from
    (
        select
            mid_id,
            page_id source,
            lead(page_id,1,null) over (partition by session_id order by ts) target,
            session_id,
            row_number() over (partition by session_id order by ts) rn
        from
        (
            select
                mid_id,
                last_page_id,
                page_id,
                ts,
                concat(mid_id,'-',last_value(if(last_page_id is null,ts,null),true) over(partition by mid_id order by ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) session_id
            from dwd_page_log
            where dt='2020-06-14'
        )t1
    )t2
)t3
group by source,target;

--建表语句
CREATE EXTERNAL TABLE `ads_user_total` (
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,0:累积值,1:最近1天,7:最近7天,30:最近30天',
  `new_user_count` BIGINT COMMENT '新注册用户数',
  `new_order_user_count` BIGINT COMMENT '新增下单用户数',
  `order_final_amount` DECIMAL(16,2) COMMENT '下单总金额',
  `order_user_count` BIGINT COMMENT '下单用户数',
  `no_order_user_count` BIGINT COMMENT '未下单用户数(具体指活跃用户中未下单用户)'
) COMMENT '用户统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_total/';

--简化版
select
    '2020-06-14',
    1 recent_days,
    sum(if(login_date_first='2020-06-14',1,0)) new_user_count,
    sum(if(order_date_first='2020-06-14',1,0)) new_order_user_count,
    sum(order_last_1d_final_amount) order_final_amount,
    sum(if(order_last_1d_count>0,1,0)) order_user_count,
    sum(if(login_date_last='2020-06-14' and order_last_1d_count=0,1,0)) no_order_user_count
from dwt_user_topic
where dt='2020-06-14'
union

select
    '2020-06-14',
    7 recent_days,
    sum(if(login_date_first>=date_add('2020-06-14',-6),1,0)) new_user_count,
    sum(if(order_date_first>=date_add('2020-06-14',-6),1,0)) new_order_user_count,
    sum(order_last_7d_final_amount) order_final_amount,
    sum(if(order_last_7d_count>0,1,0)) order_user_count,
    sum(if(login_date_last>=date_add('2020-06-14',-6) and order_last_7d_count=0,1,0)) no_order_user_count
from dwt_user_topic
where dt='2020-06-14'
union
select
    '2020-06-14',
    30 recent_days,
    sum(if(login_date_first>=date_add('2020-06-14',-29),1,0)) new_user_count,
    sum(if(order_date_first>=date_add('2020-06-14',-29),1,0)) new_order_user_count,
    sum(order_last_30d_final_amount) order_final_amount,
    sum(if(order_last_30d_count>0,1,0)) order_user_count,
    sum(if(login_date_last>=date_add('2020-06-14',-29) and order_last_30d_count=0,1,0)) no_order_user_count
from dwt_user_topic
where dt='2020-06-14'
union

select
    '2020-06-14',
    0 recent_days,
    sum(if(login_date_first>='1970-01-01',1,0)) new_user_count,
    sum(if(order_date_first>='1970-01-01',1,0)) new_order_user_count,
    sum(order_final_amount) order_final_amount,
    sum(if(order_count>0,1,0)) order_user_count,
    sum(if(login_date_last>='1970-01-01' and order_count=0,1,0)) no_order_user_count
from dwt_user_topic
where dt='2020-06-14';

insert into table ads_user_total
select
    '2020-06-14',
    recent_days,
    sum(if(login_date_first>=recent_days_ago,1,0)) new_user_count,
    sum(if(order_date_first>=recent_days_ago,1,0)) new_order_user_count,
    sum(order_amount) order_final_amount,
    sum(if(order_count>0,1,0)) order_user_count,
    sum(if(login_date_last>=recent_days_ago and order_count=0,1,0)) no_order_user_count
from
(
    select
        login_date_first,
        login_date_last,
        order_date_first,
        recent_days,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
            when recent_days=0 then order_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
            when recent_days=0 then order_final_amount
        end order_amount,
        if(recent_days=0,'1970-01-01',date_add('2020-06-14',-recent_days+1)) recent_days_ago
    from dwt_user_topic
    lateral view explode(`array`(0,1,7,30)) tmp as recent_days
    where dt='2020-06-14'
)t1
group by recent_days;

--用户变动统计 (流失 + 回流)
CREATE EXTERNAL TABLE `ads_user_change` (
  `dt` STRING COMMENT '统计日期',
  `user_churn_count` BIGINT COMMENT '流失用户数(新增)',
  `user_back_count` BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_change/';

insert into table ads_user_change
select
    '2020-06-14',
    churn.user_churn_count,
    back.user_back_count
from
(
    select
        '2020-06-14' dt,
        count(*) user_churn_count
    from dwt_user_topic
    where dt='2020-06-14'
    and login_date_last=date_add('2020-06-14',-7)
)churn
join
(
    select
        '2020-06-14' dt,
        count(*) user_back_count
    from
    (
        select
            user_id
        from dwt_user_topic
        where dt='2020-06-14'
        and login_date_last='2020-06-14'
    ) t1
    join
    (
        select
            user_id
        from dwt_user_topic
        where dt=date_add('2020-06-14',-1)
        and login_date_last<=date_add('2020-06-14',-8)
    )t2
    on t1.user_id=t2.user_id
)back
on churn.dt=back.dt;

--用户行为漏斗分析
CREATE EXTERNAL TABLE `ads_user_action` (
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `home_count` BIGINT COMMENT '浏览首页人数',
  `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
  `cart_count` BIGINT COMMENT '加入购物车人数',
  `order_count` BIGINT COMMENT '下单人数',
  `payment_count` BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_action/';

--最终版
insert into table ads_user_action
select
    '2020-06-14',
    tmp_cop.recent_days,
    tmp_page.home_count,
    tmp_page.good_detail_count,
    tmp_cop.cart_count,
    tmp_cop.order_count,
    tmp_cop.payment_count
from
(
    select
        '2020-06-14' dt,
        recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from
    (
        select
            recent_days,
            mid_id,
            page_id
        from dwd_page_log
        lateral view explode(`array`(1,7,30)) tmp as recent_days
        where dt>=date_add('2020-06-14',-recent_days+1)
        and page_id in ('home','good_detail')
        group by recent_days,mid_id,page_id
    )t1
    group by recent_days
) tmp_page
join
(
    select
        '2020-06-14' dt,
        recent_days,
        sum(if(cart_count>0,1,0)) cart_count,
        sum(if(order_count>0,1,0)) order_count,
        sum(if(payment_count>0,1,0)) payment_count
    from
    (
        select
            recent_days,
            case
                when recent_days=1 then cart_last_1d_count
                when recent_days=7 then cart_last_7d_count
                when recent_days=30 then cart_last_30d_count
            end cart_count,
            case
                when recent_days=1 then order_last_1d_count
                when recent_days=7 then order_last_7d_count
                when recent_days=30 then order_last_30d_count
            end order_count,
            case
                when recent_days=1 then payment_last_1d_count
                when recent_days=7 then payment_last_7d_count
                when recent_days=30 then payment_last_30d_count
            end payment_count
        from dwt_user_topic
        lateral view explode(`array`(1,7,30)) tmp as recent_days
        where dt='2020-06-14'
    ) t1
    group by recent_days
)tmp_cop
on tmp_page.dt=tmp_cop.dt
and tmp_page.recent_days=tmp_cop.recent_days;


--简化版
select
    '2020-06-14',
    tmp_page.home_count,
    tmp_page.good_detail_count,
    tmp_cop.cart_count,
    tmp_cop.order_count,
    tmp_cop.payment_count
from
(
    select
        '2020-06-14' dt,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from
    (
        select
            mid_id,
            page_id
        from dwd_page_log
        where dt='2020-06-14'
        and page_id in ('home','good_detail')
        group by mid_id,page_id
    )t1
) tmp_page
join
(
    select
        '2020-06-14' dt,
        sum(if(cart_last_1d_count>0,1,0)) cart_count,
        sum(if(order_last_1d_count>0,1,0)) order_count,
        sum(if(payment_last_1d_count>0,1,0)) payment_count
    from dwt_user_topic
    where dt='2020-06-14'
)tmp_cop
on tmp_page.dt=tmp_cop.dt;

--用户留存率
CREATE EXTERNAL TABLE ads_user_retention (
  `dt` STRING COMMENT '统计日期',
  `create_date` STRING COMMENT '用户新增日期',
  `retention_day` BIGINT COMMENT '截至当前日期留存天数',
  `retention_count` BIGINT COMMENT '留存用户数量',
  `new_user_count` BIGINT COMMENT '新增用户数量',
  `retention_rate` DECIMAL(16,2) COMMENT '留存率'
) COMMENT '用户留存率'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_retention/';

--简单版
--2020-06-14的计算任务
--1.计算2020-06-13的1日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-1) create_date,
    1,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-1)
union all
--2.计算2020-06-12的2日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-2) create_date,
    2,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-2)
union all
--3.计算2020-06-11的3日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-3) create_date,
    3,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-3)
union all
--4.计算2020-06-10的4日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-4) create_date,
    4,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-4)
union all
--5.计算2020-06-09的5日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-5) create_date,
    5,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-5)
union all
--6.计算2020-06-08的6日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-6) create_date,
    6,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-6)
union all
--7.计算2020-06-07的7日留存率
select
    '2020-06-14' dt,
    date_add('2020-06-14',-7) create_date,
    7,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first=date_add('2020-06-14',-7);

--最终版
insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2020-06-14' dt,
    login_date_first create_date,
    (unix_timestamp('2020-06-14','yyyy-MM-dd')-unix_timestamp(login_date_first,'yyyy-MM-dd'))/86400 retention_day,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first>=date_add('2020-06-14',-7)
and login_date_first<'2020-06-14'
group by login_date_first;

--商品统计
CREATE EXTERNAL TABLE `ads_order_spu_stats` (
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `spu_id` STRING COMMENT '商品ID',
    `spu_name` STRING COMMENT '商品名称',
    `tm_id` STRING COMMENT '品牌ID',
    `tm_name` STRING COMMENT '品牌名称',
    `category3_id` STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `category2_id` STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category1_id` STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `order_count` BIGINT COMMENT '订单数',
    `order_amount` DECIMAL(16,2) COMMENT '订单金额'
) COMMENT '商品销售统计'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_spu_stats/';

--简化版
select
    '2020-06-14' dt,
    spu_id,
    spu_name,
    tm_id,
    tm_name,
    category3_id,
    category3_name,
    category2_id,
    category2_name,
    category1_id,
    category1_name,
    sum(order_last_1d_count) order_count,
    sum(order_last_1d_final_amount) order_amount
from
(
    select
        sku_id,
        order_last_1d_count,
        order_last_1d_final_amount
    from dwt_sku_topic
    where dt='2020-06-14'
) t1
left join
(
    select
        id,
        spu_id,
        spu_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_info
    where dt='2020-06-14'
)t2
on t1.sku_id=t2.id
group by spu_id,spu_name,tm_id,tm_name,category3_id,category3_name,category2_id,category2_name,category1_id,category1_name;

--最终版
insert into table ads_order_spu_stats
select
    '2020-06-14' dt,
    recent_days,
    spu_id,
    spu_name,
    tm_id,
    tm_name,
    category3_id,
    category3_name,
    category2_id,
    category2_name,
    category1_id,
    category1_name,
    sum(order_count) order_count,
    sum(order_amount) order_amount
from
(
    select
        sku_id,
        recent_days,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_amount
    from dwt_sku_topic
    lateral view explode(`array`(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
) t1
left join
(
    select
        id,
        spu_id,
        spu_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name,
        tm_id,
        tm_name
    from dim_sku_info
    where dt='2020-06-14'
)t2
on t1.sku_id=t2.id
group by recent_days,spu_id,spu_name,tm_id,tm_name,category3_id,category3_name,category2_id,category2_name,category1_id,category1_name;

--品牌复购率
CREATE EXTERNAL TABLE `ads_repeat_purchase` (
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `tm_id` STRING COMMENT '品牌ID',
  `tm_name` STRING COMMENT '品牌名称',
  `order_repeat_rate` DECIMAL(16,2) COMMENT '复购率'
) COMMENT '品牌复购率'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_repeat_purchase/';

--简化版
select
    '2020-06-14' dt,
    tm_id,
    tm_name,
    sum(if(user_tm_count>=2,1,0))/sum(if(user_tm_count>=1,1,0))*100 order_repeat_rate
from
(
    select
        user_id,
        tm_id,
        tm_name,
        sum(user_sku_count) user_tm_count
    from
    (
        select
            user_id,
            sku_id,
            count(*) user_sku_count
        from dwd_order_detail
        where dt='2020-06-14'
        group by user_id,sku_id
    )t1
    left join
    (
        select
            id,
            tm_id,
            tm_name
        from dim_sku_info
        where dt='2020-06-14'
    )t2
    on t1.sku_id=t2.id
    group by user_id,tm_id,tm_name
)t3
group by tm_id,tm_name;

--最终版
select
    '2020-06-14' dt,
    recent_days,
    tm_id,
    tm_name,
    sum(if(user_tm_count>=2,1,0))/sum(if(user_tm_count>=1,1,0))*100 order_repeat_rate
from
(
    select
        recent_days,
        user_id,
        tm_id,
        tm_name,
        sum(user_sku_count) user_tm_count
    from
    (
        select
            recent_days,
            user_id,
            sku_id,
            count(*) user_sku_count
        from dwd_order_detail
        lateral view explode(`array`(1,7,30)) tmp as recent_days
        where dt>=date_add('2020-06-14',-recent_days+1)
        group by recent_days,user_id,sku_id
    )t1
    left join
    (
        select
            id,
            tm_id,
            tm_name
        from dim_sku_info
        where dt='2020-06-14'
    )t2
    on t1.sku_id=t2.id
    group by recent_days,user_id,tm_id,tm_name
)t3
group by recent_days,tm_id,tm_name;

--订单统计
CREATE EXTERNAL TABLE `ads_order_total` (
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `order_count` BIGINT COMMENT '订单数',
  `order_amount` DECIMAL(16,2) COMMENT '订单金额',
  `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '订单统计'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_total/';

--简化版
select
    '2020-06-14',
    sum(order_last_1d_count) order_count,
    sum(order_last_1d_final_amount) order_amount,
    sum(if(order_last_1d_count>0,1,0)) order_user_count
from dwt_user_topic
where dt='2020-06-14';

--最终版
insert into table ads_order_total
select
    '2020-06-14',
    recent_days,
    sum(order_count) order_count,
    sum(order_amount) order_amount,
    sum(if(order_count>0,1,0)) order_user_count
from
(
    select
        recent_days,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_amount
    from dwt_user_topic
    lateral view explode(`array`(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
)t1
group by recent_days;

--地区订单统计
CREATE EXTERNAL TABLE `ads_order_by_province` (
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `province_id` STRING COMMENT '省份id',
  `province_name` STRING COMMENT '省份名称',
  `area_code` STRING COMMENT '地区编码',
  `iso_code` STRING COMMENT '国际标准地区编码',
  `iso_code_3166_2` STRING COMMENT '国际标准地区编码',
  `order_count` BIGINT COMMENT '订单数',
  `order_amount` DECIMAL(16,2) COMMENT '订单金额'
) COMMENT '用户留存率'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_by_province/';

--简化版
select
    '2020-06-14',
    t1.province_id,
    t2.province_name,
    t2.area_code,
    t2.iso_code,
    t2.iso_3166_2,
    t1.order_last_1d_count,
    t1.order_last_1d_final_amount
from
(
    select
        province_id,
        order_last_1d_count,
        order_last_1d_final_amount
    from dwt_area_topic
    where dt='2020-06-14'
) t1
left join dim_base_province t2
on t1.province_id=t2.id;

--最终版
insert into table ads_order_by_province
select
    '2020-06-14',
    t1.recent_days,
    t1.province_id,
    t2.province_name,
    t2.area_code,
    t2.iso_code,
    t2.iso_3166_2,
    t1.order_count,
    t1.order_amount
from
(
    select
        province_id,
        recent_days,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_amount
    from dwt_area_topic
    lateral view explode(`array`(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
) t1
left join dim_base_province t2
on t1.province_id=t2.id;








