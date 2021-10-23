## HIVE的课堂笔记

#### 第一章.hive的基本概念

##### 1.1hive是什么

   hive是一个hadoop的数据仓库工具,它可以将hdfs上的结构化的数据映射成一张表,并提供了类sql(HQL)语法来分析数据

```sql
test
    tel        up   down
13855554444	  134   1568
13855554445   134   1566
13855554446   133   1565
13855554447   135   1561
13855554448   137   1568
13855554449   139   1568
```

按照每一行手机号 求一个上行流量和下行流量的和

```sql
select tel,up+down from test;
```

Hive其实是一个hadoop客户端,它本身不存储任何数据,它的数据存在hdfs上,hive能给这些在hdfs上数据加上元数据,元数据存在关系型数据库里(derby ,一般会选择把元数据存在mysql)

元数据:描述数据的数据(表名,字段名,类型,hdfs的路径)

hive的本质是将hql转化成mapreduce程序

在启动Hive之前需要起hadoop(yarn) 

Hive是写sql的工具,但是依赖hadoop

##### 1.2 hive的优缺点

1.优点:hive提供了类sql语法,降低了学习成本,降低了大数据的门槛,通用性高

2.缺点:不够智能,执行延迟高,调优困难,粒度较粗,不能支持行级别更新

##### 1.3 hive的架构

1.cli  cli(hive脚本) jdbc

2.元数据(默认放在derby),放在Mysql里面存储

3.hadoop（hdfs结构化数据存储,mr计算引擎）

4.driver

4.1解析器:将写好的hql翻译成ast树,判断语法是否正确,表是否存在,字段是否存在

4.2编译器:将翻译后的ast树,按照一定逻辑生成计划

4.3优化器:将生成逻辑做一定的优化

4.4执行器:将最终优化后的计划翻译成mr运行

5.运行机制

5.1创建的时候,会将你创建表的元数据存在元数据库中,如果hdfs上没有表的文件夹(会在hdfs上创建一个)并且会对应在hdfs上去找对应的数据

5.2查询的时候,会先去找你的元数据，再去找hdfs上的数据,最终翻译mr运行

##### 1.4 Hive和数据库区别

1.hive不是数据库 不是数据库 不是数据库 他们虽然都是用sql开发

数据延迟 数据规模 执行引擎 数据存储 

#### 第二章.安装

##### 2.1hive的常用交互命令

   常用在脚本里面,跑一些半夜可能需要跑的任务

```
1 hive -e 能执行一条命令行的sql
hive -e 'select *  from stu'
2. hive -f 能够执行sql文本 
hive -f hive.sql
```

##### 2.2hive的参数配置方式

hive查看参数的方式

```sql
在hive/beeline 使用 set; 能够查看所有的配置项
在hive/beeline 使用 set 参数名 能查看指定参数名的配置项
```

修改参数配置的方式

1.永久生效

```sql
在你的Hive/conf下面所有的文件都可以改hive的参数  hive-site.xml hive-env.sh hive-log4j2.properties
```

2.临时生效(对单次客户端生效)

```sql
hive -hiveconf 参数名=参数值
beeline -u jdbc:hive2://hadoop102:10000 -n atguigu -hiveconf 参数名=参数值
```

3.临时生效(对单次客户端生效)

```sql
在hive/beeline 里面使用 set 参数名=参数值  
```

他们的优先级是按照 1 2 3 的顺序依次增大的

#### 第三章.hive的数据类型

##### 3.1 基本数据类型

| HIVE      | MySQL     | JAVA    | 长度                                                 | 例子                                 |
| --------- | --------- | ------- | ---------------------------------------------------- | ------------------------------------ |
| TINYINT   | TINYINT   | byte    | 1byte有符号整数                                      | 2                                    |
| SMALINT   | SMALINT   | short   | 2byte有符号整数                                      | 20                                   |
| INT       | INT       | int     | 4byte有符号整数                                      | 20                                   |
| BIGINT    | BIGINT    | long    | 8byte有符号整数                                      | 20                                   |
| BOOLEAN   | 无        | boolean | 布尔类型，true或者false                              | TRUE  FALSE                          |
| FLOAT     | FLOAT     | float   | 单精度浮点数                                         | 3.14159                              |
| DOUBLE    | DOUBLE    | double  | 双精度浮点数                                         | 3.14159                              |
| STRING    | VARCHAR   | string  | 字符系列。可以指定字符集。可以使用单引号或者双引号。 | ‘now is the time’ “for all good men” |
| TIMESTAMP | TIMESTAMP |         | 时间类型                                             |                                      |
| BINARY    | BINARY    |         | 字节数组                                             |                                      |

对于Hive的String类型相当于数据库的varchar类型，该类型是一个可变的字符串，不过它不能声明其中最多能存储多少个字符，理论上它可以存储2GB的字符数。

做一个数据类型测试

```sql
create table person(id int ,name string,weight double,money bigint);
insert into person values(1,'qiangge',80,999999999999);
```

##### 3.2 集合数据类型

集合数据类型测试

```sql
songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long guan_beijing_10010
yangyang,caicai_susu,xiao yang:18_xiaoxiao yang:19,chao yang_beijing_10011

create table test(
 name string,
 friends array<string>,
 children map<string,int>,
 address struct<street:string,city:string,email:int>
)
row format delimited fields terminated by ','    --字段分隔符
collection items terminated by '_'               --集合元素分隔符
map keys terminated by ':'                       --map的kv分隔符
lines terminated by '\n';                        --每一行数据分隔符
```

查 songsong 的第一个朋友 xiao song 年龄 ,邮编

```sql
select name,friends[0],children['xiao song'],address.email from test where name ='songsong';

select name,friends[0],children['xiao song'],address.email from test;
```

查 songsong 的第一个朋友,第一个孩子 ,邮编

```sql
select name,friends[0],map_keys(children)[0],address.email from test where name ='songsong';
select name,friends[0],map_values(children)[0],address.email from test where name ='songsong';
```

##### 3.3 Hive 里面的类型转换

```sql
1. double在Hive里面是最大 string可以转成double
2. 强制类型转化  cast('1' as type)   
```

#### 第四章.DDL(数据定义语言)

##### 4.1 库的ddl

###### 4.1.1 创建库的语法

```sql
CREATE DATABASE [IF NOT EXISTS] database_name      --创建数据库 if not exists 加强健壮性
[COMMENT database_comment]                         --注释  解释当前干什么事的          
[LOCATION hdfs_path]                               --指定当前库在hdfs上对应的文件夹    
[WITH DBPROPERTIES (property_name=property_value, ...)]; --库的属性和值，但是一点用没有(鸡肋)
```

###### 4.1.2 增

```sql
create database db_hive
comment 'this in my first db'
with dbproperties('dbtype'='hive','owner'='atguigu');

create database db_hive2
location '/db_hive2';

create database db_hive3
location '/dsadsadsasd';

create database if not exists db_hive2
location '/db_hive2';

--在你不指定的location的情况下 默认在你的hdfs/user/hive/warehouse下创建一个以database_name.db名的文件夹 来当做库
--在你指定location的情况下 拿最后一级目录当做库的名字
```

###### 4.1.3 查

```sql
--展示所有的数据库
show databases; 
--模糊展示
show databases like 'db_hive*'
--描述数据库    --不会展示 库的属性 dbproperties
desc database 库名
desc database db_hive; 
--描述数据库详情 --会展示 库的属性  dbproperties parameters
desc database extended 库名;
desc database extended db_hive;
```

###### 4.1.4 切换数据库

```sql
use 库名
use db_hive2;
```

###### 4.1.5 改

```sql
用户可以使用ALTER DATABASE命令为某个数据库的DBPROPERTIES设置键-值对属性值，来描述这个数据库的属性信息。数据库的其他元数据信息都是不可更改的，包括数据库名和数据库所在的目录位置。

alter database db_hive set dbproperties('dbtype'='db');  --修改原来的属性
alter database db_hive set dbproperties('createtime'='2020-08-19');  --增加原来的属性
```

###### 4.1.6 删

```sql
drop database 库名
drop database db_hive2;

drop database if exists db_hive2; --加上 if exists 增加代码的健壮性

drop database db_hive cascade; --强制删除 (当你库下面有表的时候) 慎用(只有你确定所有表都没用的时候)
```

##### 4.2 表的ddl

###### 4.2.1创建表的语法

```sql
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name --EXTERNAL 决定表的类型是否外部表
[(col_name data_type [COMMENT col_comment], ...)]  --字段名，字段类型，字段注释
[COMMENT table_comment]                            --表的注释
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]  --分区表
[CLUSTERED BY (col_name, col_name, ...)                           --分桶表
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]  --分几个桶 桶内排序字段
[
ROW FORMAT DELIMITED             --当前表对应的数据的分隔符
 [FIELDS TERMINATED BY char]     --字段分隔符  
  有默认值  对应ascii码表0001号位 ^A  怎么敲出来的ctrl+v ctrl+a
  songsong,bingbing_lili,xiao song:18_xiaoxiao song:19,hui long guan_beijing_10010
  对于上一行数据 分隔符就是 ','
 [COLLECTION ITEMS TERMINATED BY char] --集合元素分隔符（array map struct）
  有默认值  对应ascii码表0010号位 ^B  怎么敲出来的ctrl+v ctrl+b
    bingbing_lili  xiao song:18_xiaoxiao song:19 hui long guan_beijing_10010
  对于上述集合 分隔符是'_'
 [MAP KEYS TERMINATED BY char]         --map的kv分隔符
  有默认值  对应ascii码表0011号位 ^C  怎么敲出来的ctrl+v ctrl+c
  xiao song:18
  对于上述kv而言 分隔符是 ":"
 [LINES TERMINATED BY char]            --各行数据分隔符  
  有默认值'\n'
] 
[STORED AS file_format]     --当前表所对应的数据的类型 textfile
[LOCATION hdfs_path]        --当前表所对应的hdfs路径
[TBLPROPERTIES (property_name=property_value, ...)] --表的属性 有大用
[AS select_statement]       --根据查询结果创建一张表
[LIKE table_name]           --模仿一张表
```

###### 4.2.2 增

4.2.2.1 增加内部表(管理表)

```sql
内部表的含义:hive掌握着表的数据的生命周期,当在Hive里删除表的时候,会一并把hdfs上数据给删了
用的少  1.中间表 2.测试表
--内部表测试
create table student(id int, name string)
row format delimited fields terminated by '\t'
--默认分隔符测试
create table test2(id int, name string)
--根据查询结构创建一张表  它虽然会带表结构和数据 但是分隔符不会带 会使用默认值
create table student2 as select * from student;
--根据查询结构创建一张表  创建一张相同分隔符的
create table student3 row format delimited fields terminated by '\t' as select * from student;
--根据存在的表的结构来创建一张表  拿不到数据 --他的分隔符跟模仿表的是一样的
create table student4 like student;
```

4.2.2.2 增加外部表

```sql
外部表的含义:hive不掌握着表的数据生命周期,当在Hive里删除表的时候,不会一并把hdfs上数据给删了，只会删除元数据
除了上述内部表的情况 全是外部表
create external table if not exists dept(
deptno int,
dname string,
loc int)
row format delimited fields terminated by '\t'
location '/company/dept';

create external table if not exists emp(
empno int,
ename string,
job string,
mgr int,
hiredate string, 
sal double, 
comm double,
deptno int)
row format delimited fields terminated by '\t'
location '/company/emp/';
```

4.2.2.3 内部表和外部表相互转换

```sql
Table Type:            EXTERNAL_TABLE       
Table Parameters:      EXTERNAL            TRUE 
表是否为内部表还是外部表是由Table Parameters 里面的EXTERNAL属性来控制 包括TRUE和FALSE 都得大写
--内部表转换成外部表
alter table student4 set tblproperties('EXTERNAL'='TRUE');
--外部表转成内部表
alter table emp set tblproperties('EXTERNAL'='FALSE');
```

###### 4.2.3 查

```sql
--展示库下面的所有表
show tables;
--描述表
desc student;
--描述表的详情
desc formatted student;
```

###### 4.2.4 删

```sql
1.删除表
1.1 删除内部表 
drop table student;
1.2 删除外部表
drop table dept;  --只能删除元数据 不能删除hdfs上的数据
1.3 清空表
truncate table student3;
清空外部表测试      --不能清空外部表
truncate table emp;
```

###### 4.2.5 改

```sql
--改表名  会连同你的hdfs文件夹名字一起改掉
alter table student3 rename to student2;
--更新列  注意改的列的数据类型 只能由小往大改 或者不变
ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
alter table stu2 change column id id int;
alter table stu2 change column id id tinyint; --这是错的
alter table stu2 change column id ids bigint;
alter table stu2 change column id idss bigint;
-- 增加列
ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment], ...) 
alter table stu2 add columns(weight double,hair bigint);
-- 替换列
ALTER TABLE table_name REPLACE COLUMNS (col_name data_type [COMMENT col_comment], ...) 
-- 替换之减少列  如果你想替换时候较少列 那么你减少后剩余部分 应该和之前字段 满足类型的大小关系
alter table stu2 replace columns (id bigint , name string);
-- 替换之增加列  增加部分可以没有类型大小的关系 ，如果有对应的部分则满足类型大小的对应关系
alter table stu2 replace columns (id bigint , name string , height double, hair bigint);
```

#### 第五章.DML（数据操作语言）

##### 5.1 数据导入

###### 5.1.1 load 装载数据

```sql
load data [local] inpath '数据的path' [overwrite] into table student [partition (partcol1=val1,…)];
测试表
create table student (id int ,name string) row format delimited fields terminated by '\t';
--load 数据之追加数据   本地导入 是复制进去的
load data local inpath '/opt/module/hive/datas/student.txt' into table student;
--load 数据之覆盖数据
load data local inpath '/opt/module/hive/datas/student1.txt' overwrite into table student;
--load 数据之hdfs导入  hdfs导入时剪切进去的
load data  inpath '/student.txt' into table student;
```

###### 5.1.2 insert 插入数据

```sql
--追加插入
insert into table student2 values(1,'banzhang'),(2,'haiwangbin');
--覆盖插入
insert overwrite table student values(1,'banzhang'),(2,'haiwangbin');
--查询插入  --注意:第一你所插入的表必须存在 然后你查询的字段必须满足目标表的里的字段数
insert into table student  select id,name from student3;
--查询覆盖
insert overwrite table student  select id,name from student3;
```

###### 5.1.3 as select

```sql
create table if not exists student3
as select id, name from student;

create as select, insert into table table_name select这两个就是拿来创建中间表
 
```

###### 5.1.4 location

```sql
create  table if not exists student4(
id int, name string
)
row format delimited fields terminated by '\t'
location '/student4';
--指定location 必须是文件夹
```

###### 5.1.5 import 导入（必须是export导出 并且导入的表不能存在）

```sql
import table student6 from '/user/hive/warehouse/export/student'
```



##### 5.2 数据导出(少)

###### 5.2.1 insert 导出

```sql
--无格式导出
insert overwrite local directory '/opt/module/hive/datas/export/student1' select * from student;
--有格式导出
insert overwrite local directory '/opt/module/hive/datas/export/student1' row format delimited fields terminated by '\t' select * from student;
--没有local 写在hdfs上
insert overwrite  directory '/opt/module/hive/datas/export/student1' row format delimited fields terminated by '\t' select * from student;
```

###### 5.2.2 hadoop 下载

```sql
hadoop fs  -get /user/hive/warehouse/student/student.txt
/opt/module/hive/datas/export/student3.txt;
```

###### 5.2.3 hive 的shell命令

```sql
hive -e 'select * from default.student;' > /opt/module/hive/datas/export/student4.txt
```

###### 5.2.4 export 导出

```sql
export table student to '/student';
```

##### 第六章 查询

###### 6.1 查询简介

```sql
SELECT [ALL | DISTINCT] select_expr, select_expr, ...   distinct 对结果集去重
  FROM table_reference                                  从xxx表查询
  [WHERE where_condition]                               过滤条件
  [GROUP BY col_list]                                   以xxx分组
   [HAVING col_list]                                    分组后过滤
  [ORDER BY col_list]                                   全局排序
  [CLUSTER BY col_list
    | [DISTRIBUTE BY col_list] [SORT BY col_list]       hive 里的4个by
  ]
 [LIMIT number]                                        限制输出的行数 翻页
 
 
 select
  count(*)
 from  join  where group by having order by limit

 from<join<where<group by <count(*)<having<select<order by <limit sql的执行顺序
```

```sql
数据含义讲解
create table if not exists emp(
empno int,      --员工编号
ename string,   --员工姓名
job string,     --员工职位
mgr int,        --员工领导
hiredate string,--员工的入职日期 
sal double,     --员工的薪资
comm double,    --员工的奖金
deptno int)     --员工的部门编号
row format delimited fields terminated by '\t';
```

```sql
select
 empno id,
 ename name
from emp e
```

###### 6.2 group by 

计算emp表每个部门的平均工资

```sql
select
 deptno,
 avg(sal)
from 
  emp
group by
  deptno
```

计算emp每个部门中每个岗位的最高薪水

```sql
select
 deptno,
 job,
 max(sal)
from 
   emp
group by
   deptno,job
```

求每个部门的平均薪水大于2000的部门

```sql
select
 deptno,
 avg(sal) avg_sal
from emp
group by deptno
having avg_sal>2000;
```

###### 6.3 join

```sql
根据员工表和部门表中的部门编号相等，查询员工编号、员工名称和部门名称；

select
e.deptno
from emp e join dept d
on e.deptno=d.deptno

select
*
from emp e join dept d
on e.deptno!=d.deptno

左外连接

select
*
from emp e left join dept d
on e.deptno=d.deptno


select
e.*,
d.*
from dept d left join emp e
on d.deptno=e.deptno

右外连接
select
*
from emp e right join dept d
on e.deptno=d.deptno


满外连接
select
 e.*,
 d.*
from emp e full join dept d
on e.deptno =d.deptno

--在Mysql里面的实现方式
select
*
from dept d left join emp e
on d.deptno=e.deptno
union
select
*
from dept d right join emp e
on d.deptno=e.deptno

union 竖向拼接两张表  可以将相同数据去重
union all 竖向拼接两张表  直接拼接不去重
union all 效率更高 union往往是我们想要的结果

要 员工姓名，部门名称，位置名称
--多表连接
select
 e.ename,
 d.dname,
 l.loc_name
from emp e join dept d
on e.deptno=d.deptno
join location l
on d.loc=l.loc

select
 e.ename,
 d.dname,
 l.loc_name
from emp e join dept d join location l
on e.deptno=d.deptno and d.loc=l.loc

--笛卡尔积(千万注意)
select * from dept join emp;
select * from dept,emp;
select * from dept join emp on 1=1;
```

###### 6.4排序

```sql
order by  全局排序  只会起一个reducer对你结果集进行

--按照人员的薪资排序
select 
*
from emp
order by sal desc

asc 升序 (默认)
desc 倒序

--按照部门的人员薪资排序
select
 *
from emp
order by deptno,sal

select
 *
from emp
order by deptno desc ,sal desc

select
ename,
sal,
comm,
sal+comm
from emp;


--distribute by （分区） and sort by（区内排序）
insert overwrite local directory '/opt/module/hive/datas/distribute-result'
select
*
from
emp 
distribute by cast(deptno/10 as int) sort by sal desc

--cluster by   分区排序
select * from
emp cluster by deptno;

order by 表示全局排序
distribute by(分区) sort by（区内排序）  他两是在一起使用
cluster by（既分区又排序）  是distribute by sort by 相同字段的时候可以简写  但是用的少
```

##### 第七章 分区和分桶

##### 7.1分区表

```sql
Hive里有个很大毛病 它没有索引 ,它每次扫描都只能扫描全表
```

```sql
分区表测试
create table dept_partition(
deptno int, dname string, loc string
)
partitioned by (day string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/hive/datas/dept_20200401.log' into table dept_partition ;  --错误的 (虽然能运行)
load data local inpath '/opt/module/hive/datas/dept_20200401.log' into table dept_partition partition(day='20200401');
load data local inpath '/opt/module/hive/datas/dept_20200402.log' into table dept_partition partition(day='20200402');
load data local inpath '/opt/module/hive/datas/dept_20200403.log' into table dept_partition partition(day='20200403');

分区表
其实就是在分文件夹 , 但是他又可以当做一个列来使用，帮助我们定位数据位置,不需要再暴力扫描全表了
创建的时候 分区字段一定不能是表里面存在的列
create table dept_partition2(
deptno int, dname string, loc string
)
partitioned by (deptno string)
row format delimited fields terminated by '\t';
```

###### 7.1.1  增

```sql
alter table dept_partition add partition(day = '20200404');
alter table dept_partition add partition(day = '20200405')partition(day='20200406');
```

###### 7.1.2 查

```sql
show partitions dept_partition;
show partitions dept;  --不能查一个不是分区表的表
desc dept_partition;
desc formatted dept_partition;
```

###### 7.1.3 删（对于外部表 只能删分区的元数据信息 hdfs文件夹会保留）

```sql
alter table dept_partition drop partition(day = '__HIVE_DEFAULT_PARTITION__')
alter table dept_partition drop partition(day='20200405'),partition(day='20200406')
```

###### 7.1.4 二级分区

```sql
导入数据
load data local inpath '/opt/module/hive/datas/dept_20200401.log' into table
dept_partition2 partition(day='20200401',hour = '13');

load data local inpath '/opt/module/hive/datas/dept_20200402.log' into table
dept_partition2 partition(day='20200401',hour = '14');

load data local inpath '/opt/module/hive/datas/dept_20200403.log' into table
dept_partition2 partition(day='20200402',hour = '13');

--增
alter table dept_partition2 add partition(day = '20200402',hour='13') ;

--删
alter table dept_partition2 drop partition(day = '20200401') ;
```

###### 7.1.5 让分区表和下面的分区文件夹产生关系三种方式

```
1.修复(能够自动扫描对应的表的文件夹下面符合规则的文件夹并添加元数据)
msck repair table dept_partition2;
2.手动添加一个分区信息
alter table dept_partition2 add partition(day = '20200404',hour='13') ;
3.load 数据到一个指定分区里面
load data local inpath '/opt/module/hive/datas/dept_20200401.log' into table
dept_partition2 partition(day='20200405',hour='13');
```

###### 7.1.6动态分区（能够根据数据中的最后的列 来放到不同分区目录下）

```sql
在hive 2.x版本 动态分区是不能拿load来做  查询后插入
insert into table dept_partition partition(day) select deptno,dname,loc,day from dept1;
在hive 3.x版本 动态分区是直接拿load来做  优化
load data local inpath '/opt/module/hive/datas/dept_20200401.log' into table dept_partition
--二级分区
create table dept_partition_dy2(id int) partitioned by (name string,loc int) row format delimited fields terminated by '\t';

load data local inpath '/opt/module/hive/datas/dept.txt' into table dept_partition_dy2;

insert into table dept_partition_dy2 partition(name,loc)  select deptno, dname,loc from dept;
但是要记住 严格模式 是在你指定partition 时候才有效果
```

##### 7.2分桶（分的是你具体的数据）

```sql
create table stu_buck(id int, name string)
clustered by(id) 
into 4 buckets
row format delimited fields terminated by '\t';
```

###### 7.2.1创建一个又分区又分桶的表

```sql
create table stu_buck_part(id int, name string)
partitioned by (day string)
clustered by(id) 
into 4 buckets
row format delimited fields terminated by '\t';

load data  inpath '/student.txt' into table stu_buck_part partition(day = '20201109')
                                                                    
```

###### 7.2.2分区和分桶的区别

```sql
1. 分区分的是目录  分桶分的是具体的数据
2. 分区字段必不能再创建表的字段里 分桶字段必在创建的字段里
```

#### 第八章 函数

```sql
1. 显示系统自带所有函数
show functions;
2. 描述指定函数的作用
desc function 函数名;
3. 描述函数的详情信息(一般是都是有例子的)
desc function extended 函数名;
```

###### 8.1 NVL

```sql
1.将奖金为null的人奖金替换0
select ename,comm,nvl(comm,0) from emp;
2.按照奖金和工资的和 降序排序
select
  ename,
  sal,
  comm,
  nvl(comm,0),
  sal+nvl(comm,0) s_n
from emp
  order by s_n desc;


3.当奖金为Null时 用领导id 替代
select
 ename,
 comm,
 sal,
 nvl(comm,nvl(mgr,0))
from emp
```

###### 8.2 case when

```sql
1.不管格式
select
 dept_id,
 sex,
 count(*)
from emp_sex
group by dept_id,sex

+----------+------+------+
| dept_id  | sex  | _c2  |
+----------+------+------+
| A        | 女    | 1    |
| A        | 男    | 2    |
| B        | 女    | 2    |
| B        | 男    | 1    |

2.格式
dept_Id     男       女
A     		2       1
B     		1       2

+----------+------+------+
| dept_id  | sex  | sex  |
+----------+------+------+
| A        | 男    | 男    |
| A        | 男    | 男    |
| B        | 男    | 男    |
| A        | 女    | 女    |
| B        | 女    | 女    |
| B        | 女    | 女    |
+----------+------+------+
select
  dept_id,
  case sex when '男' then 1 else 0 end male,
  case sex when '女' then 1 else 0 end female
from emp_sex
----------+-------+---------+
| dept_id  | male  | female  |
+----------+-------+---------+
| A        | 1     | 0       |
| A        | 1     | 0       |
| B        | 1     | 0       |
| A        | 0     | 1       |
| B        | 0     | 1       |
| B        | 0     | 1       |
+----------+-------+---------+
select
 t1.dept_id,
 sum(t1.male) male,
 sum(t1.female)female
from (
  select
  dept_id,
  case sex when '男' then 1 else 0 end male,
  case sex when '女' then 1 else 0 end female
from emp_sex
)t1
group by t1.dept_id

select
 dept_id,
 sum( case sex when '男' then 1 else 0 end) male,
 sum(case sex when '女' then 1 else 0 end) female
from emp_sex
group by dept_id
-+---------+
| dept_id  | male  | female  |
+----------+-------+---------+
| A        | 2     | 1       |
| B        | 1     | 2       |
+----------+-------+---------+
select
  dept_id,
  sum(if(sex='男',1,0)) male,
  sum(if(sex='女',1,0)) female
from emp_sex
group by dept_id
+----------+-------+---------+
| dept_id  | male  | female  |
+----------+-------+---------+
| A        | 2     | 1       |
| B        | 1     | 2       |
--支付
case pay when '支付宝' then 1 when '微信' then 2  when '信用卡 ' then 3 else 0 end
```

###### 8.3 行转列

```sql
1. concat  
select concat(empno,'-',ename,'-',sal,'-',deptno) from emp;

2.concat_ws CONCAT_WS must be "string or array<string>"

select concat_ws('-',cast(empno as string),ename,cast(sal as string)) from emp;
--按组统计 每个组有多少人 分别是谁
select
 deptno,
 count(*),
 collect_list(ename)
from emp
group by deptno
--对emp_sex这张表 需要如下统计
a   悟空 大海 凤姐
b   宋宋 婷姐 婷婷

3.collect_set（对结果集去重）
select
 dept_id,
 collect_set(name)
from emp_sex
group by dept_id

4.collect_list（不对结果集去重）
--两步
select
 name,
 concat(constellation,',',blood_type) c_b
from person_info

select
 t1.c_b,
 concat_ws("|",collect_list(t1.name))
from (
   select
    name,
    concat(constellation,',',blood_type) c_b
   from person_info 
)t1
group by t1.c_b

--一步写完
select
  concat(constellation,',',blood_type) ,
  concat_ws("|",collect_list(name))
from person_info
group by concat(constellation,',',blood_type) 
+--------+----------+
|  _c0   |   _c1    |
+--------+----------+
| 射手座,A  | 大海|凤姐 |
| 白羊座,A  | 孙悟空|猪八戒 |
| 白羊座,B  | 宋宋|苍老师 |
+--------+----------+
```

###### 8.4 列转行

```sql
Split(str, separator)：将字符串按照后面的分隔符切割，转换成字符array。
EXPLODE(col)：将hive一列中复杂的array或者map结构拆分成多行。
LATERAL VIEW

--尝试炸开
select
movie,
EXPLODE(split(category,',')) 
from movie_info
| col  |
+------+
| 悬疑   |
| 动作   |
| 科幻   |
| 剧情   |
| 悬疑   |
| 警匪   |
| 动作   |
| 心理   |
| 剧情   |
| 战争   |
| 动作   |
| 灾难   |

select 
movie
from movie_info
+--------------+
|    movie     |
+--------------+
| 《疑犯追踪》       |
| 《Lie to me》  |
| 《战狼2》        |
+--------------+
--尝试join
select
EXPLODE(split(category,','))  
from movie_info  t1

select 
movie
from movie_info t2

select
  t2.movie,
  t1.*
from(
    select
      EXPLODE(split(category,','))  
    from movie_info 
) t1 right join (
    select 
      movie
    from movie_info
)t2 

|   t2.movie   | t1.category  |
+--------------+--------------+
| 《疑犯追踪》       | 悬疑           |
| 《疑犯追踪》       | 动作           |
| 《疑犯追踪》       | 科幻           |
| 《疑犯追踪》       | 剧情           |
| 《疑犯追踪》       | 悬疑           |
| 《疑犯追踪》       | 警匪           |
| 《疑犯追踪》       | 动作           |
| 《疑犯追踪》       | 心理           |
| 《疑犯追踪》       | 剧情           |
| 《疑犯追踪》       | 战争           |
| 《疑犯追踪》       | 动作           |
| 《疑犯追踪》       | 灾难           |
| 《Lie to me》  | 悬疑           |
| 《Lie to me》  | 动作           |
| 《Lie to me》  | 科幻           |
| 《Lie to me》  | 剧情           |
| 《Lie to me》  | 悬疑           |
| 《Lie to me》  | 警匪           |
| 《Lie to me》  | 动作           |
| 《Lie to me》  | 心理           |
| 《Lie to me》  | 剧情           |
| 《Lie to me》  | 战争           |
| 《Lie to me》  | 动作           |
| 《Lie to me》  | 灾难           |
| 《战狼2》        | 悬疑           |
| 《战狼2》        | 动作           |
| 《战狼2》        | 科幻           |
| 《战狼2》        | 剧情           |
| 《战狼2》        | 悬疑           |
| 《战狼2》        | 警匪           |
| 《战狼2》        | 动作           |
| 《战狼2》        | 心理           |
| 《战狼2》        | 剧情           |
| 《战狼2》        | 战争           |
| 《战狼2》        | 动作           |
| 《战狼2》        | 灾难           |
+--------------+--------------+
--侧写表
select
movie,
category_name
from movie_info
lateral view explode(split(category,",")) tmp as category_name
where category_name='悬疑'
+--------------+----------------+
|    movie     | category_name  |
+--------------+----------------+
| 《疑犯追踪》       | 悬疑             |
| 《Lie to me》  | 悬疑             |
+--------------+----------------+
+--------------+----------------+
|    movie     | category_name  |
+--------------+----------------+
| 《疑犯追踪》       | 悬疑             |
| 《疑犯追踪》       | 动作             |
| 《疑犯追踪》       | 科幻             |
| 《疑犯追踪》       | 剧情             |
| 《Lie to me》  | 悬疑             |
| 《Lie to me》  | 警匪             |
| 《Lie to me》  | 动作             |
| 《Lie to me》  | 心理             |
| 《Lie to me》  | 剧情             |
| 《战狼2》        | 战争             |
| 《战狼2》        | 动作             |
| 《战狼2》        | 灾难             |
+--------------+----------------+
```

###### 8.5 窗口函数(开窗函数)

```sql
1.什么叫窗口函数 
窗口函数一个高阶函数 mysql 5.6 5.7 没有  mysql 5.8有了 但是收费的
orcale 一直有 一直收费
hive 也有 开源
2.哪些函数是窗口函数
2.1 窗口函数
lead
lag
first_value
last_value
2.2聚合函数
sum
count
avg
max
min
2.3排名分析函数
rank
row_number
dense_rank
ntile
3.窗口函数的定义
窗口+函数  
窗口表示限定函数的计算范围
窗口函数是一行一行走的
4.窗口函数的语法
窗口函数()+over([partition by 字段...][order by 字段...][窗口子句])
over表示开窗

窗口子句
(ROWS | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECEDING | CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN CURRENT ROW AND (CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN [num] FOLLOWING AND (UNBOUNDED | [num]) FOLLOWING
窗口子句是有默认值的
When ORDER BY is specified with missing WINDOW clause, the WINDOW specification defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
当有order by 但是缺少窗口子句的时候 窗口的默认范围是上无边界到当前行
When both ORDER BY and WINDOW clauses are missing, the WINDOW specification defaults to ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
当既没有窗口子句又没有order by的时候 窗口默认的范围是上无边界到下无边界
```

8.5.1 需求1 查询在2017年4月份购买过的顾客及总人数  总人次

```sql
查询在2017年4月份购买过的顾客及总人数
1.需要先过滤出4月份购买的顾客数据
--第一种过滤
 select
 *
 from business
 where month(orderdate)='4';
--第二种过滤 
  select
 *
 from business
 where substring(orderdate,1,7)='2017-04';
--第三种方式
  select
 *
 from business
 where date_format(orderdate,'yyyy-MM')='2017-04';
--需求1 第一种
 select
 name,
 count(*)over(rows between UNBOUNDED PRECEDING and UNBOUNDED following)
 from business
 where date_format(orderdate,'yyyy-MM')='2017-04'
 group by name
--需求1 第二种
 select
 name,
 count(*)over()
 from business
 where date_format(orderdate,'yyyy-MM')='2017-04'
 group by name

--需求1 不用窗口函数怎么写(要你所有月份的总人数 和 人)

select 
name,
date_format(orderdate,'yyyy-MM') ordermonth,
cost
from business t1

select
t1.ordermonth,
collect_set(t1.name) n_c,
size(collect_set(t1.name))
from (
    select 
     name,
     date_format(orderdate,'yyyy-MM') ordermonth,
     cost
    from business
)t1
group by t1.ordermonth
| t1.orderdate  |       n_c        | p_c  |1
+---------------+------------------+------+
| 2017-01       | ["jack","tony"]  | 2    |
| 2017-02       | ["jack"]         | 1    |
| 2017-04       | ["jack","mart"]  | 2    |
| 2017-05       | ["neil"]         | 1    |
| 2017-06       | ["neil"]         | 1    |
+---------------+------------------+------+
需求1变种1  查询在2017年4月份购买过的顾客及累计人数
select
name,
count(*)over(rows between UNBOUNDED PRECEDING and UNBOUNDED following)
from business
where substring(orderdate,1,7)='2017-04'
group by name
+-------+-----------------+
| mart  | 1               |
| jack  | 2               |
+-------+-----------------+
需求1变种2  查询在2017年4月份购买过的顾客及总人次
select
name,
orderdate,
cost,
count(*)over(rows between UNBOUNDED PRECEDING and current row)
from business
where month(orderdate)='4'

需求1变种3  查询在购买过的顾客及总人次
select
name,
orderdate,
cost,
count(*)over(rows between UNBOUNDED PRECEDING and UNBOUNDED following)
from business
需求1变种4  查询在购买过的顾客及累加人次
select
name,
orderdate,
cost,
count(*)over(rows between UNBOUNDED PRECEDING and current row)
from business
需求1变种5  查询在购买过的顾客及总人数/累计人数
select
name,
count(*)over(rows between UNBOUNDED PRECEDING and UNBOUNDED following)
from business
group by name
```

8.5.2需求2 查询顾客的购买明细及月购买总额

```sql
select
 name,
 orderdate,
 cost,
 sum(cost)over(partition by name,month(orderdate))
from business
+-------+-------------+-------+---------------+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-05  | 46    | 111           |
| jack  | 2017-01-08  | 55    | 111           |
| jack  | 2017-01-01  | 10    | 111           |
| jack  | 2017-02-03  | 23    | 23            |
| jack  | 2017-04-06  | 42    | 42            |
| mart  | 2017-04-13  | 94    | 299           |
| mart  | 2017-04-11  | 75    | 299           |
| mart  | 2017-04-09  | 68    | 299           |
| mart  | 2017-04-08  | 62    | 299           |
| neil  | 2017-05-10  | 12    | 12            |
| neil  | 2017-06-12  | 80    | 80            |
| tony  | 2017-01-04  | 29    | 94            |
| tony  | 2017-01-02  | 15    | 94            |
| tony  | 2017-01-07  | 50    | 94            |
+-------+-------------+-------+---------------+


需求2 变种1  查询顾客的购买明细及购买总额
select
name,
orderdate,
cost,
sum(cost)over(partition by name )
from business;

| jack  | 2017-01-05  | 46    | 176           |
| jack  | 2017-01-08  | 55    | 176           |
| jack  | 2017-01-01  | 10    | 176           |
| jack  | 2017-04-06  | 42    | 176           |
| jack  | 2017-02-03  | 23    | 176           |
| mart  | 2017-04-13  | 94    | 299           |
| mart  | 2017-04-11  | 75    | 299           |
| mart  | 2017-04-09  | 68    | 299           |
| mart  | 2017-04-08  | 62    | 299           |
| neil  | 2017-05-10  | 12    | 92            |
| neil  | 2017-06-12  | 80    | 92            |
| tony  | 2017-01-04  | 29    | 94            |
| tony  | 2017-01-02  | 15    | 94            |
| tony  | 2017-01-07  | 50    | 94            |
+-------+-------------+-------+---------------+

需求2 变种2  查询购买明细和购买总额
select
name,
orderdate,
cost,
sum(cost)over( )
from business;


需求2 变种3  查询购买明细和累加总额
select
name,
orderdate,
cost,
sum(cost)over(rows between UNBOUNDED PRECEDING and current row )
from business;
```

8.5.3上述的场景, 将每个顾客的cost按照日期进行累加

```sql
select
 name,
 orderdate,
 cost,
 sum(cost)over(partition by name order by orderdate rows between  UNBOUNDED PRECEDING  and current row)
from business

| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 10            |
| jack  | 2017-01-05  | 46    | 56            |
| jack  | 2017-01-08  | 55    | 111           |
| jack  | 2017-02-03  | 23    | 134           |
| jack  | 2017-04-06  | 42    | 176           |
| mart  | 2017-04-08  | 62    | 62            |
| mart  | 2017-04-09  | 68    | 130           |
| mart  | 2017-04-11  | 75    | 205           |
| mart  | 2017-04-13  | 94    | 299           |
| neil  | 2017-05-10  | 12    | 12            |
| neil  | 2017-06-12  | 80    | 92            |
| tony  | 2017-01-02  | 15    | 15            |
| tony  | 2017-01-04  | 29    | 44            |
| tony  | 2017-01-07  | 50    | 94            |

--需求3的变种1 直接按照日期将花费进行累加
select
name,
orderdate,
cost,
sum(cost)over( order by orderdate )
from business
--需求3的变种2 将每个顾客的cost按照日期统计总花费
select
name,
orderdate,
cost,
sum(cost)over(partition by name order by orderdate rows between UNBOUNDED PRECEDING and UNBOUNDED following)
from business
+-------+-------------+-------+---------------+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 176           |
| jack  | 2017-01-05  | 46    | 176           |
| jack  | 2017-01-08  | 55    | 176           |
| jack  | 2017-02-03  | 23    | 176           |
| jack  | 2017-04-06  | 42    | 176           |
| mart  | 2017-04-08  | 62    | 299           |
| mart  | 2017-04-09  | 68    | 299           |
| mart  | 2017-04-11  | 75    | 299           |
| mart  | 2017-04-13  | 94    | 299           |
| neil  | 2017-05-10  | 12    | 92            |
| neil  | 2017-06-12  | 80    | 92            |
| tony  | 2017-01-02  | 15    | 94            |
| tony  | 2017-01-04  | 29    | 94            |
| tony  | 2017-01-07  | 50    | 94            |
+-------+-------------+-------+---------------+

--需求3的变种3 每个顾客的cost按照日期求上一次和当前一次消费的和
select
 name,
 orderdate,
 cost,
 sum(cost)over(partition by name order by orderdate rows between 1 PRECEDING and current row)
from business
+-------+-------------+-------+---------------+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 10            |
| jack  | 2017-01-05  | 46    | 56            |
| jack  | 2017-01-08  | 55    | 101           |
| jack  | 2017-02-03  | 23    | 78            |
| jack  | 2017-04-06  | 42    | 65            |
| mart  | 2017-04-08  | 62    | 62            |
| mart  | 2017-04-09  | 68    | 130           |
| mart  | 2017-04-11  | 75    | 143           |
| mart  | 2017-04-13  | 94    | 169           |
| neil  | 2017-05-10  | 12    | 12            |
| neil  | 2017-06-12  | 80    | 92            |
| tony  | 2017-01-02  | 15    | 15            |
| tony  | 2017-01-04  | 29    | 44            |
| tony  | 2017-01-07  | 50    | 79            |
+-------+-------------+-------+---------------+
--需求3的变种4 每个顾客的cost按照日期求当前和下一次消费的和
select
name,
orderdate,
cost,
sum(cost)over(partition by name order by orderdate rows between current row and 1 following)
from business

| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 56            |
| jack  | 2017-01-05  | 46    | 101           |
| jack  | 2017-01-08  | 55    | 78            |
| jack  | 2017-02-03  | 23    | 65            |
| jack  | 2017-04-06  | 42    | 42            |
| mart  | 2017-04-08  | 62    | 130           |
| mart  | 2017-04-09  | 68    | 143           |
| mart  | 2017-04-11  | 75    | 169           |
| mart  | 2017-04-13  | 94    | 94            |
| neil  | 2017-05-10  | 12    | 92            |
| neil  | 2017-06-12  | 80    | 80            |
| tony  | 2017-01-02  | 15    | 44            |
| tony  | 2017-01-04  | 29    | 79            |
| tony  | 2017-01-07  | 50    | 50            |
+-------+-------------+-------+---------------+
--需求3的变种5 每个顾客的cost按照日期求上一次到下一次消费的和
select
name,
orderdate,
cost,
sum(cost)over(partition by name order by orderdate rows between  1 PRECEDING and 1 following)
from business
+-------+-------------+-------+---------------+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 56            |
| jack  | 2017-01-05  | 46    | 111           |
| jack  | 2017-01-08  | 55    | 124           |
| jack  | 2017-02-03  | 23    | 120           |
| jack  | 2017-04-06  | 42    | 65            |
| mart  | 2017-04-08  | 62    | 130           |
| mart  | 2017-04-09  | 68    | 205           |
| mart  | 2017-04-11  | 75    | 237           |
| mart  | 2017-04-13  | 94    | 169           |
| neil  | 2017-05-10  | 12    | 92            |
| neil  | 2017-06-12  | 80    | 92            |
| tony  | 2017-01-02  | 15    | 44            |
| tony  | 2017-01-04  | 29    | 94            |
| tony  | 2017-01-07  | 50    | 79            |
+-------+-------------+-------+---------------+
--需求3的变种5 每个顾客的cost按照日期求上一次和下一次消费的和
select
name,
orderdate,
cost,
sum(cost)over(partition by name order by orderdate rows between  1 PRECEDING and 1 following)-cost
from business
```

8.5.4查询顾客购买明细以及上次的购买时间和下次购买时间

```sql
select
 name,
 orderdate,
 cost,
 lag(orderdate,1,'0000-00-00')over(partition by name order by orderdate) prve_time,
 lead(orderdate,1,'9999-99-99')over(partition by name order by orderdate) next_time
from business
| name  |  orderdate  | cost  |  prev_time  |  next_time  |
+-------+-------------+-------+-------------+-------------+
| jack  | 2017-01-01  | 10    | NULL        | 2017-01-05  |
| jack  | 2017-01-05  | 46    | 2017-01-01  | 2017-01-08  |
| jack  | 2017-01-08  | 55    | 2017-01-05  | 2017-02-03  |
| jack  | 2017-02-03  | 23    | 2017-01-08  | 2017-04-06  |
| jack  | 2017-04-06  | 42    | 2017-02-03  | NULL        |
| mart  | 2017-04-08  | 62    | NULL        | 2017-04-09  |
| mart  | 2017-04-09  | 68    | 2017-04-08  | 2017-04-11  |
| mart  | 2017-04-11  | 75    | 2017-04-09  | 2017-04-13  |
| mart  | 2017-04-13  | 94    | 2017-04-11  | NULL        |
| neil  | 2017-05-10  | 12    | NULL        | 2017-06-12  |
| neil  | 2017-06-12  | 80    | 2017-05-10  | NULL        |
| tony  | 2017-01-02  | 15    | NULL        | 2017-01-04  |
| tony  | 2017-01-04  | 29    | 2017-01-02  | 2017-01-07  |
| tony  | 2017-01-07  | 50    | 2017-01-04  | NULL        |
+-------+-------------+-------+-------------+-------------+
select
name,
orderdate,
cost,
lag(orderdate,1,'0000-00-00')over(partition by name order by orderdate) prve_time,
lead(orderdate,1,'9999-99-99')over(partition by name order by orderdate) next_time
from business

| name  |  orderdate  | cost  |  prev_time  |  next_time  |
+-------+-------------+-------+-------------+-------------+
| jack  | 2017-01-01  | 10    | 0000-00-00  | 2017-01-05  |
| jack  | 2017-01-05  | 46    | 2017-01-01  | 2017-01-08  |
| jack  | 2017-01-08  | 55    | 2017-01-05  | 2017-02-03  |
| jack  | 2017-02-03  | 23    | 2017-01-08  | 2017-04-06  |
| jack  | 2017-04-06  | 42    | 2017-02-03  | 9999-99-99  |
| mart  | 2017-04-08  | 62    | 0000-00-00  | 2017-04-09  |
| mart  | 2017-04-09  | 68    | 2017-04-08  | 2017-04-11  |
| mart  | 2017-04-11  | 75    | 2017-04-09  | 2017-04-13  |
| mart  | 2017-04-13  | 94    | 2017-04-11  | 9999-99-99  |
| neil  | 2017-05-10  | 12    | 0000-00-00  | 2017-06-12  |
| neil  | 2017-06-12  | 80    | 2017-05-10  | 9999-99-99  |
| tony  | 2017-01-02  | 15    | 0000-00-00  | 2017-01-04  |
| tony  | 2017-01-04  | 29    | 2017-01-02  | 2017-01-07  |
| tony  | 2017-01-07  | 50    | 2017-01-04  | 9999-99-99  |
```

课后练习

```sql
求每个顾客的购买明细以及上一次购买和下一次购买花费的和
select
  t1.name,
  t1.orderdate,
  t1.cost,
  t1.prev_cost,
  t1.next_cost,
  t1.prev_cost + t1.next_cost sum_cost
from (
  select 
    name,
    orderdate,
    cost,
    lag(cost,1,0) 
      over(partition by name order by orderdate) prev_cost,
    lead(cost,1,0) 
      over(partition by name order by orderdate) next_cost
 from business
) t1;

select
name,
orderdate,
cost,
sum(cost)over(partition by name order by orderdate rows between 1 PRECEDING and 1 following)-cost
from business;
```

8.5.6 查询顾客每个月第一次的购买时间 和 每个月的最后一次购买时间

```sql
select
 name,
 orderdate,
 cost,
 first_value(orderdate)over(partition by name,month(orderdate) order by orderdate  rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_time,
 last_value(orderdate)over(partition by name,month(orderdate) order by orderdate  rows between UNBOUNDED PRECEDING and UNBOUNDED following) last_time
from business

| name  |  orderdate  | cost  | first_order  | last_order  |
+-------+-------------+-------+--------------+-------------+
| jack  | 2017-01-01  | 10    | 2017-01-01   | 2017-01-08  |
| jack  | 2017-01-05  | 46    | 2017-01-01   | 2017-01-08  |
| jack  | 2017-01-08  | 55    | 2017-01-01   | 2017-01-08  |
| jack  | 2017-02-03  | 23    | 2017-02-03   | 2017-02-03  |
| jack  | 2017-04-06  | 42    | 2017-04-06   | 2017-04-06  |
| mart  | 2017-04-08  | 62    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-09  | 68    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-11  | 75    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-13  | 94    | 2017-04-08   | 2017-04-13  |
| neil  | 2017-05-10  | 12    | 2017-05-10   | 2017-05-10  |
| neil  | 2017-06-12  | 80    | 2017-06-12   | 2017-06-12  |
| tony  | 2017-01-02  | 15    | 2017-01-02   | 2017-01-07  |
| tony  | 2017-01-04  | 29    | 2017-01-02   | 2017-01-07  |
| tony  | 2017-01-07  | 50    | 2017-01-02   | 2017-01-07  |

--需求 求每个顾客的第一次购买时间和最后一次购买时间
select
name,
orderdate,
cost,
first_value(orderdate)over(partition by name order by orderdate
rows between UNBOUNDED PRECEDING AND UNBOUNDED following ) first_order,
last_value(orderdate)over(partition by name order by orderdate
rows between UNBOUNDED PRECEDING AND UNBOUNDED following ) last_order
from business

+-------+-------------+-------+--------------+-------------+
| name  |  orderdate  | cost  | first_order  | last_order  |
+-------+-------------+-------+--------------+-------------+
| jack  | 2017-01-01  | 10    | 2017-01-01   | 2017-04-06  |
| jack  | 2017-01-05  | 46    | 2017-01-01   | 2017-04-06  |
| jack  | 2017-01-08  | 55    | 2017-01-01   | 2017-04-06  |
| jack  | 2017-02-03  | 23    | 2017-01-01   | 2017-04-06  |
| jack  | 2017-04-06  | 42    | 2017-01-01   | 2017-04-06  |
| mart  | 2017-04-08  | 62    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-09  | 68    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-11  | 75    | 2017-04-08   | 2017-04-13  |
| mart  | 2017-04-13  | 94    | 2017-04-08   | 2017-04-13  |
| neil  | 2017-05-10  | 12    | 2017-05-10   | 2017-06-12  |
| neil  | 2017-06-12  | 80    | 2017-05-10   | 2017-06-12  |
| tony  | 2017-01-02  | 15    | 2017-01-02   | 2017-01-07  |
| tony  | 2017-01-04  | 29    | 2017-01-02   | 2017-01-07  |
| tony  | 2017-01-07  | 50    | 2017-01-02   | 2017-01-07  |

```

8.5.7 查询前20%时间的订单信息

```sql
select
 name,
 orderdate,
 cost,
 ntile(5)over(order by orderdate) n_c
from business  t1

select
 t1.name,
 t1.orderdate,
 t1.cost,
 t1.n_c
from(
    select
     name,
     orderdate,
     cost,
     ntile(5)over(order by orderdate) n_c
    from business
) t1
where t1.n_c=1
| t1.name  | t1.orderdate  | t1.cost  | t1.group_id  |
+----------+---------------+----------+--------------+
| jack     | 2017-01-01    | 10       | 1            |
| tony     | 2017-01-02    | 15       | 1            |
| tony     | 2017-01-04    | 29       | 1            |
```

8.5.8计算每门学科成绩排名。

```sql
select
 name,
 subject,
 score,
 rank()over(partition by subject order by score desc) rk,
 dense_rank()over(partition by subject order by score desc) drk,
 row_number()over(partition by subject order by score desc)rn
from score

| name  | subject  | score  | rk  | drk  | rn  |
+-------+----------+--------+-----+------+------+
| 孙悟空 | 数学      | 95     | 1   | 1    | 1    |
| 宋宋   | 数学       | 86     | 2   | 2    | 2   |
| 婷婷   | 数学       | 85     | 3   | 3    | 3   |
| 大海   | 数学       | 56     | 4   | 4    | 4   |
| 宋宋   | 英语       | 84     | 1   | 1    | 1   |
| 大海   | 英语       | 84     | 1   | 1    | 2   |
| 婷婷   | 英语       | 78     | 3   | 2    | 3   |
| 孙悟空  | 英语       | 68     | 4   | 3    | 4  |
| 大海   | 语文       | 94     | 1   | 1    | 1   |
| 孙悟空 | 语文       | 87     | 2   | 2    | 2   |
| 婷婷   | 语文       | 65     | 3   | 3    | 3   |
| 宋宋   | 语文       | 64     | 4   | 4    | 4   |
```

```sql
select
name,
orderdate,
cost,
sum(cost)over( order by month(orderdate)) 
from business;
+-------+-------------+-------+---------------+
| name  |  orderdate  | cost  | sum_window_0  |
+-------+-------------+-------+---------------+
| jack  | 2017-01-01  | 10    | 205           |
| jack  | 2017-01-08  | 55    | 205           |
| tony  | 2017-01-07  | 50    | 205           |
| jack  | 2017-01-05  | 46    | 205           |
| tony  | 2017-01-04  | 29    | 205           |
| tony  | 2017-01-02  | 15    | 205           |
| jack  | 2017-02-03  | 23    | 228           |
| mart  | 2017-04-13  | 94    | 569           |
| jack  | 2017-04-06  | 42    | 569           |
| mart  | 2017-04-11  | 75    | 569           |
| mart  | 2017-04-09  | 68    | 569           |
| mart  | 2017-04-08  | 62    | 569           |
| neil  | 2017-05-10  | 12    | 581           |
| neil  | 2017-06-12  | 80    | 661           |
+-------+-------------+-------+---------------+
```

关于 建表语句和排序和窗口函数的容易混淆的语法

```sql
一. 建表的时候    
1.partitioned by 表示你创建的表为分区表
2.clustered by 表示你创建表为分桶表
二. 查询语句里排序的四个by
order by 表示全局排序
distribute by 以什么分区 sort by 区内排序字段
cluster by 表示分区排序
三.窗口函数的partition
partition by 表示 更细窗口划分
order by 窗口以什么排序

distribute by sort by  相当于 partition by  order by 

select
 name,
 orderdate,
 cost,
 first_value(orderdate)over(distribute by name,month(orderdate) sort by orderdate  rows between UNBOUNDED PRECEDING and UNBOUNDED following) first_time,
 last_value(orderdate)over(partition by name,month(orderdate) order by orderdate  rows between UNBOUNDED PRECEDING and UNBOUNDED following) last_time
from business
```

###### 8.5关于创建函数

8.5.1创建临时函数

```sql
1.add jar /opt/module/hive/datas/myudf.jar;
2.创建函数
create temporary function my_len as "com.atguigu.udf.MyUDF";
临时函数只对你当前的单次会话生效,并且可以跨库使用
3.删除临时函数
drop  temporary function my_len;
```

8.5.2创建永久函数

```sql
1.add jar /opt/module/hive/datas/myudf.jar;
2.创建函数
create function my_len2 as "com.atguigu.udf.MyUDF";
3.创建真正的永久函数
 create function my_len3 as "com.atguigu.udf.MyUDF" using jar "hdfs://hadoop102:8020/udf/myudf.jar";
4.删除永久函数
drop function my_len2;
```

#### 第九章压缩存储

```sql
当你在公司里 使用 mr做引擎玩数仓的时候  你文件存储格式 可以为 orc+lzo
当你在公司里 使用 spark做引擎玩数仓的时候  你文件存储格式 可以为 parquet+snappy
```

