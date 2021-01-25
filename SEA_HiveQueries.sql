--initiating hive
hive;

--create table posts
create external table if not exists posts (Id int, PostTypeId int, Score int, Body string, OwnerUserId int, OwnerDisplayName string, Title string, Tags string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

--loading data from HDFS into Hive
Load data inpath '/pigfile' into table posts;

--Include column headers
SET hive.cli.print.header=true;

--Verification of data loaded in Hive from HDFS
SELECT * FROM posts limit 5;

--First query - 1. The top 10 posts by score
SELECT id, title, score 
FROM posts
ORDER BY score DESC LIMIT 10;

--Second query - 2. The top 10 users by post score
SELECT OwnerUserId, SUM(Score) AS TotalScore
FROM posts
GROUP BY OwnerUserId
ORDER BY TotalScore DESC LIMIT 10;

--Third query - 3. The number of distinct users, who used the word 'hadoop' in one of their posts
SELECT COUNT(DISTINCT OwnerUserId)
FROM posts
WHERE (body LIKE '%hadoop%' OR title LIKE '%hadoop%' OR tags LIKE '%hadoop%');