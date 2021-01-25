--For checking the range of viewcounts
SELECT count(*) FROM Posts WHERE ViewCount>110000 
SELECT count(*) FROM Posts WHERE ViewCount>65000 and ViewCount<111930
SELECT count(*) FROM Posts WHERE ViewCount>40000 and ViewCount<65000
SELECT count(*) FROM Posts WHERE ViewCount>36500 and ViewCount<47000

--Checking the no. of posts with the same viewcount
SELECT count(*) FROM Posts WHERE ViewCount=111930
SELECT count(*) FROM Posts WHERE ViewCount=65887 
SELECT count(*) FROM Posts WHERE ViewCount=47039 
SELECT count(*) FROM Posts WHERE ViewCount=36590

--Query to download 50,000 unique records
SELECT top 50000 * FROM Posts WHERE ViewCount>110000 ORDER BY ViewCount DESC
SELECT * FROM Posts WHERE ViewCount>=65887 and ViewCount<111930 ORDER BY ViewCount DESC
SELECT * FROM Posts WHERE ViewCount>=47039 and ViewCount<65887 ORDER BY ViewCount DESC
SELECT * FROM Posts WHERE ViewCount>=36590 and ViewCount<47039 ORDER BY ViewCount DESC