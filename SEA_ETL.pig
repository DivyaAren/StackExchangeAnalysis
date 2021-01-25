-- Placing data files on HDFS
hdfs dfs -put 1-QueryResults.csv /pfile1
hdfs dfs -put 2-QueryResults.csv /pfile2
hdfs dfs -put 3-QueryResults.csv /pfile3
hdfs dfs -put 4-QueryResults.csv /pfile4

-- Initiating grunt shell
Pig;

-- Register Piggybank 
REGISTER /usr/lib/pig/piggybank.jar
REGISTER 'hive-hcatalog-core-3.1.1.jar'

-- Define CSVExcelStorage() UDF
define CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Loading 4 CSV files from HDFS directory to Pig
file1 = LOAD '/pfile1' USING CSVExcelStorage(',', 'YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

file2 = LOAD '/pfile2' USING CSVExcelStorage(',', 'YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

file3 = LOAD '/pfile3' USING CSVExcelStorage(',', 'YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

file4 = LOAD '/pfile4' USING CSVExcelStorage(',', 'YES_MULTILINE','UNIX','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

-- Merge the four CSV files into a single file
Merge = UNION file1, file2, file3, file4;

-- Selecting required columns and cleaning the data of spaces, return carriages, tabs and HTML Tags
Clean = FOREACH Merge GENERATE Id AS Id, PostTypeId as PostTypeId, Score AS Score, REPLACE(REPLACE(REPLACE(Body, '\n*', ''),',*',''),'\\<.*?\\>','') AS Body, OwnerUserId AS OwnerUserId, REPLACE(Title,',*','') AS Title, REPLACE(Tags,',*','') AS Tags;

--Filter NULL records
Fil = FILTER Clean BY (OwnerUserId IS NOT NULL) AND (Score IS NOT NULL);

--Filter headers from file, if any
Nohead = FILTER Fil BY (PostTypeId == 1);

--Dump select records to check data is transformed as per expectation
limit_data = LIMIT Nohead 5;
DUMP limit_data;

--Store Data into HDFS directory
STORE Nohead INTO '/pigfile' USING PigStorage(',');

--Exiting grunt shell
quit;

--Delete SUCCESS logs
hdfs dfs -rm -r /pigfile/_SUCCESS 