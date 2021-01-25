-- TF-IDF calculation

create table wikipage as (select owneruserid as docid, CONCAT_WS(' ',body, tags, title) as page from posts where owneruserid in (select OwnerUserId from topusers limit 10));

select * from wikipage limit 5;


-- add jars


add jar /tmp/hivemall-core-0.4.2-rc.2.jar;
source define-all.hive;


-- Define macros used in the TF-IDF computation


create temporary macro max2(x INT, y INT)
if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT)
tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);


-- Data preparation


create or replace view wikipage_exploded
as
select
  docid, 
  word
from
  wikipage LATERAL VIEW explode(tokenize(page,true)) t as word
where
  not is_stopword(word);



-- Define views of TF/DF

create or replace view term_frequency 
as
select
  docid, 
  word,
  freq
from (
select
  docid,
  tf(word) as word2freq
from
  wikipage_exploded
group by
  docid
) t 
LATERAL VIEW explode(word2freq) t2 as word, freq;

create or replace view document_frequency
as
select
  word, 
  count(distinct docid) docs
from
  wikipage_exploded
group by
  word;

-- TF-IDF calculation for each docid/word pair

-- set the total number of documents
select count(distinct docid) from wikipage;
set hivevar:n_docs=10;

create or replace view tfidf
as
select
  tf.docid,
  tf.word, 
  -- tf.freq * (log(10, CAST(${n_docs} as FLOAT)/max2(1,df.docs)) + 1.0) as tfidf
  tfidf(tf.freq, df.docs, ${n_docs}) as tfidf
from
  term_frequency tf 
  JOIN document_frequency df ON (tf.word = df.word)
order by 
  tfidf desc;

  select * from tfidf limit 5;

-- Query to return per user TF-IDF for top 10 words used by top 10 users by score
  SELECT * FROM ( SELECT ROW_NUMBER() OVER(PARTITION BY docid ORDER BY tfidf DESC) AS TfRank, * FROM tfidf) n WHERE TfRank IN (1,2,3,4,5,6,7,8,9,10)
