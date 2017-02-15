ADD JAR /home/training/exercises/projects/twitter-project/lib/hive-serdes-1.0-SNAPSHOT.jar;
CREATE EXTERNAL TABLE tweets1 (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweet_count INT,
   retweeted_status STRUCT<
      text:STRING,
      user:STRUCT<screen_name:STRING,name:STRING>>,
   entities STRUCT<
      urls:ARRAY<STRUCT<expanded_url:STRING>>,
      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
      hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   user STRUCT<
      screen_name:STRING,
      name:STRING,
      friends_count:INT,
      followers_count:INT,
      statuses_count:INT,
      verified:BOOLEAN,
      utc_offset:INT,
      time_zone:STRING>,
   in_reply_to_screen_name STRING
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/hive/warehouse/tweets';

SELECT t.retweeted_screen_name, sum(retweets) AS total_retweets, count(*) AS tweet_count FROM (SELECT retweeted_status.user.screen_name as retweeted_screen_name, retweeted_status.text, max(retweet_count) as retweets FROM tweets GROUP BY retweeted_status.user.screen_name, retweeted_status.text) t GROUP BY t.retweeted_screen_name ORDER BY total_retweets DESC LIMIT 10;

select user.screen_name, user.followers_count c from tweets order by c;

SELECT t.retweeted_screen_name, sum(retweets) AS total_retweets, count(*) AS tweet_count 
FROM 
(SELECT retweeted_status.user.screen_name as retweeted_screen_name, retweeted_status.text, max(retweet_count) as retweets 
FROM tweets1 GROUP BY retweeted_status.user.screen_name, retweeted_status.text) t 
GROUP BY t.retweeted_screen_name ORDER BY total_retweets DESC LIMIT 10;

CREATE EXTERNAL TABLE tweets2 (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    user:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  user STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/hive/warehouse/tweets';


CREATE EXTERNAL TABLE tweets (
entities STRUCT<
urls:ARRAY,
user_mentions:ARRAY,
hashtags:ARRAY<STRUCT>>
)
ROW FORMAT SERDE ‘com.cloudera.hive.serde.JSONSerDe’
LOCATION '/user/hive/tweets';


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar
create external table tweets(id BigInt, created_at String, scource String, favorited Boolean, retweet_count int, 
retweeted_status Struct < 
    text:String,user:Struct< 
        screen_name:String, name:String>>,
    entities Struct<
        urls:Array<Struct<          
             expanded_url:String>>,
        user_mentions:Array<Struct<
            screen_name:String,
            name:String>>,
        hashtags:Array<Struct<text:String>>>,

text String,
user Struct<
    screen_name:String,
    name:String,
    friends_count:int,
    followers_count:int,
     statuses_count:int,
    verified:boolean, 
    utc_offset:int,
    time_zone:String> , 
in_reply_to_screen_name String)
partitioned by (datehour int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hive/tweets';


CREATE EXTERNAL TABLE twitter_publicstream (
  coordinates String,
  retweeted BOOLEAN,
  source String,
  entities STRUCT<
    trends: string,
    symbols: string,
    urls: ARRAY<
      STRUCT<
        expanded_url: string,
        indices: ARRAY< INT >,
        display_url: string,
        url: string
      >
    >,
    hashtags: ARRAY<
      STRUCT<
        text: string,
        indices: ARRAY< INT >
      >
    >,
    user_mentions: ARRAY<
      STRUCT<
        id: BIGINT,
        name: string,
        indices: ARRAY< INT >,
        screen_name: string,
        id_str: string
      >
    >
  >,
  favorite_count INT,
  in_reply_to_status_id_str string,
  geo STRUCT<type:string,coordinates:string>,
  id_str string,
  in_reply_to_user_id String,
  timestamp_ms string,
  truncated BOOLEAN,
  text string,
  retweet_count INT,
  id String,
  in_reply_to_status_id String,
  possibly_sensitive BOOLEAN,
  filter_level string,
  created_at string,
  place STRUCT<
    id: string,
    place_type: string,
    bounding_box: STRUCT<
      type: string,
      coordinates: ARRAY<
        ARRAY<
          ARRAY< DOUBLE >
        >
      >
    >,
    name: string,
    attributes: string,
    country_code: string,
    url: string,
    full_name: string,
    country: string
  >,
  favorited BOOLEAN,
  lang string,
  contributors string,
  in_reply_to_screen_name string,
  in_reply_to_user_id_str string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hive/tweets';
STORED AS TEXTFILE;


add jar /home/training/exercises/projects/twitter-project/lib/json-serde-1.1.4-jar-with-dependencies.jar;
create table tweets (
   created_at string,
   entities struct <
      hashtags: array ,
            text: string>>,
      media: array ,
            media_url: string,
            media_url_https: string,
            sizes: array >,
            url: string>>,
      urls: array ,
            url: string>>,
      user_mentions: array ,
            name: string,
            screen_name: string>>>,
   geo struct <
      coordinates: array ,
      type: string>,
   id bigint,
   id_str string,
   in_reply_to_screen_name string,
   in_reply_to_status_id bigint,
   in_reply_to_status_id_str string,
   in_reply_to_user_id int,
   in_reply_to_user_id_str string,
   retweeted_status struct <
      created_at: string,
      entities: struct <
         hashtags: array ,
               text: string>>,
         media: array ,
               media_url: string,
               media_url_https: string,
               sizes: array >,
               url: string>>,
         urls: array ,
               url: string>>,
         user_mentions: array ,
               name: string,
               screen_name: string>>>,
      geo: struct <
         coordinates: array ,
         type: string>,
      id: bigint,
      id_str: string,
      in_reply_to_screen_name: string,
      in_reply_to_status_id: bigint,
      in_reply_to_status_id_str: string,
      in_reply_to_user_id: int,
      in_reply_to_user_id_str: string,
      source: string,
      text: string,
      user: struct <
         id: int,
         id_str: string,
         name: string,
         profile_image_url_https: string,
         protected: boolean,
         screen_name: string,
         verified: boolean>>,
   source string,
   text string,
   user struct <
      id: int,
      id_str: binary,
      name: string,
      profile_image_url_https: string,
      protected: boolean,
      screen_name: string,
      verified: boolean>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

ADD JAR /home/training/exercises/projects/twitter-project/lib/hive-serde-1.0.jar;
CREATE EXTERNAL TABLE tweets (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    user:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  user STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/hive/tweets';

CREATE TABLE IF NOT EXISTS employee ( eid int, name String,
salary String, destination String)
COMMENT ‘Employee details’
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ‘\t’
LINES TERMINATED BY ‘\n’
STORED AS TEXTFILE;
