### Start scylla database as docker container

```bash
./docker/start-docker.sh  
```

### Create keyspace and table

* <strong>Starting the CQL shell<strong/>
    ```bash
    docker exec -it scylla-node-1 cqlsh
    ```
* create keyspace
    ```sql
    CREATE KEYSPACE IF NOT EXISTS twitter WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
    ```
* create table and indexes
    ```sql
    
    DROP TABLE IF EXISTS twitter.summary_2021_01;
    CREATE TABLE IF NOT EXISTS twitter.summary_2021_01
    (
        sentiment varchar,
        created   date,
        country   varchar,
        tweet_id  bigint,
        ner_text  frozen<list<varchar>>,
        primary key ((sentiment, created), country, tweet_id, ner_text)
    );
    CREATE INDEX summary_2021_01_by_sentiment ON twitter.summary_2021_01 (sentiment);
    CREATE INDEX summary_2021_01_by_created ON twitter.summary_2021_01 (created);
    CREATE INDEX summary_2021_01_by_country ON twitter.summary_2021_01 (country);
    ```

### Download Covid-19 tweets datasets files using subversion

* create folders
    ```bash
      mkdir -p data/lopezbec/ner data/lopezbec/details data/lopezbec/sentiment
    ```
    ```bash
      mkdir -p data/lopezbec/sentiment5
    ```
* download Summary_NER files
    ```bash
    cd data/ner
    svn export --force https://github.com/lopezbec/COVID19_Tweets_Dataset_2021/trunk/Summary_NER/2021_01
    ```
* download Summary_Details files
    ```bash
    cd data/details
    svn export --force https://github.com/lopezbec/COVID19_Tweets_Dataset_2021/trunk/Summary_Details/2021_01

* download Summary_Sentiment files
    ```bash
    cd data/sentiment
    svn export --force https://github.com/lopezbec/COVID19_Tweets_Dataset_2021/trunk/Summary_Sentiment/2021_01

### Save downloaded Covid-19 tweets datasets to database

```
python  -m src.summary.indexing
```

### Queries database table:

```bash
docker exec -it summary-node-1 cqlsh
```

```sql
use twitter;
```

* select all tweets for 2021-01-01

```sql
  SELECT sentiment, created, count(1)
  FROM summary_2021_01
  WHERE created = '2021-01-01'
  GROUP BY sentiment, created
      ALLOW FILTERING USING TIMEOUT 240s;
  ```

```bash
 sentiment | created    | count
-----------+------------+---------
  positive | 2021-01-01 |  371966
  negative | 2021-01-01 | 1179204
   neutral | 2021-01-01 | 1146081
```

* select all tweets for 2021-01-01 and country = 'US'

```sql
  SELECT sentiment, created, count(1)
  FROM summary_2021_01
  WHERE created = '2021-01-01'
    AND country = 'US'
  GROUP BY sentiment, created
      ALLOW FILTERING USING TIMEOUT 240s;
```

```bash
 sentiment | created    | count
-----------+------------+-------
  positive | 2021-01-01 |  3357
  negative | 2021-01-01 |  5636
   neutral | 2021-01-01 |  3059
```

* select all tweets for 2021-01-01 and country = 'US'

```sql
  SELECT sentiment, created, count(1)
  FROM summary_2021_01
  WHERE created = '2021-01-01'
    AND ner_text CONTAINS 'covid'
  GROUP BY sentiment, created
      ALLOW FILTERING USING TIMEOUT 240s;
```

```bash
 sentiment | created    | count
-----------+------------+-------
  positive | 2021-01-01 |  8131
  negative | 2021-01-01 | 43327
   neutral | 2021-01-01 | 41044
```

* select all tweets for 2021-01-01 and country = 'US' and ner_text CONTAINS 'covid'

```sql
  SELECT sentiment, created, count(1)
  FROM summary_2021_01
  WHERE created = '2021-01-01'
    AND country = 'US'
    AND ner_text CONTAINS 'covid'
  GROUP BY sentiment, created
      ALLOW FILTERING USING TIMEOUT 240s;
```

```bash
 sentiment | created    | count
-----------+------------+-------
  positive | 2021-01-01 |   144
  negative | 2021-01-01 |   253
   neutral | 2021-01-01 |   123
```

* select for negative tweets and country = 'US' and ner_text CONTAINS 'covid'

```sql
 SELECT sentiment, created, count(1)
 FROM summary_2021_01
 WHERE sentiment = 'negative'
   AND country = 'US'
   AND ner_text CONTAINS 'covid'
 GROUP BY sentiment, created ALLOW FILTERING USING TIMEOUT 240s;
```

```bash
 sentiment | created    | count
-----------+------------+-------
  negative | 2021-01-13 |   262
  negative | 2021-01-28 |   180
  negative | 2021-01-02 |   248
  negative | 2021-01-25 |   190
  negative | 2021-01-30 |   167
  negative | 2021-01-14 |   192
  negative | 2021-01-12 |   279
  negative | 2021-01-01 |   253
  negative | 2021-01-26 |   211
  negative | 2021-01-03 |   313
  negative | 2021-01-23 |   258
  negative | 2021-01-27 |   212
  negative | 2021-01-31 |   179
  negative | 2021-01-08 |   230
  negative | 2021-01-06 |   330
  negative | 2021-01-29 |   233
  negative | 2021-01-22 |   248
  negative | 2021-01-24 |   181
  negative | 2021-01-04 |   216
  negative | 2021-01-18 |   203
  negative | 2021-01-09 |   196
  negative | 2021-01-19 |   227
  negative | 2021-01-17 |   205
  negative | 2021-01-21 |   251
  negative | 2021-01-20 |   246
  negative | 2021-01-11 |   181
  negative | 2021-01-15 |   215
  negative | 2021-01-05 |   257
  negative | 2021-01-16 |   217
  negative | 2021-01-10 |   211
  negative | 2021-01-07 |   261
```

