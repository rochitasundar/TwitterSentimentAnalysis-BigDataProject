/*
   1. Create database 'twitter_netflix_project' in AWS Athena
   2. Create table 'final_twitter_2_predictions_subset' in the database by reading data from S3 buckets
 */

CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_netflix_project`.`final_twitter_2_predictions_subset` (
  `tweet_id` bigint,
  `user_name` string,
  `user_screen_name` string,
  `user_followers_count` int,
  `user_statuses_count` int,
  `user_location` string,
  `tweet_text` string,
  `tweet_hashtags` string,
  `tweet_created_at` string,
  `tweet_sentiment_label` string,
  `tweet_text_clean` string,
  `tokens` string,
  `filtered` string,
  `label` double,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://xxxxxxxxxxxxxxx/twitter_project_netflix/final_twitter_2_predictions_subset/'
TBLPROPERTIES ('has_encrypted_data'='false');
-- -------------------------------------------------------------------------------------------------------------

/*
   1. Feature engineer new columns
        - mapping prediction labels 0,1,2 to positive, neutral, negative
        - creating a new column isRightPrediction by comparing label & prediction
   2. Clean columns
        - extract day & hour from tweet_created_at as tweet_day_hour
        - extract the first hashtag in each tweet_text as firstHashtag
   3. Save the results by creating a new table called twitter_netflix_final
 */

create table twitter_netflix_project.twitter_netflix_final as
    select *,
           case when prediction = 0 then 'positive'
                when prediction = 1 then 'neutral'
                when prediction = 2 then 'negative'
           end as prediction_cat,
           case when prediction = label then 'Right prediction'
                when prediction <> label then 'Wrong prediction'
           end as isRightPrediction,
           substr(tweet_created_at,1,14) as tweet_day_hour,
           substr(tweet_hashtags,12,strpos(tweet_hashtags, ',')-13) as firstHashtag
    from final_twitter_2_predictions_subset;

-- -------------------------------------------------------------------------------------------------------------

/*
   Count of tweet sentiments predicted by ML model
 */

select prediction_cat, isRightPrediction, count(*)
from twitter_netflix_final
group by prediction_cat, isRightPrediction
order by prediction_cat, isRightPrediction;

/* Output:
 #	prediction_cat	isRightPrediction	_col2
------------------------------------------------
 1	negative	    Right prediction	13706
 2	negative	    Wrong prediction	90
 3	neutral	        Right prediction	44560
 4	neutral	        Wrong prediction	274
 5	positive	    Right prediction	44520
 6	positive	    Wrong prediction	155
 */
-- -------------------------------------------------------------------------------------------------------------

/*
   Tweet count by day & hour with further segregation by predicted sentiments
   as a pivot table
 */

select tweet_day_hour,
       sum(case when prediction_cat = 'positive' then 1 end) as positive_count,
       sum(case when prediction_cat = 'neutral' then 1 end) as neutral_count,
       sum(case when prediction_cat = 'negative' then 1 end) as negative_count
from twitter_netflix_final
group by tweet_day_hour
order by tweet_day_hour;

/*
#	tweet_day_hour	positive_count	neutral_count	negative_count
--------------------------------------------------------------------
	Thu Apr 28 00:	3902	        3601	        1595
	Thu Apr 28 01:	3785	        3759	        1300
	Thu Apr 28 02:	3613	        3721	        1257
	Thu Apr 28 03:	3504	        3557	        1060
	Thu Apr 28 04:	3065	        3600	        884
	Thu Apr 28 05:	2745	        3240	        815
	Wed Apr 27 19:	4081	        4146	        782
	Wed Apr 27 20:	4069	        3781	        832
	Wed Apr 27 21:	4531	        4286	        880
	Wed Apr 27 22:	4589	        3802	        1254
	Wed Apr 27 23:	4100	        3712	        2474
 */
-- -------------------------------------------------------------------------------------------------------------

/*
    Top 5 firstHashtags by count for each sentiment
 */

select prediction_cat, firsthashtag, count(firsthashtag)
from twitter_netflix_final
where prediction_cat = 'positive' and firsthashtag is not null  -- replace 'positive' with 'negative' or 'neutral'
group by prediction_cat, firsthashtag order by 3 desc
limit 5; -- replace to get top 'x'

/*
 positive sentiments -
 #	prediction_cat	firsthashtag	_col2
------------------------------------------
1	positive    	Netflix	        394
2	positive	    TheGrayMan	    278
3	positive    	Heartstopper	235
4	positive	    XiaoZhan	    233
5	positive	    365daysThisDay	144

 neutral sentiments -
 #	prediction_cat	firsthashtag	_col2
------------------------------------------
1	neutral	        SuperNature	    594
2	neutral	        jualnetflix	    508
3	neutral	        TheGrayMan	    248
4	neutral	        Netflix	        207
5	neutral	        SilvertonSiege	143

 negative sentiments -
 #	prediction_cat	firsthashtag	_col2
------------------------------------------
1	negative	    365daysThisDay	120
2	negative	    Netflix     	88
3	negative	    셔누	            78
4	negative	    김세정	        60
5	negative	    maker_proofs	15
 */

-- -------------------------------------------------------------------------------------------------------------

/*
 Top 5 twitter user names by follower count & predicted segments
 */

select prediction_cat, user_name, avg(user_followers_count)  -- since number of followers may change with time
from twitter_netflix_final
where prediction_cat = 'positive' -- replace 'positive' with 'negative' or 'neutral'
group by prediction_cat, user_name
order by 3 desc
limit 5; -- replace to get top 'x'

/*
  positive sentiments -
  #	prediction_cat	user_name	         user_followers_count
----------------------------------------------------------------
1	positive	    Netflix	             1.7686383E7
2	positive	    The Associated Press 1.5781671E7
3	positive	    The New Yorker	     9049172.0
4	positive	    Financial Times	     7234724.0
5	positive	    USA TODAY	         4719884.0

 neutral sentiments -
 #	prediction_cat	user_name	        user_followers_count
---------------------------------------------------------------
1	neutral	        Kevin Hart	        3.7194246E7
2	neutral	        Ricky Gervais	    1.4781032E7
3	neutral	        Mashable	        9429552.0
4	neutral	        Netflix	            8842867.0
5	neutral	        IGN	                8807969.0

 negative sentiments -
 #	prediction_cat	user_name	        user_followers_count
---------------------------------------------------------------
1	negative	    Forbes	            1.78025305E7
2	negative	    Netflix	            1.7689998666666668E7
3	negative	    Los Angeles Times	3921802.0
4	negative	    Business Standard	2193595.5
5	negative	    GMA Network	        1646366.0
 */

-- -------------------------------------------------------------------------------------------------------------

/*
 Similar analysis can be performed for Top 5 twitter user names by status count & predicted segments
 */

select prediction_cat, user_name, avg(user_statuses_count) -- since number of followers may change with time
from twitter_netflix_final
where prediction_cat = 'positive' -- replace 'positive' with 'negative' or 'neutral'
group by prediction_cat, user_name
order by 3 desc
limit 5;

/*
 positive sentiments -
 #	prediction_cat	    user_name	                            _col2
------------------------------------------------------------------------
1	positive	        Trade Alerts, Trade Ideas and Crypto 🇺🇸	2560683.0
2	positive	        Berkley Bear	                        2014066.0
3	positive	        LlaneroABManchegueroCR6969X	            1939035.5
4	positive	        sarah	                                1883589.8333333333
5	positive	        Jeff Strong	                            1881787.0

 neutral sentiments -
 #	prediction_cat	    user_name	                            _col2
------------------------------------------------------------------------
1	neutral	            Telkomsel	                            6096500.0
2	neutral	            Woody	                                2654649.0
3	neutral	            FilaFresh	                            2544430.0
4	neutral	            gocoo（悟空）	                        2363481.5
5	neutral         	gocoo	                                2335920.5

  negative sentiments -
 #	prediction_cat	    user_name	                            _col2
------------------------------------------------------------------------
1	negative	        Woody	                                2654630.0
2	negative	        anodyne	                                2173995.5
3	negative	        Jeff Strong	                            1881804.0
4	negative	        ronnie cassol	                        1806836.0
5	negative	        Charles Myrick -CEO	                    1805607.0

 */

-- -------------------------------------------------------------------------------------------------------------

-- & other visualizations possible