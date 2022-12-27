create or replace table clickstream (post_visid_high number, post_visid_low number, date_time timestamp);
insert into clickstream values (1,0, '2013-09-04T15:49:49');
insert into clickstream values (1,0, '2013-09-04T15:49:58');
insert into clickstream values (1,0, '2013-09-04T15:49:49');
insert into clickstream values (1,0, '2013-09-04T15:49:58');
insert into clickstream values (1,0, '2013-09-04T16:37:11');
insert into clickstream values (1,0, '2013-09-04T16:37:18');
insert into clickstream values (1,0, '2013-09-04T16:39:27');
insert into clickstream values (1,0, '2013-09-04T16:43:57');
insert into clickstream values (1,0, '2013-09-04T20:12:03');
insert into clickstream values (1,0, '2013-09-05T00:00:17');
insert into clickstream values (1,0, '2013-09-05T00:20:35');
insert into clickstream values (2,0, '2013-09-05T00:22:37');
insert into clickstream values (2,0, '2013-09-05T01:19:29');
insert into clickstream values (1,0, '2013-09-05T01:19:39');
insert into clickstream values (1,0, '2013-09-05T01:20:03');
insert into clickstream values (1,0, '2013-09-05T01:20:17');
insert into clickstream values (1,0, '2013-09-05T02:33:42');

select * from clickstream;



select 
  post_visid_high || ':' || post_visid_low as visitor_id
  , date_time
  , datediff(
      minute
      , lag(date_time) over (partition by visitor_id order by date_time asc)
      , date_time
    ) as minutes_since_last_action
  , conditional_true_event(minutes_since_last_action > 50) 
      over (partition by visitor_id order by date_time asc)
    as session_count
from clickstream;
