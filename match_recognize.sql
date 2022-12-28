select 
  post_visid_high || ':' || post_visid_low as visitor_id  
  , date_time, referrer, page_url
from clickstream_data 
match_recognize (
partition by post_visid_high || ':' || post_visid_low
order by date_time
all rows per match
pattern (entry onsite+ checkout_start)
define 
  entry as referrer is null or referrer not like '%buy.com%',
  onsite as page_url like '%buy.com%' 
    and page_url not like 'https://buy.com/checkout/%',
  checkout_start as page_url like 'https://buy.com/checkout/%'
);
