select
  '' 
  , symbol
  , date
  , close
  , row_number() over (partition by symbol order by date) as day
  , conditional_true_event (close > lag(close) over (partition by symbol order by date)) 
      over (partition by symbol order by date) as days_of_increase
from us_stock_market_data_for_data_science.public.stock_history
where symbol='SNOW';
