#!/bin/bash
python /home/Investing/athens_stock_exchange/scripts/get_daily_historical_quotes.py
bq load --skip_leading_rows=1 athens_stock_exchange.company_Daily_Quotes gs://athens_stock_exchange/company_information/company_Daily_Quotes_new_day.csv 'collection_date':timestamp,'sticker':string,'companyName':string,'date':timestamp,'adjusted_close':float,'volume':float,'open':float,'high':float,'low':float,'close':float
printf "\nProcess has been succesfully completed!"
