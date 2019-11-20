from google.cloud import storage, bigquery
from google.cloud.storage import Blob

from openpyxl import load_workbook

import datetime
import time

import pandas_datareader
import pandas as pd



def read_company_information_from_xlsx(fileName):
    """Reads an xlx file localy inside the VM"""

    workbook = load_workbook(fileName)
    first_sheet = workbook.get_sheet_names()[0]
    worksheet = workbook.get_sheet_by_name(first_sheet)

    list_symbol = []
    list_company_name = []

    i=0
    for row in worksheet.rows:
		if i>0:
			list_symbol.append(row[1].value)
			list_company_name.append(row[2].value)

		i+=1

    return list_symbol, list_company_name
	
	

def store_information_in_CloudStorage(companyData_df, companyInformation_bucket, companyInformation_csv_output):
	"""Stores data in Cloud Storage in csv format"""

	companyInformation_vm_filepath = "/home/Investing/athens_stock_exchange/company_information/company_Daily_Quotes.csv"

	companyData_df.to_csv(companyInformation_vm_filepath, index=False)

	"""Uploads a blob to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(companyInformation_bucket)
	blob = bucket.blob(companyInformation_csv_output)

	blob.upload_from_filename(companyInformation_vm_filepath)
	


def collect_and_store_daily_quotes(company_symbol_list, company_name_list, price_quote_collectionDate):
	"""Collects daily price quotes from Yahoo! Finance and stores data in a BigQuery table and Cloud Storage in csv format"""
	
	df_price_quotes = pd.DataFrame()
	
	i=1
	for symbol, companyName in zip(company_symbol_list, company_name_list):
		try:
			symbol_price_quote = pandas_datareader.data.DataReader(symbol + '.AT', 'yahoo', price_quote_collectionDate)
		except pandas_datareader._utils.RemoteDataError:
			i+=1
			continue
			
		symbol_price_quote["collection_date"] = [datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')]
		symbol_price_quote["sticker"] = [symbol]
		symbol_price_quote["companyName"] = [companyName]
		symbol_price_quote["date"] = symbol_price_quote.index.strftime('%Y-%m-%d 00:00:00')
		
		df_price_quotes = df_price_quotes.append(symbol_price_quote)
		i+=1
	

	df_price_quotes.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
	'Adj Close': 'adjusted_close', 'Volume': 'volume'}, inplace=True)
	df_price_quotes = df_price_quotes[['collection_date', 'sticker', 'companyName', 'date', 'adjusted_close', 'volume',
	'open', 'high', 'low', 'close']]
	
	return df_price_quotes


def main(): 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  #price_quote_historical_first_date = "1988-01-01"
  price_quote_collectionDate = datetime.datetime.today().strftime('%Y-%m-%d')
 
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_bucket = "athens_stock_exchange"
  company_daily_quotes_csv_output = "company_information/company_Daily_Quotes_new_day.csv"
  start_html_process = time.time()

  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  # Collect daily price quotes and store in BigQuery and Cloud Storage
  df_price_quotes = collect_and_store_daily_quotes(company_symbol_list, company_name_list, price_quote_collectionDate)
  store_information_in_CloudStorage(df_price_quotes, companyInformation_bucket, company_daily_quotes_csv_output)
  
  # Process end message
  print "\nDialy data collection job for " + price_quote_collectionDate + " completed succesfully after " + str(time.time()-start_html_process) + ' seconds.'


if __name__ == '__main__':
  main()

