from google.cloud import storage, bigquery
from google.cloud.storage import Blob

from openpyxl import load_workbook
import urllib2
import datetime
import time
from bs4 import BeautifulSoup

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
	
	
def store_information_in_CloudStorage(companyData_df, html_collectionDate_filename, companyInformation_bucket, companyKeyStats_csv_output):
	"""Stores data in Cloud Storage in csv format"""

	companyInformation_vm_filepath = "/home/Investing/athens_stock_exchange/company_information/company_KeyStats_" + html_collectionDate_filename + ".csv"

	companyData_df.to_csv(companyInformation_vm_filepath, index=False)

	"""Uploads a blob to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(companyInformation_bucket)
	blob = bucket.blob(companyKeyStats_csv_output)

	blob.upload_from_filename(companyInformation_vm_filepath)	
	
	


def parse_html_keyStats(company_symbols, company_name_list, html_collectionDate):
	"""Parse HTML files and pulls financials data"""

	list_collection_date = []
	list_sticker = []
	list_companyName = []
	list_price_earnings_ttm = []
	list_price_book = []
	list_price_sales_ttm = []
	list_rev_growth_3years_avg = []
	list_net_income_growth_3_years_avg = []
	list_operating_margin_ttm = []
	list_net_margin_ttm = []
	list_roa_ttm = []
	list_roe_ttm = []
	list_debt_equity = []

	
	j=1
	for symbol, companyName in zip(company_symbols, company_name_list):
			
		# Download HTML file and initiate BeautifulSoup	
		url = "http://quotes.morningstar.com/stockq/c-keystats?&t=XATH:" + symbol + "&region=grc&culture=en-US"
		htmlFile = urllib2.urlopen(url).read()				
		soup = BeautifulSoup(htmlFile[20:-3].replace("\\", ""), 'html.parser')
		
		print "Downloading and parsing key stats data for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
		
		# Populate Collection Date field
		list_collection_date += [html_collectionDate]
		
		# Populate Sticker field 
		list_sticker += [symbol]
		
		# Populate Company Name field 
		list_companyName += [companyName]
		
		# Populate Price Earnings field 
		price_earnings_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[0].findAll("td")[1].getText().encode('utf-8')
		try:
			price_earnings_ttm = float(price_earnings_ttm_tmp)
			list_price_earnings_ttm.append(price_earnings_ttm)
		except ValueError:
			list_price_earnings_ttm.append("")
		
		# Populate Price Book field 
		price_book_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[1].findAll("td")[1].getText().encode('utf-8')		
		try:
			price_book = float(price_book_tmp)
			list_price_book.append(price_book)
		except ValueError:
			list_price_book.append("")
		
		# Populate Price Sales field 
		price_sales_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[2].findAll("td")[1].getText().encode('utf-8')
		try:
			price_sales_ttm = float(price_sales_ttm_tmp)
			list_price_sales_ttm.append(price_sales_ttm)
		except ValueError:
			list_price_sales_ttm.append("")
		
		# Populate Revenue Growth field 
		rev_growth_3years_avg_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[3].findAll("td")[1].getText().encode('utf-8')
		try:
			rev_growth_3years_avg = float(rev_growth_3years_avg_tmp)
			list_rev_growth_3years_avg.append(rev_growth_3years_avg)
		except ValueError:
			list_rev_growth_3years_avg.append("")
		
		# Populate Income Growth field 
		net_income_growth_3_years_avg_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[4].findAll("td")[1].getText().encode('utf-8')
		try:
			net_income_growth_3_years_avg = float(net_income_growth_3_years_avg_tmp)
			list_net_income_growth_3_years_avg.append(net_income_growth_3_years_avg)
		except ValueError:
			list_net_income_growth_3_years_avg.append("")
		
		# Populate Operating Margin field 
		operating_margin_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[5].findAll("td")[1].getText().encode('utf-8')
		try:
			operating_margin_ttm = float(operating_margin_ttm_tmp)
			list_operating_margin_ttm.append(operating_margin_ttm)
		except ValueError:
			list_operating_margin_ttm.append("")
		
		# Populate Net Margin field 
		net_margin_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[6].findAll("td")[1].getText().encode('utf-8')
		try:
			net_margin_ttm = float(net_margin_ttm_tmp)
			list_net_margin_ttm.append(net_margin_ttm)
		except ValueError:		
			list_net_margin_ttm.append("")
		
		# Populate ROA field 
		roa_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[7].findAll("td")[1].getText().encode('utf-8')
		try:
			roa_ttm = float(roa_ttm_tmp)
			list_roa_ttm.append(roa_ttm)
		except ValueError:
			list_roa_ttm.append("")
		
		# Populate ROE field 
		roe_ttm_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[8].findAll("td")[1].getText().encode('utf-8')
		try:
			roe_ttm = float(roe_ttm_tmp)
			list_roe_ttm.append(roe_ttm)
		except ValueError:
			list_roe_ttm.append("")
		
		# Populate Debt Equity field 
		debt_equity_tmp = soup.findAll("tr", {"class":"gr_table_row7"})[9].findAll("td")[1].getText().encode('utf-8')
		try:
			debt_equity = float(debt_equity_tmp)
			list_debt_equity.append(debt_equity)
		except ValueError:		
			list_debt_equity.append("")
		
		j+=1
	
		

	# Create pandas dataframe from tuples
	companyKeyStats_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'price_earnings_ttm':list_price_earnings_ttm, 'price_book':list_price_book, 'price_sales_ttm':list_price_sales_ttm, 'rev_growth_3years_avg':list_rev_growth_3years_avg, 
	'net_income_growth_3_years_avg':list_net_income_growth_3_years_avg, 'operating_margin_ttm':list_operating_margin_ttm, 'net_margin_ttm':list_net_margin_ttm, 
	'roa_ttm':list_roa_ttm, 'roe_ttm':list_roe_ttm, 'debt_equity':list_debt_equity})

	companyKeyStats_df = companyKeyStats_df[['collection_date', 'sticker', 'companyName', 'price_earnings_ttm', 'price_book', 'price_sales_ttm', 
	'rev_growth_3years_avg', 'net_income_growth_3_years_avg', 'operating_margin_ttm', 'net_margin_ttm', 'roa_ttm', 'roe_ttm', 'debt_equity']]
	
	return companyKeyStats_df
	

	

def main():

  # Entry job message
  print "\n Process of parsing Key Stats started.." 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  #html_collectionDate = datetime.datetime.today().strftime('%Y%m%d')
  html_collectionDate = "2018-02-02 00:00:00"
  html_collectionDate_filename = "20180202"
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_bucket = "athens_stock_exchange"
  companyKeyStats_csv_output = "company_information/company_KeyStats_" + html_collectionDate_filename + ".csv"
  start_html_process = time.time()


  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  # Test script only for one symbol
  #company_symbol_list = ["FFGRP", "AEGN"]
  #company_name_list =  ["Folli Follie", "AEGEAN"]
  
  # Pull list of companys' website, address, sector, industry, business description, and number of employees
  companyKeyStats_df = parse_html_keyStats(company_symbol_list, company_name_list, html_collectionDate)
  store_information_in_CloudStorage(companyKeyStats_df, html_collectionDate_filename, companyInformation_bucket, companyKeyStats_csv_output)
  
  

  # Process end message
  print "\nProcessed completed succesfully after " + str(time.time()-start_html_process) + ' seconds.'


if __name__ == '__main__':
  main()
