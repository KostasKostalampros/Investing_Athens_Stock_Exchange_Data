from google.cloud import storage, bigquery
from google.cloud.storage import Blob

from openpyxl import load_workbook
import urllib2
import datetime
import time
import csv

import pandas as pd


# URL of Githab page: https://gist.github.com/hahnicity/45323026693cdde6a116



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
	
	
	
def pull_MorningStar_data(company_symbols, company_name_list, html_collectionDate):

	df_Income_Statement_metadata = pd.Series()
	df_Balance_Sheet_metadata = pd.Series()
	df_Cash_Flow_metadata = pd.Series()
	
	j=1
	for symbol, companyName in zip(company_symbols, company_name_list):
	
		print "Downloading and parsing key stats data for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
	
		# Download all three csv files of company financial statements and store them in Cloud Storage
		url_income_statement = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&reportType=is&period=12&dataType=A&order=asc&columnYear=5&number=3"
		url_balance_sheet = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&reportType=bs&period=12&dataType=A&order=asc&columnYear=5&number=3"
		url_cash_flow_statement = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&reportType=cf&period=12&dataType=A&order=asc&columnYear=5&number=3"
		
		url_list_financial_statements = [url_income_statement, url_balance_sheet, url_cash_flow_statement]
	
		for financial_statement_url in url_list_financial_statements:
		
			html_response = urllib2.urlopen(financial_statement_url)
			csv_reader = csv.reader(html_response, delimiter=',')
			
			# Define parsing algorithm for Income Statement csv output
			if "reportType=is" in financial_statement_url:
				df_output_income_statement = parse_csv_Income_Statement(csv_reader, html_collectionDate, symbol, companyName)
	
			elif "reportType=bs" in financial_statement_url:
				df_output_balance_sheet = parse_csv_Balance_Sheet(csv_reader, html_collectionDate, symbol, companyName)
				
			elif "reportType=cf" in financial_statement_url:
				df_output_cash_flow = parse_csv_Cash_Flow(csv_reader, html_collectionDate, symbol, companyName)
				
		
		df_Income_Statement_metadata = df_Income_Statement_metadata.append(df_output_income_statement)
		df_Balance_Sheet_metadata = df_Balance_Sheet_metadata.append(df_output_balance_sheet)
		df_Cash_Flow_metadata = df_Cash_Flow_metadata.append(df_output_cash_flow)
		
		j+=1
	
	
	df_Income_Statement_metadata.to_csv("/home/Investing/athens_stock_exchange/IncomeStatementColumns.csv", index=False)
	df_Balance_Sheet_metadata.to_csv("/home/Investing/athens_stock_exchange/BalanceSheetColumns.csv", index=False)
	df_Cash_Flow_metadata.to_csv("/home/Investing/athens_stock_exchange/CashFlowColumns.csv", index=False)
	

	"""Uploads a blob to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket("athens_stock_exchange")
	
	blob = bucket.blob("FinancialStatements_metadata/IncomeStatementColumns.csv")
	blob.upload_from_filename("/home/Investing/athens_stock_exchange/IncomeStatementColumns.csv")	
	
	blob2 = bucket.blob("FinancialStatements_metadata/BalanceSheetColumns.csv")
	blob2.upload_from_filename("/home/Investing/athens_stock_exchange/BalanceSheetColumns.csv")	
	
	blob3 = bucket.blob("FinancialStatements_metadata/CashFlowColumns.csv")
	blob3.upload_from_filename("/home/Investing/athens_stock_exchange/CashFlowColumns.csv")	
				


def parse_csv_Income_Statement(csv_reader, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""
	
	column_list = []
	
	for row in csv_reader:
		if len(row) > 1:
			column_list.append(row[0])
	
	df = pd.Series(column_list)
	
	return df
	


def parse_csv_Balance_Sheet(csv_reader, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""

	column_list = []
	
	for row in csv_reader:
		if len(row) > 1:
			column_list.append(row[0])
	
	df = pd.Series(column_list)
	
	return df


	
	
def parse_csv_Cash_Flow(csv_reader, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""

	column_list = []
	
	for row in csv_reader:
		if len(row) > 1:
			column_list.append(row[0])
	
	df = pd.Series(column_list)
	
	return df
	

	

def main():

  # Entry job message
  print "\n Process of parsing Financial Statements started.." 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  html_collectionDate = datetime.datetime.today().strftime('%Y%m%d')
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_bucket = "athens_stock_exchange"
  companyIncomeStatement_csv_output = "company_information/company_Income_Statement_" + html_collectionDate + ".csv"
  companyBalanceSheet_csv_output = "company_information/company_Balance_Sheet_" + html_collectionDate + ".csv"
  companyCashFlow_csv_output = "company_information/company_Cash_Flow_" + html_collectionDate + ".csv"
  start_html_process = time.time()


  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  # Test script only for one symbol
  #company_symbol_list = ["FFGRP", "AEGN"]
  #company_name_list =  ["Folli Follie", "AEGEAN"]
  
  # Pull information from Morning Star and store in server
  pull_MorningStar_data(company_symbol_list, company_name_list, html_collectionDate)
  

  # Process end message
  print "\nProcessed completed succesfully after " + str(time.time()-start_html_process) + ' seconds.'


if __name__ == '__main__':
  main()
