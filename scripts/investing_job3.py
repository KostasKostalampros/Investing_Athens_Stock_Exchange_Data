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
	
	
def store_information_in_CloudStorage(companyData_df, html_collectionDate_filename, companyInformation_bucket, companyInformation_csv_output):
	"""Stores data in Cloud Storage in csv format"""

	companyInformation_vm_filepath = "/home/Investing/athens_stock_exchange/company_information/companyFinancials_" + html_collectionDate_filename + ".csv"

	companyData_df.to_csv(companyInformation_vm_filepath, index=False)

	"""Uploads a blob to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(companyInformation_bucket)
	blob = bucket.blob(companyInformation_csv_output)

	blob.upload_from_filename(companyInformation_vm_filepath)	
	
	


def parse_html_financials(company_symbols, company_name_list, html_collectionDate):
	"""Parse HTML files and pulls financials data"""

	list_collection_date = []
	list_sticker = []
	list_companyName = []
	list_date = []
	list_revenue = []
	list_gross_margin = []
	list_operating_income = []
	list_operating_margin = []
	list_net_income = []
	list_earnings_per_share = []
	list_dividends = []
	list_payout_ratio = []
	list_shares = []
	list_book_value_per_share = []
	list_operating_cash_flow = []
	list_cap_spending = []
	list_free_cash_flow = []
	list_free_cash_flow_per_share = []
	list_working_capital = []

	column_years = ["Y0", "Y1", "Y2", "Y3", "Y4", "Y5", "Y6", "Y7", "Y8", "Y9", "Y10"]
	
	j=1
	for symbol, companyName in zip(company_symbols, company_name_list):
			
		# Download HTML file and initiate BeautifulSoup	
		url = "http://financials.morningstar.com/finan/financials/getFinancePart.html?&callback=?&t=XATH:" + symbol + "&region=grc&culture=en-US&cur=&order="
		htmlFile = urllib2.urlopen(url).read()				
		soup = BeautifulSoup(htmlFile[20:-3].replace("\\", ""), 'html.parser')
		
		# Check if data exist - If not skip to the next symbol
		if soup.find("th", {"id":"Y0"}) is None:
			print "No Financials data to for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
			j+=1
			continue
		
		print "Downloading and parsing financials data for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
		
		# Populate Collection Date field
		list_collection_date += 11 * [html_collectionDate]
		
		# Populate Sticker field 
		list_sticker += 11 * [symbol]
		
		# Populate Company Name field 
		list_companyName += 11 * [companyName]
		
		
		# Pull Date field
		for year in column_years:
			date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
			if date == "TTM":
				date = "2017-12-01 00:00:00"
			elif date == "Latest Qtr":
				date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
			else:
				date = date + "-01 00:00:00"
			list_date.append(date)
			
			
		metrics_lists_names = [list_revenue, list_gross_margin, list_operating_income, list_operating_margin, list_net_income,
		list_earnings_per_share, list_dividends, list_payout_ratio, list_shares, list_book_value_per_share, list_operating_cash_flow,
		list_cap_spending, list_free_cash_flow, list_free_cash_flow_per_share, list_working_capital]
		
		metrics_id = ["i0", "i1", "i2", "i3", "i4", "i5", "i6", "i91", "i7", "i8", "i9", "i10", "i11", "i90", "i80"]
		
		# Parse all metrics and store output
		for list_name, metric_id in zip(metrics_lists_names, metrics_id):
			data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
			for i in data_all_years:
				try:
					metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
				except ValueError:
					metric_data = ""
				list_name.append(metric_data)
			
		j+=1
		
	# Create pandas dataframe from tuples
	companyFinancials_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'revenue':list_revenue, 'gross_margin':list_gross_margin, 'operating_income':list_operating_income, 
	'operating_margin':list_operating_margin, 'net_income':list_net_income, 'earnings_per_share':list_earnings_per_share, 
	'dividends':list_dividends, 'payout_ratio':list_payout_ratio, 'shares':list_shares, 'book_value_per_share':list_book_value_per_share, 
	'operating_cash_flow':list_operating_cash_flow, 'cap_spending':list_cap_spending,'free_cash_flow':list_free_cash_flow, 
		'free_cash_flow_per_share':list_free_cash_flow_per_share, 'working_capital':list_working_capital})

	companyFinancials_df = companyFinancials_df[['collection_date', 'sticker', 'companyName', 'date', 'revenue', 'gross_margin', 
	'operating_income', 'operating_margin', 'net_income', 'earnings_per_share', 'dividends', 'payout_ratio', 'shares',
	'book_value_per_share', 'operating_cash_flow', 'cap_spending', 'free_cash_flow', 'free_cash_flow_per_share', 'working_capital']]
	
	return companyFinancials_df
	

	
def parse_html_KeyRatios(company_symbol_list, company_name_list, html_collectionDate, html_collectionDate_filename, companyInformation_bucket):
	"""Parse HTML files and pulls Key Ratios data"""
	
	start_html_process = time.time()
	
	df_Margin_of_Sales = pd.DataFrame()
	df_Profitability = pd.DataFrame()
	df_Growth = pd.DataFrame()
	df_Cash_Flow_Ratios = pd.DataFrame()
	df_Balance_Sheet_Items = pd.DataFrame()
	df_Liquidity_Financial_Health = pd.DataFrame()
	df_Efficiency_Ratios = pd.DataFrame()
	
	j=1
	for symbol, companyName in zip(company_symbol_list, company_name_list):
	
		# Download HTML file and initiate BeautifulSoup	
		url = "http://financials.morningstar.com/finan/financials/getKeyStatPart.html?&callback=?&t=XATH:" + symbol + "&region=grc&culture=en-US&cur=&order="
		htmlFile = urllib2.urlopen(url).read()				
		soup = BeautifulSoup(htmlFile[20:-3].replace("\\", ""), 'html.parser')
		
		# Check if data exist - If not skip to the next symbol
		if soup.find("th", {"id":"pr-Y0"}) is None:
			print "No Key Ratios data to for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
			j+=1
			continue
			
		
		print "Downloading and parsing Key Ratios data for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
		
		# Populate Collection Date field
		list_collection_date = []
		list_collection_date += 11 * [html_collectionDate]
		
		# Populate Sticker field 
		list_sticker = []
		list_sticker += 11 * [symbol]
		
		# Populate Company Name field 
		list_companyName = []
		list_companyName += 11 * [companyName]
		
		# Parse individually all Key Ratio tables
		df_ouput_Margin_of_Sales = parse_and_store_data_Margins_of_Sales(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Profitability = parse_and_store_data_Profitability(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Growth = parse_and_store_data_Growth(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Cash_Flow_Ratios = parse_and_store_data_Cash_Flow_Ratios(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Balance_Sheet_Items = parse_and_store_data_Balance_Sheet_Items(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Liquidity_Financial_Health = parse_and_store_data_Liquidity_Financial_Health(soup, list_collection_date, list_sticker, list_companyName)
		df_output_Efficiency_Ratios  = parse_and_store_data_Efficiency_Ratios(soup, list_collection_date, list_sticker, list_companyName)
		
		
		# Append ouput data to dataframes
		df_Margin_of_Sales = df_Margin_of_Sales.append(df_ouput_Margin_of_Sales)
		df_Profitability = df_Profitability.append(df_output_Profitability)
		df_Growth = df_Growth.append(df_output_Growth)
		df_Cash_Flow_Ratios = df_Cash_Flow_Ratios.append(df_output_Cash_Flow_Ratios)
		df_Balance_Sheet_Items = df_Balance_Sheet_Items.append(df_output_Balance_Sheet_Items)
		df_Liquidity_Financial_Health = df_Liquidity_Financial_Health.append(df_output_Liquidity_Financial_Health)
		df_Efficiency_Ratios = df_Efficiency_Ratios.append(df_output_Efficiency_Ratios)
		
		j+=1
		
	
	# Srore output data in csv format and upload in Cloud Storage
	csv_output_filename_Margin_of_Sales = "company_information/company_Margins_of_Sales_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Profitability = "company_information/company_Profitability_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Growth = "company_information/company_Growth_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Cash_Flow_Ratios = "company_information/company_Cash_Flow_Ratios_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Balance_Sheet_Items = "company_information/company_Balance_Sheet_Items_in_Perc_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Liquidity_Financial_Health = "company_information/company_Liquidity_Financial_Health_" + html_collectionDate_filename + ".csv"
	csv_output_filename_Efficiency_Ratios = "company_information/company_Efficiency_Ratios_" + html_collectionDate_filename + ".csv"
	
	store_information_in_CloudStorage(df_Margin_of_Sales, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Margin_of_Sales)
	store_information_in_CloudStorage(df_Profitability, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Profitability)
	store_information_in_CloudStorage(df_Growth, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Growth)
	store_information_in_CloudStorage(df_Cash_Flow_Ratios, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Cash_Flow_Ratios)
	store_information_in_CloudStorage(df_Balance_Sheet_Items, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Balance_Sheet_Items)
	store_information_in_CloudStorage(df_Liquidity_Financial_Health, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Liquidity_Financial_Health)
	store_information_in_CloudStorage(df_Efficiency_Ratios, html_collectionDate_filename, companyInformation_bucket, csv_output_filename_Efficiency_Ratios)
	
	
	
def parse_and_store_data_Margins_of_Sales(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Margin % of Sales data"""

	list_date = []
	list_revenue = []
	list_cost_of_goods_sold = []
	list_gross_margin = []
	list_sga = []
	list_research_and_development = []
	list_other = []
	list_operating_margin = []
	list_net_int_inc_and_other = []
	list_ebt_margin = []

	column_years = ["pr-Y0", "pr-Y1", "pr-Y2", "pr-Y3", "pr-Y4", "pr-Y5", "pr-Y6", "pr-Y7", "pr-Y8", "pr-Y9", "pr-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_revenue, list_cost_of_goods_sold, list_gross_margin, list_sga, list_research_and_development,
	list_other, list_operating_margin, list_net_int_inc_and_other, list_ebt_margin]
	
	metrics_id = ["i12", "i13", "i14", "i15", "i16", "i17", "i18", "i19", "i20"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyMargins_of_Sales_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'revenue':list_revenue, 'cost_of_goods_sold':list_cost_of_goods_sold, 'gross_margin':list_gross_margin, 
	'sga':list_sga, 'research_and_development':list_research_and_development, 'other':list_other, 'operating_margin':list_operating_margin,
	'net_int_inc_and_other':list_net_int_inc_and_other, 'ebt_margin':list_ebt_margin})

	companyMargins_of_Sales_df = companyMargins_of_Sales_df[['collection_date', 'sticker', 'companyName', 'date', 'revenue', 'cost_of_goods_sold', 
	'gross_margin', 'sga', 'research_and_development', 'other', 'operating_margin', 'net_int_inc_and_other', 'ebt_margin']]
	
	return companyMargins_of_Sales_df
	
	
	

def parse_and_store_data_Profitability(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Profitability data"""

	list_date = []
	list_tax_rate = []
	list_net_margin = []
	list_asset_turnover = []
	list_return_on_assets = []
	list_financial_leverage = []
	list_return_on_equity = []
	list_return_on_invested_capital = []
	list_interest_coverage = []

	column_years = ["pr-pro-Y0", "pr-pro-Y1", "pr-pro-Y2", "pr-pro-Y3", "pr-pro-Y4", "pr-pro-Y5", "pr-pro-Y6", "pr-pro-Y7", "pr-pro-Y8", "pr-pro-Y9", "pr-pro-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_tax_rate, list_net_margin, list_asset_turnover, list_return_on_assets, list_financial_leverage,
	list_return_on_equity, list_return_on_invested_capital, list_interest_coverage]
	
	metrics_id = ["i21", "i22", "i23", "i24", "i25", "i26", "i27", "i95"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyProfitability_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'tax_rate':list_tax_rate, 'net_margin':list_net_margin, 'asset_turnover':list_asset_turnover, 
	'return_on_assets':list_return_on_assets, 'financial_leverage':list_financial_leverage, 'return_on_equity':list_return_on_equity,
	'return_on_invested_capital':list_return_on_invested_capital, 'interest_coverage':list_interest_coverage})

	companyProfitability_df = companyProfitability_df[['collection_date', 'sticker', 'companyName', 'date', 'tax_rate', 'net_margin', 
	'asset_turnover', 'return_on_assets', 'financial_leverage', 'return_on_equity', 'return_on_invested_capital', 'interest_coverage']]
	
	return companyProfitability_df
	
	
	
	
def parse_and_store_data_Growth(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Growth data"""

	list_date = []
	list_revenue_YoY = []
	list_revenue_3_Year_Average = []
	list_revenue_5_Year_Average = []
	list_revenue_10_Year_Average = []
	list_operating_income_YoY = []
	list_operating_income_3_Year_Average = []
	list_operating_income_5_Year_Average = []
	list_operating_income_10_Year_Average = []
	list_net_income_YoY = []
	list_net_income_3_Year_Average = []
	list_net_income_5_Year_Average = []
	list_net_income_10_Year_Average = []
	list_eps_YoY = []
	list_eps_3_Year_Average = []
	list_eps_5_Year_Average = []
	list_eps_10_Year_Average = []

	column_years = ["gr-Y0", "gr-Y1", "gr-Y2", "gr-Y3", "gr-Y4", "gr-Y5", "gr-Y6", "gr-Y7", "gr-Y8", "gr-Y9", "gr-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else: 
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_revenue_YoY, list_revenue_3_Year_Average, list_revenue_5_Year_Average, list_revenue_10_Year_Average,
	list_operating_income_YoY, list_operating_income_3_Year_Average, list_operating_income_5_Year_Average,
	list_operating_income_10_Year_Average, list_net_income_YoY, list_net_income_3_Year_Average, list_net_income_5_Year_Average,
	list_net_income_10_Year_Average,list_eps_YoY, list_eps_3_Year_Average, list_eps_5_Year_Average, list_eps_10_Year_Average]
	
	metrics_id = ["i28", "i29", "i30", "i31", "i32", "i33", "i34", "i35", "i81", "i82", "i83", "i84", "i36", "i37", "i38", "i39"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyGrowth_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'revenue_YoY':list_revenue_YoY, 'revenue_3_Year_Average':list_revenue_3_Year_Average, 'revenue_5_Year_Average':list_revenue_5_Year_Average,
	'revenue_10_Year_Average':list_revenue_10_Year_Average, 'operating_income_YoY':list_operating_income_YoY, 'operating_income_3_Year_Average':list_operating_income_3_Year_Average,
	'operating_income_5_Year_Average':list_operating_income_5_Year_Average, 'operating_income_10_Year_Average':list_operating_income_10_Year_Average,
	'net_income_YoY':list_net_income_YoY, 'net_income_3_Year_Average':list_net_income_3_Year_Average, 'net_income_5_Year_Average':list_net_income_5_Year_Average,
	'net_income_10_Year_Average':list_net_income_10_Year_Average, 'eps_YoY':list_eps_YoY, 'eps_3_Year_Average':list_eps_3_Year_Average,
	'eps_5_Year_Average':list_eps_5_Year_Average, 'eps_10_Year_Average':list_eps_10_Year_Average})

	companyGrowth_df = companyGrowth_df[['collection_date', 'sticker', 'companyName', 'date', 'revenue_YoY', 'revenue_3_Year_Average', 
	'revenue_5_Year_Average', 'revenue_10_Year_Average', 'operating_income_YoY', 'operating_income_3_Year_Average', 'operating_income_5_Year_Average',
	'operating_income_10_Year_Average', 'net_income_YoY', 'net_income_3_Year_Average', 'net_income_5_Year_Average', 'net_income_10_Year_Average',
	'eps_YoY', 'eps_3_Year_Average', 'eps_5_Year_Average', 'eps_10_Year_Average']]
	
	return companyGrowth_df
	

	
	
def parse_and_store_data_Cash_Flow_Ratios(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Cash Flow Ratios data"""

	list_date = []
	list_operating_cash_flow_growth_YoY= []
	list_free_cash_flow_growth_YoY = []
	list_cap_ex_as_a_perc_of_sales = []
	list_free_cash_flow_by_sales_as_perc = []
	list_free_cash_flow_by_net_Income = []

	column_years = ["cf-Y0", "cf-Y1", "cf-Y2", "cf-Y3", "cf-Y4", "cf-Y5", "cf-Y6", "cf-Y7", "cf-Y8", "cf-Y9", "cf-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_operating_cash_flow_growth_YoY, list_free_cash_flow_growth_YoY, list_cap_ex_as_a_perc_of_sales,
	list_free_cash_flow_by_sales_as_perc, list_free_cash_flow_by_net_Income]
	
	metrics_id = ["i40", "i41", "i42", "i43", "i44"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyCashFlowRatios_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'operating_cash_flow_growth_YoY':list_operating_cash_flow_growth_YoY, 'free_cash_flow_growth_YoY':list_free_cash_flow_growth_YoY, 
	'cap_ex_as_a_perc_of_sales':list_cap_ex_as_a_perc_of_sales,	'free_cash_flow_by_sales_as_perc':list_free_cash_flow_by_sales_as_perc, 
	'free_cash_flow_by_net_Income':list_free_cash_flow_by_net_Income})

	companyCashFlowRatios_df = companyCashFlowRatios_df[['collection_date', 'sticker', 'companyName', 'date', 'operating_cash_flow_growth_YoY',
	'free_cash_flow_growth_YoY', 'cap_ex_as_a_perc_of_sales', 'free_cash_flow_by_sales_as_perc', 'free_cash_flow_by_net_Income']]
	
	return companyCashFlowRatios_df
	

	
	
def parse_and_store_data_Balance_Sheet_Items(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Balance Sheet Items data"""

	list_date = []
	list_cash_and_short_term_investments = []
	list_accounts_receivable = []
	list_inventory = []
	list_other_current_assets = []
	list_total_current_assets = []
	list_net_pp_and_e = []
	list_intangibles = []
	list_other_long_term_assets = []
	list_total_assets = []
	list_accounts_payable = []
	list_short_term_debt = []
	list_taxes_payable = []
	list_accrued_liabilities = []
	list_other_short_term_liabilities = []
	list_total_current_liabilities = []
	list_long_term_debt = []
	list_other_long_term_liabilities = []
	list_total_liabilities = []
	list_total_stockholders_equity = []
	list_total_liabilities_and_equity = []

	column_years = ["fh-Y0", "fh-Y1", "fh-Y2", "fh-Y3", "fh-Y4", "fh-Y5", "fh-Y6", "fh-Y7", "fh-Y8", "fh-Y9", "fh-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_cash_and_short_term_investments, list_accounts_receivable, list_inventory, list_other_current_assets,
	list_total_current_assets, list_net_pp_and_e, list_intangibles, list_other_long_term_assets, list_total_assets,
	list_accounts_payable, list_short_term_debt, list_taxes_payable,list_accrued_liabilities, list_other_short_term_liabilities,
	list_total_current_liabilities, list_long_term_debt, list_other_long_term_liabilities, list_total_liabilities,
	list_total_stockholders_equity, list_total_liabilities_and_equity]
	
	metrics_id = ["i45", "i46", "i47", "i48", "i49", "i50", "i51", "i52", "i53", "i54", "i55", "i56", "i57", "i58", "i59", "i60", "i61", "i62", "i63", "i64"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyBalanceSheetItems_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'cash_and_short_term_investments':list_cash_and_short_term_investments, 'accounts_receivable':list_accounts_receivable,
	'inventory':list_inventory, 'other_current_assets':list_other_current_assets, 'total_current_assets':list_total_current_assets,
	'net_pp_and_e':list_net_pp_and_e, 'intangibles':list_intangibles, 'other_long_term_assets':list_other_long_term_assets, 'total_assets':list_total_assets,
	'accounts_payable':list_accounts_payable, 'short_term_debt':list_short_term_debt, 'taxes_payable':list_taxes_payable, 'accrued_liabilities':list_accrued_liabilities,
	'other_short_term_liabilities':list_other_short_term_liabilities, 'total_current_liabilities':list_total_current_liabilities, 'long_term_debt':list_long_term_debt,
	'other_long_term_liabilities':list_other_long_term_liabilities, 'total_liabilities':list_total_liabilities, 'total_stockholders_equity':list_total_stockholders_equity,
	'total_liabilities_and_equity':list_total_liabilities_and_equity})

	companyBalanceSheetItems_df = companyBalanceSheetItems_df[['collection_date', 'sticker', 'companyName', 'date', 'cash_and_short_term_investments',
	'accounts_receivable', 'inventory', 'other_current_assets', 'total_current_assets', 'net_pp_and_e', 'intangibles', 'other_long_term_assets',
	'total_assets', 'accounts_payable', 'short_term_debt', 'taxes_payable', 'accrued_liabilities','other_short_term_liabilities', 
	'total_current_liabilities', 'long_term_debt', 'other_long_term_liabilities', 'total_liabilities', 'total_stockholders_equity',
	'total_liabilities_and_equity']]

	return companyBalanceSheetItems_df
	
	


def parse_and_store_data_Liquidity_Financial_Health(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Liquidity Financial Health data"""

	list_date = []
	list_current_ratio = []
	list_quick_ratio = []
	list_financial_leverage = []
	list_debt_equity = []

	column_years = ["lfh-Y0", "lfh-Y1", "lfh-Y2", "lfh-Y3", "lfh-Y4", "lfh-Y5", "lfh-Y6", "lfh-Y7", "lfh-Y8", "lfh-Y9", "lfh-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_current_ratio, list_quick_ratio, list_financial_leverage, list_debt_equity]
	
	
	metrics_id = ["i65", "i66", "i67", "i68"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyLiquidity_Financial_Health_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'current_ratio':list_current_ratio, 'quick_ratio':list_quick_ratio, 'financial_leverage':list_financial_leverage,
	'debt_equity':list_debt_equity})

	companyLiquidity_Financial_Health_df = companyLiquidity_Financial_Health_df[['collection_date', 'sticker', 'companyName', 'date', 'current_ratio',
	'quick_ratio', 'financial_leverage', 'debt_equity']]
	
	return companyLiquidity_Financial_Health_df



def parse_and_store_data_Efficiency_Ratios(soup, list_collection_date, list_sticker, list_companyName):
	"""Parse HTML files and pulls Efficiency_Ratios data"""

	list_date = []
	list_days_sales_outstanding = []
	list_days_inventory = []
	list_payables_period = []
	list_cash_conversion_cycle = []
	list_receivables_turnover = []
	list_inventory_turnover = []
	list_fixed_assets_turnover = []
	list_asset_turnover = []

	column_years = ["ef-Y0", "ef-Y1", "ef-Y2", "ef-Y3", "ef-Y4", "ef-Y5", "ef-Y6", "ef-Y7", "ef-Y8", "ef-Y9", "ef-Y10"]
		
	# Pull Date field
	for year in column_years:
		date = soup.find("th", {"id":year}).getText().encode('utf-8').strip()
		if date == "TTM":
			date = "2017-12-01 00:00:00"
		elif date == "Latest Qtr":
			date = datetime.datetime.today().strftime('%Y-%m-%d') + " 00:00:00"
		else:
			date = date + "-01 00:00:00"
		list_date.append(date)
		
		
	metrics_lists_names = [list_days_sales_outstanding, list_days_inventory, list_payables_period, list_cash_conversion_cycle, list_receivables_turnover,
	list_inventory_turnover, list_fixed_assets_turnover, list_asset_turnover]
	
	metrics_id = ["i69", "i70", "i71", "i72", "i73", "i74", "i75", "i76"]
	
	# Parse all metrics and store output
	for list_name, metric_id in zip(metrics_lists_names, metrics_id):
		data_all_years = soup.find("th", {"id":metric_id}).parent.findAll("td") 		
		for i in data_all_years:
			try:
				metric_data = float(i.getText().encode('utf-8').strip().replace(',', ''))
			except ValueError:
				metric_data = ""
			list_name.append(metric_data)
		
		
	# Create pandas dataframe from tuples
	companyEfficiency_Ratios_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 
	'date':list_date, 'days_sales_outstanding':list_days_sales_outstanding, 'days_inventory':list_days_inventory, 'payables_period':list_payables_period,
	'cash_conversion_cycle':list_cash_conversion_cycle, 'receivables_turnover':list_receivables_turnover, 'inventory_turnover':list_inventory_turnover,
	'fixed_assets_turnover':list_fixed_assets_turnover, 'asset_turnover':list_asset_turnover})

	companyEfficiency_Ratios_df = companyEfficiency_Ratios_df[['collection_date', 'sticker', 'companyName', 'date', 'days_sales_outstanding', 'days_inventory', 
	'payables_period', 'cash_conversion_cycle', 'receivables_turnover', 'inventory_turnover', 'fixed_assets_turnover', 'asset_turnover']]
	
	return companyEfficiency_Ratios_df

	
	

def main():

  # Entry job message
  print "\n Process of parsing financial and key ratio html files started.." 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  #html_collectionDate = datetime.datetime.today().strftime('%Y%m%d')
  html_collectionDate = "2017-11-05 00:00:00"
  html_collectionDate_filename = "20171105"
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_bucket = "athens_stock_exchange"
  companyFinancials_csv_output = "company_information/company_Financials_" + html_collectionDate_filename + ".csv"
  start_html_process = time.time()


  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  # Test script only for one symbol
  #company_symbol_list = ["FFGRP", "AEGN"]
  #company_name_list =  ["Folli Follie", "AEGEAN"]
  
  # Pull list of companys' website, address, sector, industry, business description, and number of employees
  company_financials_df = parse_html_financials(company_symbol_list, company_name_list, html_collectionDate)
  store_information_in_CloudStorage(company_financials_df, html_collectionDate_filename, companyInformation_bucket, companyFinancials_csv_output)
  
  parse_html_KeyRatios(company_symbol_list, company_name_list, html_collectionDate, html_collectionDate_filename, companyInformation_bucket)
  
  

  # Process end message
  print "\nProcessed completed succesfully after " + str(time.time()-start_html_process) + ' seconds.'


if __name__ == '__main__':
  main()
