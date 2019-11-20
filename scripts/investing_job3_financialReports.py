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
	
	
def store_information_in_CloudStorage(companyData_df, companyInformation_bucket, company_csv_output):
	"""Stores data in Cloud Storage in csv format"""

	companyInformation_vm_filepath = "/home/Investing/athens_stock_exchange/company_information/" + company_csv_output + ".csv"

	companyData_df.to_csv(companyInformation_vm_filepath, index=False)

	"""Uploads a blob to the bucket."""
	storage_client = storage.Client()
	bucket = storage_client.get_bucket(companyInformation_bucket)
	blob = bucket.blob("company_information/" + company_csv_output)

	blob.upload_from_filename(companyInformation_vm_filepath)	
	
def request_url(url):

	request = urllib2.Request(url, headers = {'User-Agent' : "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"})
	html_response = urllib2.urlopen(request)
	csv_reader = csv.reader(html_response, delimiter=',')
	
	return csv_reader

	
	
def pull_MorningStar_data(company_symbols, company_name_list, html_collectionDate):

	df_Income_Statement = pd.DataFrame()
	df_Balance_Sheet = pd.DataFrame()
	df_Cash_Flow = pd.DataFrame()
	
	
	j=1
	for symbol, companyName in zip(company_symbols, company_name_list):
	
		print "Downloading and parsing Financial Statement data for " + str(j) + " symbol: " + str(symbol) + " from morningstar.com"
	
		# Download all three csv files of company financial statements and store them in Cloud Storage
		url_income_statement = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&region=grc&reportType=is&period=12&dataType=A&order=asc&columnYear=5&number=3"
		url_balance_sheet = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&region=grc&reportType=bs&period=12&dataType=A&order=asc&columnYear=5&number=3"
		url_cash_flow_statement = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + symbol + "&region=grc&reportType=cf&period=12&dataType=A&order=asc&columnYear=5&number=3"
		
		url_list_financial_statements = [url_income_statement, url_balance_sheet, url_cash_flow_statement]
	
		for financial_statement_url in url_list_financial_statements:
			
			for i in range(10):
				csv_reader = request_url(financial_statement_url)
				data = list(csv_reader)
				if len(data) > 0: break
			
			# Define parsing algorithm for Income Statement csv output
			if "reportType=is" in financial_statement_url:
				if len(data) > 0:
					df_output_income_statement = parse_csv_Income_Statement(data, html_collectionDate, symbol, companyName)
					df_Income_Statement = df_Income_Statement.append(df_output_income_statement)
					print "Income Statement report processed"
				
			
			elif "reportType=bs" in financial_statement_url:
				if len(data) > 0:
					df_output_balance_sheet = parse_csv_Balance_Sheet(data, html_collectionDate, symbol, companyName)
					df_Balance_Sheet = df_Balance_Sheet.append(df_output_balance_sheet)
					print "Balance Sheet report processed"
				
			elif "reportType=cf" in financial_statement_url:
				if len(data) > 0:
					df_output_cash_flow = parse_csv_Cash_Flow(data, html_collectionDate, symbol, companyName)
					df_Cash_Flow = df_Cash_Flow.append(df_output_cash_flow)
					print "Cash Flow report processed"
		
		j+=1
		
	
	return df_Income_Statement, df_Balance_Sheet, df_Cash_Flow
				


def parse_csv_Income_Statement(data, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""

	list_collection_date = []
	list_sticker = []
	list_companyName = []
	list_date = []
	list_revenue = 6 * [""]
	list_cost_of_revenue = 6 * [""]
	list_gross_profit = 6 * [""]
	list_operating_expences_sales_general_administrative = 6 * [""]
	list_other_operating_expenses = 6 * [""]
	list_total_operating_expenses = 6 * [""]
	list_operating_income = 6 * [""]
	list_interest_expense = 6 * [""]
	list_other_income_expense = 6 * [""]
	list_income_before_income_taxes = 6 * [""]
	list_provision_for_income_taxes = 6 * [""]
	list_minority_interest = 6 * [""]
	list_other_income = 6 * [""]
	list_net_income_from_continuing_operations = 6 * [""]
	list_net_income_from_discontinuing_operations = 6 * [""]
	list_net_income_from_discontinuing_operations2 = 6 * [""]
	list_net_income_from_discontinuing_operations3 = 6 * [""]
	list_other = 6 * [""]
	list_net_income = 6 * [""]
	list_net_income_available_to_common_shareholders = 6 * [""]
	list_earnings_per_share_basic = 6 * [""]
	list_earnings_per_share_diluted = 6 * [""]
	list_weighted_average_shares_outstanding_basic = 6 * [""]
	list_weighted_average_shares_outstanding_diluted = 6 * [""]
	list_ebitda = 6 * [""]
	
	list_commision_and_fees = 6 * [""]
	list_compensation_and_benefits = 6 * [""]
	list_cummulative_effect_of_accounting_changes = 6 * [""]
	list_depreciation_and_amortisation = 6 * [""]
	list_extraordinary_items = 6 * [""]
	list_income_from_cont_operations_before_taxes = 6 * [""]
	list_income_tax_expense_benefit = 6 * [""]
	list_interest_income = 6 * [""]
	list_investment_income_net = 6 * [""]
	list_merger_acquisition_and_restructuring = 6 * [""]
	list_net_interest_income = 6 * [""]
	list_nonrecurring_expense = 6 * [""]
	list_operating_expenses = 6 * [""]
	list_other_assets = 6 * [""]
	list_other_distributions = 6 * [""]
	list_other_expense = 6 * [""]
	list_other_expenses = 6 * [""]
	list_other_special_charges = 6 * [""]
	list_policyholder_benefits_and_claims_incurred = 6 * [""]
	list_preferred_distributions = 6 * [""]
	list_preferred_dividend = 6 * [""]
	list_premiums = 6 * [""]
	list_provision_benefit_for_taxes = 6 * [""]
	list_realised_capital_gains_net = 6 * [""]
	list_research_and_development = 6 * [""]
	list_restructuring_merger_and_acquisition = 6 * [""]
	list_securities_gains = 6 * [""]
	list_tech_communication_and_equipment = 6 * [""]
	list_total_benefits_claims_expenses = 6 * [""]
	list_total_costs_and_expenses = 6 * [""]
	list_total_expenses = 6 * [""]
	list_total_interest_expense = 6 * [""]
	list_total_interest_income = 6 * [""]
	list_total_net_revenue = 6 * [""]
	list_total_noninterest_expenses = 6 * [""]
	list_total_noninterest_revenue = 6 * [""]
	list_total_nonoperating_income_net = 6 * [""]
	list_total_revenues = 6 * [""]
	
	
	
	# Populate Collection Date field
	list_collection_date += 6 * [html_collectionDate]
	
	# Populate Sticker field 
	list_sticker += 6 * [symbol]
		
	# Populate Company Name field 
	list_companyName += 6 * [companyName]
	
	# Pull Date
	list_date = map(lambda x: "2017-12-01 00:00:00" if x == "TTM" else x + "-01 00:00:00", data[1][1:])
	
	def flags_false(): 
		flag_earnings = False
		flag_shares_oustanding = False
	
	flags_false()
	
	for row in data[2:]:
		
		
		# Pull Earnings per share
		if row[0] == "Earnings per share":
			flag_earnings = True
			
		elif row[0] == "Basic" and flag_earnings == True:
			list_earnings_per_share_basic = map(lambda x: float(x) if x != "" else '', row[1:])
		
		elif row[0] == "Diluted" and flag_earnings == True:
			list_earnings_per_share_diluted = map(lambda x: float(x) if x != "" else '', row[1:])
			flag_earnings = False
		
		# Pull Weighted average shares outstanding
		elif row[0] == "Weighted average shares outstanding":
			flag_shares_oustanding = True
			
		elif row[0] == "Basic" and flag_shares_oustanding == True:
			list_weighted_average_shares_outstanding_basic = map(lambda x: float(x) if x != "" else '', row[1:])
		
		elif row[0] == "Diluted" and flag_shares_oustanding == True:
			list_weighted_average_shares_outstanding_diluted = map(lambda x: float(x) if x != "" else '', row[1:])
			flag_shares_oustanding = False
		
		# Pull Commissions and fees
		elif row[0] == "Commissions and fees" and len(row) > 1:
			list_commision_and_fees = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
		
		# Pull Compensation and benefits
		elif row[0] == "Compensation and benefits" and len(row) > 1:
			list_compensation_and_benefits = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Cost of revenue
		elif row[0] == "Cost of revenue" and len(row) > 1:
			list_cost_of_revenue = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Cumulative effect of accounting changes
		elif row[0] == "Cumulative effect of accounting changes" and len(row) > 1:
			list_cummulative_effect_of_accounting_changes = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Depreciation and amortization
		elif row[0] == "Depreciation and amortization" and len(row) > 1:
			list_depreciation_and_amortisation = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull EBITDA
		elif row[0] == "EBITDA" and len(row) > 1:
			list_ebitda = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Extraordinary items
		elif row[0] == "Extraordinary items" and len(row) > 1:
			list_extraordinary_items = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Gross profit
		elif row[0] == "Gross profit" and len(row) > 1:
			list_gross_profit = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Income (loss) from cont ops before taxes
		elif row[0] == "Income (loss) from cont ops before taxes" and len(row) > 1:
			list_income_from_cont_operations_before_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()

		
		# Pull Income before income taxes
		elif (row[0] == "Income before income taxes" or row[0] == "Income before taxes") and len(row) > 1:
			list_income_before_income_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()

			
		# Pull Net income from discontinuing ops
		elif (row[0] == "Net income from discontinuing ops" or row[0] == "Income from discontinued operations" or row[0] == "Income from discontinued ops") and len(row) > 1:
			list_net_income_from_discontinuing_operations = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		
		
		# Pull Income tax (expense) benefit
		elif row[0] == "Income tax (expense) benefit" and len(row) > 1:
			list_income_tax_expense_benefit = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
		
		
		# Pull Interest Expense
		elif (row[0] == "Interest Expense" or row[0] == "Interest expenses") and len(row) > 1:
			list_interest_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Interest income
		elif row[0] == "Interest income" and len(row) > 1:
			list_interest_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()

		
		# Pull Investment income, net
		elif row[0] == "Investment income, net" and len(row) > 1:
			list_investment_income_net = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
		
		
		# Pull Merger, acquisition and restructuring
		elif row[0] == "Merger, acquisition and restructuring" and len(row) > 1:
			list_merger_acquisition_and_restructuring = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Minority interest
		elif row[0] == "Minority interest" and len(row) > 1:
			list_minority_interest = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
		
		# Pull Net income
		elif row[0] == "Net income" and len(row) > 1:
			list_net_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Net income available to common shareholders
		elif row[0] == "Net income available to common shareholders" and len(row) > 1:
			list_net_income_available_to_common_shareholders = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
				
			
		# Pull Net income from continuing operations
		elif (row[0] == "Net income from continuing operations" or row[0] == "Net income from continuing ops") and len(row) > 1:
			list_net_income_from_continuing_operations = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Net interest income
		elif row[0] == "Net interest income" and len(row) > 1:
			list_net_interest_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Nonrecurring expense
		elif row[0] == "Nonrecurring expense" and len(row) > 1:
			list_nonrecurring_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Operating expenses
		elif row[0] == "Operating expenses" and len(row) > 1:
			list_operating_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Operating income
		elif row[0] == "Operating income" and len(row) > 1:
			list_operating_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other
		elif row[0] == "Other" and len(row) > 1:
			list_other = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other assets
		elif row[0] == "Other assets" and len(row) > 1:
			list_other_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other distributions
		elif row[0] == "Other distributions" and len(row) > 1:
			list_other_distributions = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()	
			
		# Pull Other expense
		elif row[0] == "Other expense" and len(row) > 1:
			list_other_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other expenses
		elif row[0] == "Other expenses" and len(row) > 1:
			list_other_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Other income
		elif (row[0] == "Other income" or row[0] == "Other income (loss)") and len(row) > 1:
			list_other_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Other income (expense)
		elif row[0] == "Other income (expense)" and len(row) > 1:
			list_other_income_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other operating expenses
		elif row[0] == "Other operating expenses" and len(row) > 1:
			list_other_operating_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Other special charges
		elif row[0] == "Other special charges" and len(row) > 1:
			list_other_special_charges = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Policyholder benefits and claims incurred
		elif row[0] == "Policyholder benefits and claims incurred" and len(row) > 1:
			list_policyholder_benefits_and_claims_incurred = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Preferred distributions
		elif row[0] == "Preferred distributions" and len(row) > 1:
			list_preferred_distributions = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Preferred dividend
		elif row[0] == "Preferred dividend" and len(row) > 1:
			list_preferred_dividend = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Premiums
		elif row[0] == "Premiums" and len(row) > 1:
			list_premiums = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Provision (benefit) for taxes
		elif row[0] == "Provision (benefit) for taxes" and len(row) > 1:
			list_provision_benefit_for_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()	
			
		# Pull Provision for income taxes
		elif row[0] == "Provision for income taxes" and len(row) > 1:
			list_provision_for_income_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Realized capital gains (losses), net
		elif row[0] == "Realized capital gains (losses), net" and len(row) > 1:
			list_realised_capital_gains_net = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Research and development
		elif row[0] == "Research and development" and len(row) > 1:
			list_research_and_development = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Restructuring, merger and acquisition
		elif row[0] == "Restructuring, merger and acquisition" and len(row) > 1:
			list_restructuring_merger_and_acquisition = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Revenue
		elif row[0] == "Revenue" and len(row) > 1:
			list_revenue = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Sales, General and administrative
		elif (row[0] == "Sales, General and administrative" or row[0] == "Selling, general and administrative") and len(row) > 1:
			list_operating_expences_sales_general_administrative = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Securities gains (losses)
		elif row[0] == "Securities gains (losses)" and len(row) > 1:
			list_securities_gains = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Tech, communication and equipment
		elif row[0] == "Tech, communication and equipment" and len(row) > 1:
			list_tech_communication_and_equipment = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total benefits, claims and expenses
		elif row[0] == "Total benefits, claims and expenses" and len(row) > 1:
			list_total_benefits_claims_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total costs and expenses
		elif row[0] == "Total costs and expenses" and len(row) > 1:
			list_total_costs_and_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total expenses
		elif row[0] == "Total expenses" and len(row) > 1:
			list_total_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
			
		# Pull Total interest expense
		elif row[0] == "Total interest expense" and len(row) > 1:
			list_total_interest_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total interest income
		elif row[0] == "Total interest income" and len(row) > 1:
			list_total_interest_income = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total net revenue
		elif row[0] == "Total net revenue" and len(row) > 1:
			list_total_net_revenue = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total noninterest expenses
		elif row[0] == "Total noninterest expenses" and len(row) > 1:
			xxx = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total noninterest revenue
		elif row[0] == "Total noninterest revenue" and len(row) > 1:
			xxx = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total nonoperating income, net
		elif row[0] == "Total nonoperating income, net" and len(row) > 1:
			list_total_nonoperating_income_net = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total operating expenses
		elif row[0] == "Total operating expenses" and len(row) > 1:
			list_total_operating_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
			
		# Pull Total revenues
		elif row[0] == "Total revenues" and len(row) > 1:
			list_total_revenues = map(lambda x: float(x) if x != "" else '', row[1:])
			flags_false()
	

	# Create pandas dataframe from tuples
	companyIncomeStatement_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 'date':list_date, 'revenue':list_revenue, 'cost_of_revenue':list_cost_of_revenue, 'gross_profit':list_gross_profit, 'operating_expences_sales_general_administrative':list_operating_expences_sales_general_administrative, 'other_operating_expenses':list_other_operating_expenses, 'total_operating_expenses':list_total_operating_expenses, 'operating_income':list_operating_income, 'interest_expense':list_interest_expense, 'other_income_expense':list_other_income_expense, 'income_before_income_taxes':list_income_before_income_taxes,'provision_for_income_taxes':list_provision_for_income_taxes, 'minority_interest':list_minority_interest, 'other_income':list_other_income,'net_income_from_continuing_operations':list_net_income_from_continuing_operations,'net_income_from_discontinuing_operations':list_net_income_from_discontinuing_operations, 'net_income_from_discontinuing_operations2':list_net_income_from_discontinuing_operations2,'net_income_from_discontinuing_operations3':list_net_income_from_discontinuing_operations3,'other':list_other, 'net_income':list_net_income, 'net_income_available_to_common_shareholders':list_net_income_available_to_common_shareholders, 'earnings_per_share_basic':list_earnings_per_share_basic, 'earnings_per_share_diluted':list_earnings_per_share_diluted, 'weighted_average_shares_outstanding_basic':list_weighted_average_shares_outstanding_basic, 'weighted_average_shares_outstanding_diluted':list_weighted_average_shares_outstanding_diluted, 'ebitda':list_ebitda, 'commision_and_fees':list_commision_and_fees, 'compensation_and_benefits':list_compensation_and_benefits, 'cummulative_effect_of_accounting_changes':list_cummulative_effect_of_accounting_changes, 'depreciation_and_amortisation':list_depreciation_and_amortisation, 'extraordinary_items':list_extraordinary_items, 'income_from_cont_operations_before_taxes':list_income_from_cont_operations_before_taxes, 'income_tax_expense_benefit':list_income_tax_expense_benefit, 'interest_income':list_interest_income, 'investment_income_net':list_investment_income_net, 'merger_acquisition_and_restructuring':list_merger_acquisition_and_restructuring, 'net_interest_income':list_net_interest_income, 'nonrecurring_expense':list_nonrecurring_expense, 'operating_expenses':list_operating_expenses, 'policyholder_benefits_and_claims_incurred':list_policyholder_benefits_and_claims_incurred, 'other_distributions':list_other_distributions, 'other_expense':list_other_expense, 'other_expenses':list_other_expenses, 'other_special_charges':list_other_special_charges, 'policyholder_benefits_and_claims_incurred':list_policyholder_benefits_and_claims_incurred, 'preferred_distributions':list_preferred_distributions, 'preferred_dividend':list_preferred_dividend, 'premiums':list_premiums, 'provision_benefit_for_taxes':list_provision_benefit_for_taxes, 'realised_capital_gains_net':list_realised_capital_gains_net, 'research_and_development':list_research_and_development, 'restructuring_merger_and_acquisition':list_restructuring_merger_and_acquisition, 'securities_gains':list_securities_gains, 'tech_communication_and_equipment':list_tech_communication_and_equipment, 'total_benefits_claims_expenses':list_total_benefits_claims_expenses, 'total_costs_and_expenses':list_total_costs_and_expenses, 'total_expenses':list_total_expenses, 'total_interest_expense':list_total_interest_expense, 'total_interest_income':list_total_interest_income, 'total_net_revenue':list_total_net_revenue, 'total_noninterest_expenses':list_total_noninterest_expenses, 'total_noninterest_revenue':list_total_noninterest_revenue, 'total_nonoperating_income_net':list_total_nonoperating_income_net, 'total_revenues':list_total_revenues})

	companyIncomeStatement_df = companyIncomeStatement_df[['collection_date', 'sticker', 'companyName', 'date', 'revenue', 'cost_of_revenue', 'gross_profit', 'operating_expences_sales_general_administrative', 'other_operating_expenses', 'total_operating_expenses', 'operating_income', 'interest_expense', 'other_income_expense', 'income_before_income_taxes','provision_for_income_taxes', 'minority_interest', 'other_income', 'net_income_from_continuing_operations', 'net_income_from_discontinuing_operations', 'net_income_from_discontinuing_operations2','net_income_from_discontinuing_operations3','other', 'net_income', 'net_income_available_to_common_shareholders', 'earnings_per_share_basic', 'earnings_per_share_diluted', 'weighted_average_shares_outstanding_basic', 'weighted_average_shares_outstanding_diluted', 'ebitda', 'commision_and_fees', 'compensation_and_benefits', 'cummulative_effect_of_accounting_changes', 'depreciation_and_amortisation', 'extraordinary_items', 'income_from_cont_operations_before_taxes', 'income_tax_expense_benefit', 'interest_income', 'investment_income_net', 'merger_acquisition_and_restructuring', 'net_interest_income', 'nonrecurring_expense', 'operating_expenses', 'policyholder_benefits_and_claims_incurred', 'other_distributions', 'other_expense', 'other_expenses', 'other_special_charges', 'policyholder_benefits_and_claims_incurred', 'preferred_distributions', 'preferred_dividend', 'premiums', 'provision_benefit_for_taxes', 'realised_capital_gains_net', 'research_and_development', 'restructuring_merger_and_acquisition', 'securities_gains', 'tech_communication_and_equipment', 'total_benefits_claims_expenses', 'total_costs_and_expenses', 'total_expenses', 'total_interest_expense', 'total_interest_income', 'total_net_revenue', 'total_noninterest_expenses', 'total_noninterest_revenue', 'total_nonoperating_income_net', 'total_revenues']]
	
	
	return companyIncomeStatement_df
	


def parse_csv_Balance_Sheet(data, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""

	list_collection_date = []
	list_sticker = []
	list_companyName = []
	list_date = []
	
	# Assets
	list_currentAssets_cash_and_cash_equivalents = 5 * [""]
	list_currentAssets_cash_and_due_from_banks = 5 * [""]
	list_currentAssets_short_term_investments = 5 * [""]
	list_currentAssets_total_cash = 5 * [""]
	list_currentAssets_inventories = 5 * [""]
	list_currentAssets_deposits = 5 * [""]
	list_currentAssets_derivative_assets = 5 * [""]
	list_currentAssets_accumulated_other_comprehensive_income = 5 * [""]
	list_currentAssets_trading_assets = 5 * [""]
	list_currentAssets_other_current_assets = 5 * [""]
	list_currentAssets_total_current_assets = 5 * [""]
	
	list_nonCurrentAssets_land = 5 * [""]
	list_nonCurrentAssets_fixtures_and_equipment = 5 * [""]
	list_nonCurrentAssets_other_properties = 5 * [""]
	list_nonCurrentAssets_property_and_equipment = 5 * [""]
	list_nonCurrentAssets_property_and_equipment_at_cost = 5 * [""]
	list_nonCurrentAssets_property_and_equipment_net = 5 * [""]
	list_nonCurrentAssets_real_estate_properties = 5 * [""]
	list_nonCurrentAssets_real_estate_properties_net = 5 * [""]
	list_nonCurrentAssets_accumulated_depreciation = 5 * [""]
	list_nonCurrentAssets_goodwill = 5 * [""]
	list_nonCurrentAssets_intangible_assets = 5 * [""]
	list_nonCurrentAssets_other_intangible_assets = 5 * [""]
	list_nonCurrentAssets_deferred_revenues = 5 * [""]
	list_nonCurrentAssets_receivables = 5 * [""]
	list_nonCurrentAssets_premiums_and_other_receivables = 5 * [""]
	list_nonCurrentAssets_unearned_premiums = 5 * [""]
	list_nonCurrentAssets_prepaid_expenses = 5 * [""]
	list_nonCurrentAssets_general_partner = 5 * [""]
	list_nonCurrentAssets_minority_interest = 5 * [""]
	list_nonCurrentAssets_net_loans = 5 * [""]
	list_nonCurrentAssets_investments = 5 * [""]
	list_nonCurrentAssets_equity_and_other_investments = 5 * [""]
	list_nonCurrentAssets_other_long_term_assets = 5 * [""]
	list_nonCurrentAssets_total_non_current_assets = 5 * [""]
	list_other_assets = 5 * [""]
	list_total_assets = 5 * [""]
	
	# Liabilities
	list_currentLiabilities_accounts_payable = 5 * [""]
	list_currentLiabilities_payables_and_accrued_expenses = 5 * [""]
	list_currentLiabilities_short_term_debt = 5 * [""]
	list_currentLiabilities_short_term_borrowing = 5 * [""]
	list_currentLiabilities_capital_leases = 5 * [""]
	list_currentLiabilities_taxes_payable = 5 * [""]
	list_currentAssets_derivative_liabilities = 5 * [""]
	list_currentLiabilities_other_current_liabilities = 5 * [""]
	list_currentLiabilities_total_current_liabilities = 5 * [""]

	list_nonCurrentLiabilities_accrued_liabilities = 5 * [""]
	list_nonCurrentLiabilities_debt_securities = 5 * [""]
	list_nonCurrentLiabilities_long_term_debt = 5 * [""]
	list_nonCurrentLiabilities_capital_leases = 5 * [""]
	list_nonCurrentLiabilities_deferred_taxes_liabilities = 5 * [""]
	list_nonCurrentAssets_deferred_income_taxes = 5 * [""]
	list_nonCurrentLiabilities_pensions_and_other_benefits = 5 * [""]
	list_nonCurrentLiabilities_other_long_term_liabilities = 5 * [""]
	list_nonCurrentLiabilities_total_non_current_liabilities = 5 * [""]
	list_other_liabilities = 5 * [""]
	list_total_liabilities = 5 * [""]
	
	# Stockholder's equity
	list_stockholdersEquity_additional_paid_in_capital = 5 * [""]
	list_stockholdersEquity_retained_earnings = 5 * [""]
	list_stockholdersEquity_treasurey_stock = 5 * [""]
	list_stockholdersEquity_preferred_stock = 5 * [""]
	list_stockholdersEquity_common_stock = 5 * [""]
	list_total_stockholders_equity = 5 * [""]
	list_total_liabilities_and_stockholders_equity = 5 * [""]
	
	
	# Populate Collection Date field
	list_collection_date += 5 * [html_collectionDate]
	
	# Populate Sticker field 
	list_sticker += 5 * [symbol]
		
	# Populate Company Name field 
	list_companyName += 5 * [companyName]

	
	# Pull Date
	list_date = map(lambda x: x + "-01 00:00:00", data[1][1:])
	
	
	for row in data[2:]:
		
		
		# Pull Earnings per share
		if row[0] == "Accounts payable":
			list_currentLiabilities_accounts_payable = map(lambda x: float(x) if x != "" else '', row[1:])
		
		# Pull Accrued liabilities
		elif row[0] == "Accrued liabilities":
			list_nonCurrentLiabilities_accrued_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Accumulated Depreciation
		elif row[0] == "Accumulated Depreciation":
			list_nonCurrentAssets_accumulated_depreciation = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Accumulated other comprehensive income
		elif row[0] == "Accumulated other comprehensive income":
			list_currentAssets_accumulated_other_comprehensive_income = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Additional paid-in capital
		elif row[0] == "Additional paid-in capital":
			list_stockholdersEquity_additional_paid_in_capital = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Capital leases
		elif row[0] == "Capital leases":
			list_nonCurrentLiabilities_capital_leases = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash and cash equivalents
		elif row[0] == "Cash and cash equivalents":
			list_currentAssets_cash_and_cash_equivalents = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash and due from banks
		elif row[0] == "Cash and due from banks":
			list_currentAssets_cash_and_due_from_banks = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Common stock
		elif row[0] == "Common stock":
			list_stockholdersEquity_common_stock = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Debt securities
		elif row[0] == "Debt securities":
			list_nonCurrentLiabilities_debt_securities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deferred income taxes
		elif row[0] == "Deferred income taxes":
			list_nonCurrentAssets_deferred_income_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deferred revenues
		elif row[0] == "Deferred revenues":
			list_nonCurrentAssets_deferred_revenues = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deferred taxes liabilities
		elif row[0] == "Deferred taxes liabilities":
			list_nonCurrentLiabilities_deferred_taxes_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deposits
		elif row[0] == "Deposits":
			list_currentAssets_deposits = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Derivative assets
		elif row[0] == "Derivative assets":
			list_currentAssets_derivative_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Derivative liabilities
		elif row[0] == "Derivative liabilities":
			list_currentAssets_derivative_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Equity and other investments
		elif row[0] == "Equity and other investments":
			list_nonCurrentAssets_equity_and_other_investments = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Investments
		elif row[0] == "Investments":
			list_nonCurrentAssets_investments = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Fixtures and equipment
		elif row[0] == "Fixtures and equipment":
			list_nonCurrentAssets_fixtures_and_equipment = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull General Partner
		elif row[0] == "General Partner":
			list_nonCurrentAssets_general_partner = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Goodwill
		elif row[0] == "Goodwill":
			list_nonCurrentAssets_goodwill = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Property and equipment
		elif row[0] == "Property and equipment" or row[0] == "Premises and equipment":
			list_nonCurrentAssets_property_and_equipment = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# Pull Property and equipment, at cost
		elif row[0] == "Property and equipment, at cost" or row[0] == "Gross property, plant and equipment":
			list_nonCurrentAssets_property_and_equipment_at_cost = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# PullProperty, plant and equipment, net
		elif row[0] == "Property, plant and equipment, net" or row[0] == "Net property, plant and equipment":
			list_nonCurrentAssets_property_and_equipment_net = map(lambda x: float(x) if x != "" else '', row[1:])

			
		# Pull Intangible assets
		elif row[0] == "Intangible assets":
			list_nonCurrentAssets_intangible_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Inventories
		elif row[0] == "Inventories":
			list_currentAssets_inventories = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Land
		elif row[0] == "Land":
			list_nonCurrentAssets_land = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Long-term debt
		elif row[0] == "Long-term debt":
			list_nonCurrentLiabilities_long_term_debt = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Minority interest
		elif row[0] == "Minority interest":
			list_nonCurrentAssets_minority_interest = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net loans
		elif row[0] == "Net loans":
			list_nonCurrentAssets_net_loans = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other assets
		elif row[0] == "Other assets":
			list_other_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other current assets
		elif row[0] == "Other current assets":
			list_currentAssets_other_current_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other current liabilities
		elif row[0] == "Other current liabilities":
			list_currentLiabilities_other_current_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other intangible assets
		elif row[0] == "Other intangible assets":
			list_nonCurrentAssets_other_intangible_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other liabilities
		elif row[0] == "Other liabilities":
			list_other_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other long-term assets
		elif row[0] == "Other long-term assets":
			list_nonCurrentAssets_other_long_term_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other long-term liabilities
		elif row[0] == "Other long-term liabilities":
			list_nonCurrentLiabilities_other_long_term_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other properties
		elif row[0] == "Other properties":
			list_nonCurrentAssets_other_properties = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Payables and accrued expenses
		elif row[0] == "Payables and accrued expenses":
			list_currentLiabilities_payables_and_accrued_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Pensions and other benefits
		elif row[0] == "Pensions and other benefits" or row[0] == "Pensions and other postretirement benefits":
			list_nonCurrentLiabilities_pensions_and_other_benefits = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# Pull Preferred stock
		elif row[0] == "Preferred stock":
			list_stockholdersEquity_preferred_stock = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Premiums and other receivables
		elif row[0] == "Premiums and other receivables":
			list_nonCurrentAssets_premiums_and_other_receivables = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Prepaid expenses
		elif row[0] == "Prepaid expenses":
			list_nonCurrentAssets_prepaid_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Real estate properties
		elif row[0] == "Real estate properties":
			list_nonCurrentAssets_real_estate_properties = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Real estate properties, net
		elif row[0] == "Real estate properties, net":
			list_nonCurrentAssets_real_estate_properties_net = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Receivables
		elif row[0] == "Receivables":
			list_nonCurrentAssets_receivables = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Retained earnings
		elif row[0] == "Retained earnings":
			list_stockholdersEquity_retained_earnings = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Short-term borrowing
		elif row[0] == "Short-term borrowing":
			list_currentLiabilities_short_term_borrowing = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Short-term debt
		elif row[0] == "Short-term debt":
			list_currentLiabilities_short_term_debt = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Short-term investments
		elif row[0] == "Short-term investments":
			list_currentAssets_short_term_investments = map(lambda x: float(x) if x != "" else '', row[1:])
	
		# Pull Taxes payable
		elif row[0] == "Taxes payable":
			list_currentLiabilities_taxes_payable = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total assets
		elif row[0] == "Total assets":
			list_total_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total cash
		elif row[0] == "Total cash":
			list_currentAssets_total_cash = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total current assets
		elif row[0] == "Total current assets":
			list_currentAssets_total_current_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total current liabilities
		elif row[0] == "Total current liabilities":
			list_currentLiabilities_total_current_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total liabilities
		elif row[0] == "Total liabilities":
			list_total_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total liabilities and stockholders' equity
		elif row[0] == "Total liabilities and stockholders' equity":
			list_total_liabilities_and_stockholders_equity = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total non-current assets
		elif row[0] == "Total non-current assets":
			list_nonCurrentAssets_total_non_current_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total non-current liabilities
		elif row[0] == "Total non-current liabilities":
			list_nonCurrentLiabilities_total_non_current_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Total stockholders' equity
		elif row[0] == "Total stockholders' equity":
			list_total_stockholders_equity = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Trading assets
		elif row[0] == "Trading assets":
			list_currentAssets_trading_assets = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Treasury stock
		elif row[0] == "Treasury stock":
			list_stockholdersEquity_treasurey_stock = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Unearned premiums
		elif row[0] == "Unearned premiums":
			list_nonCurrentAssets_unearned_premiums = map(lambda x: float(x) if x != "" else '', row[1:])
		
		
	
		

	# Create pandas dataframe from tuples
	companyBalanceSheet_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 'date':list_date, 'currentAssets_cash_and_cash_equivalents':list_currentAssets_cash_and_cash_equivalents, 'currentAssets_cash_and_due_from_banks':list_currentAssets_cash_and_due_from_banks, 'currentAssets_short_term_investments':list_currentAssets_short_term_investments, 'currentAssets_total_cash':list_currentAssets_total_cash, 'currentAssets_inventories':list_currentAssets_inventories, 'currentAssets_deposits':list_currentAssets_deposits, 'currentAssets_derivative_assets':list_currentAssets_derivative_assets, 'currentAssets_accumulated_other_comprehensive_income':list_currentAssets_accumulated_other_comprehensive_income, 'currentAssets_trading_assets':list_currentAssets_trading_assets, 'currentAssets_other_current_assets':list_currentAssets_other_current_assets, 'currentAssets_total_current_assets':list_currentAssets_total_current_assets, 'nonCurrentAssets_land':list_nonCurrentAssets_land, 'nonCurrentAssets_fixtures_and_equipment':list_nonCurrentAssets_fixtures_and_equipment, 'nonCurrentAssets_other_properties':list_nonCurrentAssets_other_properties, 'nonCurrentAssets_property_and_equipment':list_nonCurrentAssets_property_and_equipment, 'nonCurrentAssets_property_and_equipment_at_cost':list_nonCurrentAssets_property_and_equipment_at_cost, 'nonCurrentAssets_property_and_equipment_net':list_nonCurrentAssets_property_and_equipment_net, 'nonCurrentAssets_real_estate_properties':list_nonCurrentAssets_real_estate_properties, 'nonCurrentAssets_real_estate_properties_net':list_nonCurrentAssets_real_estate_properties_net, 'nonCurrentAssets_accumulated_depreciation':list_nonCurrentAssets_accumulated_depreciation, 'nonCurrentAssets_goodwill':list_nonCurrentAssets_goodwill, 'nonCurrentAssets_intangible_assets':list_nonCurrentAssets_intangible_assets, 'nonCurrentAssets_other_intangible_assets':list_nonCurrentAssets_other_intangible_assets, 'nonCurrentAssets_deferred_revenues':list_nonCurrentAssets_deferred_revenues, 'nonCurrentAssets_receivables':list_nonCurrentAssets_receivables, 'nonCurrentAssets_premiums_and_other_receivables':list_nonCurrentAssets_premiums_and_other_receivables, 'nonCurrentAssets_unearned_premiums':list_nonCurrentAssets_unearned_premiums, 'nonCurrentAssets_prepaid_expenses':list_nonCurrentAssets_prepaid_expenses, 'nonCurrentAssets_general_partner':list_nonCurrentAssets_general_partner, 'nonCurrentAssets_minority_interest':list_nonCurrentAssets_minority_interest, 'nonCurrentAssets_net_loans':list_nonCurrentAssets_net_loans, 'nonCurrentAssets_investments':list_nonCurrentAssets_investments, 'nonCurrentAssets_equity_and_other_investments':list_nonCurrentAssets_equity_and_other_investments, 'nonCurrentAssets_other_long_term_assets':list_nonCurrentAssets_other_long_term_assets, 'nonCurrentAssets_total_non_current_assets':list_nonCurrentAssets_total_non_current_assets, 'other_assets':list_other_assets, 'total_assets':list_total_assets, 'currentLiabilities_accounts_payable':list_currentLiabilities_accounts_payable, 'currentLiabilities_payables_and_accrued_expenses':list_currentLiabilities_payables_and_accrued_expenses, 'currentLiabilities_short_term_debt':list_currentLiabilities_short_term_debt, 'currentLiabilities_short_term_borrowing':list_currentLiabilities_short_term_borrowing, 'currentLiabilities_capital_leases':list_currentLiabilities_capital_leases, 'currentLiabilities_taxes_payable':list_currentLiabilities_taxes_payable, 'currentAssets_derivative_liabilities':list_currentAssets_derivative_liabilities, 'currentLiabilities_other_current_liabilities':list_currentLiabilities_other_current_liabilities, 'currentLiabilities_total_current_liabilities':list_currentLiabilities_total_current_liabilities, 'nonCurrentLiabilities_accrued_liabilities':list_nonCurrentLiabilities_accrued_liabilities, 'nonCurrentLiabilities_debt_securities':list_nonCurrentLiabilities_debt_securities, 'nonCurrentLiabilities_long_term_debt':list_nonCurrentLiabilities_long_term_debt, 'nonCurrentLiabilities_capital_leases':list_nonCurrentLiabilities_capital_leases, 'nonCurrentLiabilities_deferred_taxes_liabilities':list_nonCurrentLiabilities_deferred_taxes_liabilities, 'nonCurrentAssets_deferred_income_taxes':list_nonCurrentAssets_deferred_income_taxes, 'nonCurrentLiabilities_pensions_and_other_benefits':list_nonCurrentLiabilities_pensions_and_other_benefits, 'nonCurrentLiabilities_other_long_term_liabilities':list_nonCurrentLiabilities_other_long_term_liabilities, 'nonCurrentLiabilities_total_non_current_liabilities':list_nonCurrentLiabilities_total_non_current_liabilities, 'other_liabilities':list_other_liabilities, 'total_liabilities':list_total_liabilities, 'stockholdersEquity_additional_paid_in_capital':list_stockholdersEquity_additional_paid_in_capital, 'stockholdersEquity_retained_earnings':list_stockholdersEquity_retained_earnings, 'stockholdersEquity_treasurey_stock':list_stockholdersEquity_treasurey_stock, 'stockholdersEquity_preferred_stock':list_stockholdersEquity_preferred_stock, 'stockholdersEquity_common_stock':list_stockholdersEquity_common_stock, 'total_stockholders_equity':list_total_stockholders_equity, 'total_liabilities_and_stockholders_equity':list_total_liabilities_and_stockholders_equity})
	

	companyBalanceSheet_df = companyBalanceSheet_df[['collection_date', 'sticker', 'companyName', 'date', 'currentAssets_cash_and_cash_equivalents', 'currentAssets_cash_and_due_from_banks', 'currentAssets_short_term_investments', 'currentAssets_total_cash', 'currentAssets_inventories', 'currentAssets_deposits', 'currentAssets_derivative_assets', 'currentAssets_accumulated_other_comprehensive_income', 'currentAssets_trading_assets', 'currentAssets_other_current_assets', 'currentAssets_total_current_assets', 'nonCurrentAssets_land', 'nonCurrentAssets_fixtures_and_equipment', 'nonCurrentAssets_other_properties', 'nonCurrentAssets_property_and_equipment', 'nonCurrentAssets_property_and_equipment_at_cost', 'nonCurrentAssets_property_and_equipment_net', 'nonCurrentAssets_real_estate_properties', 'nonCurrentAssets_real_estate_properties_net', 'nonCurrentAssets_accumulated_depreciation', 'nonCurrentAssets_goodwill', 'nonCurrentAssets_intangible_assets', 'nonCurrentAssets_other_intangible_assets', 'nonCurrentAssets_deferred_revenues', 'nonCurrentAssets_receivables', 'nonCurrentAssets_premiums_and_other_receivables', 'nonCurrentAssets_unearned_premiums', 'nonCurrentAssets_prepaid_expenses', 'nonCurrentAssets_general_partner', 'nonCurrentAssets_minority_interest', 'nonCurrentAssets_net_loans', 'nonCurrentAssets_investments', 'nonCurrentAssets_equity_and_other_investments', 'nonCurrentAssets_other_long_term_assets', 'nonCurrentAssets_total_non_current_assets', 'other_assets', 'total_assets', 'currentLiabilities_accounts_payable', 'currentLiabilities_payables_and_accrued_expenses', 'currentLiabilities_short_term_debt', 'currentLiabilities_short_term_borrowing', 'currentLiabilities_capital_leases', 'currentLiabilities_taxes_payable', 'currentAssets_derivative_liabilities', 'currentLiabilities_other_current_liabilities', 'currentLiabilities_total_current_liabilities', 'nonCurrentLiabilities_accrued_liabilities', 'nonCurrentLiabilities_debt_securities', 'nonCurrentLiabilities_long_term_debt', 'nonCurrentLiabilities_capital_leases', 'nonCurrentLiabilities_deferred_taxes_liabilities', 'nonCurrentAssets_deferred_income_taxes', 'nonCurrentLiabilities_pensions_and_other_benefits', 'nonCurrentLiabilities_other_long_term_liabilities', 'nonCurrentLiabilities_total_non_current_liabilities', 'other_liabilities', 'total_liabilities', 'stockholdersEquity_additional_paid_in_capital', 'stockholdersEquity_retained_earnings', 'stockholdersEquity_treasurey_stock', 'stockholdersEquity_preferred_stock', 'stockholdersEquity_common_stock', 'total_stockholders_equity', 'total_liabilities_and_stockholders_equity']]
	
	
	return companyBalanceSheet_df


	
	
def parse_csv_Cash_Flow(data, html_collectionDate, symbol, companyName):
	"""Parse HTML files and pulls financials data"""

	list_collection_date = []
	list_sticker = []
	list_companyName = []
	list_date = []
	
	list_operating_net_income = 6 * [""]
	list_operating_inventory = 6 * [""]
	list_operating_other_working_capital = 6 * [""]
	list_operating_change_in_working_capital = 6 * [""]
	list_operating_other_non_cash_items = 6 * [""]
	list_operating_loss_from_discontinued_operations = 6 * [""]
	list_operating_loss_from_disposision_of_businesses = 6 * [""]
	list_operating_depreciation_and_amortisation = 6 * [""]
	list_operating_amortisation_of_debt_discount_premium_and_issuance_cost = 6 * [""]
	list_operating_payables = 6 * [""]
	list_operating_receivable = 6 * [""]
	list_operating_accounts_payable = 6 * [""]
	list_operating_accounts_receivable = 6 * [""]
	list_operating_accrued_liabilities = 6 * [""]
	list_operating_cummulative_effect_of_accounting_change = 6 * [""]
	list_operating_prepaid_expenses = 6 * [""]
	list_operating_cash_paid_for_income_tax = 6 * [""]
	list_operating_deferred_income_taxes = 6 * [""]
	list_operating_deferred_tax_expense = 6 * [""]
	list_operating_excess_tax_benefit_from_stock_based_compensation = 6 * [""]
	list_operating_income_taxes_payable = 6 * [""]
	list_operating_interest_payable = 6 * [""]
	list_operating_effect_of_exchange_rate_changes = 6 * [""]
	list_operating_other_assets_and_liabilities = 6 * [""]
	list_operating_other_operating_activities = 6 * [""]
	list_operating_net_cash_provided_by_operating_activities = 6 * [""]
	
	list_investing_investments_in_property_plant_and_equipment = 6 * [""]
	list_investing_property_and_equipments_net = 6 * [""]
	list_investing_property_plant_and_equipment_reductions  = 6 * [""]
	list_investing_acquisitions_and_despositions = 6 * [""]
	list_investing_acquisitions_net = 6 * [""]
	list_investing_purchases_of_intangibles = 6 * [""]
	list_investing_sale_of_intangibles = 6 * [""]
	list_investing_purchases_of_investments = 6 * [""]
	list_investing_common_stock_repurchased = 6 * [""]
	list_investing_investment_asset_impairment_charges = 6 * [""]
	list_investing_investment_losses_gains = 6 * [""]
	list_investing_sale_maturities_of_investments = 6 * [""]
	list_investing_sale_maturities_of_fixed_maturity_and_equity_securities = 6 * [""]
	list_investing_other_investing_charges = 6 * [""]
	list_investing_other_investing_activities = 6 * [""]
	list_investing_net_cash_used_for_investing_activities = 6 * [""]
	
	list_financing_change_in_short_term_borrowing = 6 * [""]
	list_financing_loans = 6 * [""]
	list_financing_debt_issued = 6 * [""]
	list_financing_debt_repayment = 6 * [""]
	list_financing_long_term_debt_issued = 6 * [""]
	list_financing_long_term_debt_repayment = 6 * [""]
	list_financing_redemption_of_prefered_stock = 6 * [""]
	list_financing_repurchases_of_treasury_stock = 6 * [""]
	list_financing_cash_dividends_paid = 6 * [""]
	list_financing_cash_paid_for_interest = 6 * [""]
	list_financing_preferred_stock_issued = 6 * [""]
	list_financing_common_stock_issued = 6 * [""]
	list_financing_warrant_issued = 6 * [""]
	list_financing_stock_based_compensation = 6 * [""]
	list_financing_other_financing_activities = 6 * [""]
	list_financing_net_cash_used_for_financing_activities = 6 * [""]
	
	list_net_change_in_cash = 6 * [""]
	list_cash_at_beginning_of_period = 6 * [""]
	list_cash_at_end_of_period = 6 * [""]
	list_operating_cash_flow = 6 * [""]
	list_capital_expenditure = 6 * [""]
	list_free_cash_flow = 6 * [""]
	
	# Populate Collection Date field
	list_collection_date += 6 * [html_collectionDate]
	
	# Populate Sticker field 
	list_sticker += 6 * [symbol]
		
	# Populate Company Name field 
	list_companyName += 6 * [companyName]
	
	# Pull Date
	list_date = map(lambda x: "2017-12-01 00:00:00" if x == "TTM" else x + "-01 00:00:00", data[1][1:])
	
	for row in data[3:]:
		
		
		# Pull (Gain) Loss from discontinued operations
		if row[0] == "(Gain) Loss from discontinued operations" :
			list_operating_loss_from_discontinued_operations = map(lambda x: float(x) if x != "" else '', row[1:])
		
		# Pull (Gains) loss on disposition of businesses
		elif row[0] == "(Gains) loss on disposition of businesses":
			list_operating_loss_from_disposision_of_businesses = map(lambda x: float(x) if x != "" else '', row[1:])
		
		# Pull Accounts receivable		
		elif row[0] == "Accounts receivable":
			list_operating_accounts_receivable = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Accrued liabilities		
		elif row[0] == "Accrued liabilities":
			list_operating_accrued_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Acquisitions and dispositions		
		elif row[0] == "Acquisitions and dispositions":
			list_investing_acquisitions_and_despositions = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Acquisitions, net		
		elif row[0] == "Acquisitions, net":
			list_investing_acquisitions_net = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Amortization of debt discount/premium and issuance costs		
		elif row[0] == "Amortization of debt discount/premium and issuance costs":
			list_operating_amortisation_of_debt_discount_premium_and_issuance_cost = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Capital expenditure		
		elif row[0] == "Capital expenditure":
			list_capital_expenditure = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash at beginning of period		
		elif row[0] == "Cash at beginning of period":
			list_cash_at_beginning_of_period = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash at end of period		
		elif row[0] == "Cash at end of period":
			list_cash_at_end_of_period = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash dividends paid		
		elif row[0] == "Cash dividends paid" or row[0] == "Dividend paid":
			list_financing_cash_dividends_paid = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# Pull Cash paid for income taxes		
		elif row[0] == "Cash paid for income taxes":
			list_operating_cash_paid_for_income_tax = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cash paid for interest		
		elif row[0] == "Cash paid for interest":
			list_financing_cash_paid_for_interest = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Change in short-term borrowing		
		elif row[0] == "Change in short-term borrowing":
			list_financing_change_in_short_term_borrowing = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Change in working capital		
		elif row[0] == "Change in working capital":
			list_operating_change_in_working_capital = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other working capital		
		elif row[0] == "Other working capital":
			list_operating_other_working_capital = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Common stock issued		
		elif row[0] == "Common stock issued":
			list_financing_common_stock_issued = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Common stock repurchased		
		elif row[0] == "Common stock repurchased":
			list_investing_common_stock_repurchased = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Cumulative effect of accounting change		
		elif row[0] == "Cumulative effect of accounting change":
			list_operating_cummulative_effect_of_accounting_change = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Debt issued		
		elif row[0] == "Debt issued":
			list_financing_debt_issued = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Debt repayment		
		elif row[0] == "Debt repayment":
			list_financing_debt_repayment = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deferred income taxes		
		elif row[0] == "Deferred income taxes":
			list_operating_deferred_income_taxes = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Deferred tax (benefit) expense		
		elif row[0] == "Deferred tax (benefit) expense":
			list_operating_deferred_tax_expense = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Depreciation & amortization		
		elif row[0] == "Depreciation & amortization":
			list_operating_depreciation_and_amortisation = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Effect of exchange rate changes		
		elif row[0] == "Effect of exchange rate changes":
			list_operating_effect_of_exchange_rate_changes = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Excess tax benefit from stock based compensation		
		elif row[0] == "Excess tax benefit from stock based compensation":
			list_operating_excess_tax_benefit_from_stock_based_compensation = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Free cash flow		
		elif row[0] == "Free cash flow":
			list_free_cash_flow = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Income taxes payable		
		elif row[0] == "Income taxes payable":
			list_operating_income_taxes_payable = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Interest payable		
		elif row[0] == "Interest payable":
			list_operating_interest_payable = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Inventory		
		elif row[0] == "Inventory":
			list_operating_inventory = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Investment/asset impairment charges		
		elif row[0] == "Investment/asset impairment charges":
			list_investing_investment_asset_impairment_charges = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Investments losses (gains)		
		elif row[0] == "Investments losses (gains)" or row[0] == "Investments (gains) losses":
			list_investing_investment_losses_gains = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# Pull Investments in property, plant, and equipment		
		elif row[0] == "Investments in property, plant, and equipment":
			list_investing_investments_in_property_plant_and_equipment = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Property, and equipments, net		
		elif row[0] == "Property, and equipments, net":
			list_investing_property_and_equipments_net = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Property, plant, and equipment reductions		
		elif row[0] == "Property, plant, and equipment reductions":
			list_investing_property_plant_and_equipment_reductions = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Loans		
		elif row[0] == "Loans":
			list_financing_loans = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Long-term debt issued		
		elif row[0] == "Long-term debt issued":
			list_financing_long_term_debt_issued = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Long-term debt repayment		
		elif row[0] == "Long-term debt repayment":
			list_financing_long_term_debt_repayment = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net cash provided by (used for) financing activities		
		elif row[0] == "Net cash provided by (used for) financing activities":
			list_financing_net_cash_used_for_financing_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net cash provided by operating activities		
		elif row[0] == "Net cash provided by operating activities":
			list_operating_net_cash_provided_by_operating_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net cash used for investing activities		
		elif row[0] == "Net cash used for investing activities":
			list_investing_net_cash_used_for_investing_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net change in cash		
		elif row[0] == "Net change in cash":
			list_net_change_in_cash = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Net income		
		elif row[0] == "Net income":
			list_operating_net_income = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Operating cash flow		
		elif row[0] == "Operating cash flow":
			list_operating_cash_flow = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other assets and liabilities		
		elif row[0] == "Other assets and liabilities":
			list_operating_other_assets_and_liabilities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other financing activities		
		elif row[0] == "Other financing activities":
			list_financing_other_financing_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other investing activities		
		elif row[0] == "Other investing activities":
			list_investing_other_investing_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other investing charges		
		elif row[0] == "Other investing charges":
			list_investing_other_investing_charges = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other non-cash items		
		elif row[0] == "Other non-cash items":
			list_operating_other_non_cash_items = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Other operating activities		
		elif row[0] == "Other operating activities":
			list_operating_other_operating_activities = map(lambda x: float(x) if x != "" else '', row[1:])
			
			
		# Pull Payables		
		elif row[0] == "Payables":
			list_operating_payables = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Preferred stock issued		
		elif row[0] == "Preferred stock issued":
			list_financing_preferred_stock_issued = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Prepaid expenses		
		elif row[0] == "Prepaid expenses":
			list_operating_prepaid_expenses = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Purchases of intangibles		
		elif row[0] == "Purchases of intangibles":
			list_investing_purchases_of_intangibles = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Purchases of investments		
		elif row[0] == "Purchases of investments":
			list_investing_purchases_of_investments = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Receivable		
		elif row[0] == "Receivable":
			list_operating_receivable = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Redemption of preferred stock		
		elif row[0] == "Redemption of preferred stock":
			list_financing_redemption_of_prefered_stock = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Repurchases of treasury stock		
		elif row[0] == "Repurchases of treasury stock":
			list_financing_repurchases_of_treasury_stock = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Sales of intangibles		
		elif row[0] == "Sales of intangibles":
			list_investing_sale_of_intangibles = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Sales/maturities of fixed maturity and equity securities		
		elif row[0] == "Sales/maturities of fixed maturity and equity securities		":
			list_investing_sale_maturities_of_fixed_maturity_and_equity_securities = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Sales/Maturities of investments		
		elif row[0] == "Sales/Maturities of investments" or row[0] == "Sales/maturity of investments":
			list_investing_sale_maturities_of_investments = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Stock based compensation		
		elif row[0] == "Stock based compensation":
			list_financing_stock_based_compensation = map(lambda x: float(x) if x != "" else '', row[1:])
			
		# Pull Warrant issued		
		elif row[0] == "Warrant issued":
			list_financing_warrant_issued = map(lambda x: float(x) if x != "" else '', row[1:])
	
	
		

	# Create pandas dataframe from tuples
	companyCashFlow_df = pd.DataFrame({'collection_date':list_collection_date, 'sticker':list_sticker, 'companyName': list_companyName, 'date':list_date, 'operating_net_income':list_operating_net_income, 'operating_inventory':list_operating_inventory, 'operating_other_working_capital':list_operating_other_working_capital, 'operating_change_in_working_capital':list_operating_change_in_working_capital, 'operating_other_non_cash_items':list_operating_other_non_cash_items, 'operating_loss_from_discontinued_operations':list_operating_loss_from_discontinued_operations, 'operating_loss_from_disposision_of_businesses':list_operating_loss_from_disposision_of_businesses, 'operating_depreciation_and_amortisation':list_operating_depreciation_and_amortisation, 'operating_amortisation_of_debt_discount_premium_and_issuance_cost':list_operating_amortisation_of_debt_discount_premium_and_issuance_cost, 'operating_payables':list_operating_payables, 'operating_receivable':list_operating_receivable, 'operating_accounts_payable':list_operating_accounts_payable, 'operating_accounts_receivable':list_operating_accounts_receivable, 'operating_accrued_liabilities':list_operating_accrued_liabilities, 'operating_cummulative_effect_of_accounting_change':list_operating_cummulative_effect_of_accounting_change, 'operating_prepaid_expenses':list_operating_prepaid_expenses, 'operating_cash_paid_for_income_tax':list_operating_cash_paid_for_income_tax, 'operating_deferred_income_taxes':list_operating_deferred_income_taxes, 'operating_deferred_tax_expense':list_operating_deferred_tax_expense, 'operating_excess_tax_benefit_from_stock_based_compensation':list_operating_excess_tax_benefit_from_stock_based_compensation, 'operating_income_taxes_payable':list_operating_income_taxes_payable, 'operating_interest_payable':list_operating_interest_payable, 'operating_effect_of_exchange_rate_changes':list_operating_effect_of_exchange_rate_changes, 'operating_other_assets_and_liabilities':list_operating_other_assets_and_liabilities, 'operating_other_operating_activities':list_operating_other_operating_activities, 'operating_net_cash_provided_by_operating_activities':list_operating_net_cash_provided_by_operating_activities, 'investing_investments_in_property_plant_and_equipment':list_investing_investments_in_property_plant_and_equipment, 'investing_property_and_equipments_net':list_investing_property_and_equipments_net, 'investing_property_plant_and_equipment_reductions':list_investing_property_plant_and_equipment_reductions, 'investing_acquisitions_and_despositions':list_investing_acquisitions_and_despositions, 'investing_acquisitions_net':list_investing_acquisitions_net, 'investing_purchases_of_intangibles':list_investing_purchases_of_intangibles, 'investing_sale_of_intangibles':list_investing_sale_of_intangibles, 'investing_purchases_of_investments':list_investing_purchases_of_investments, 'investing_common_stock_repurchased':list_investing_common_stock_repurchased, 'investing_investment_asset_impairment_charges':list_investing_investment_asset_impairment_charges, 'investing_investment_losses_gains':list_investing_investment_losses_gains, 'investing_sale_maturities_of_investments':list_investing_sale_maturities_of_investments,'investing_other_investing_charges':list_investing_other_investing_charges, 'investing_sale_maturities_of_fixed_maturity_and_equity_securities':list_investing_sale_maturities_of_fixed_maturity_and_equity_securities, 'investing_other_investing_activities':list_investing_other_investing_activities, 'investing_net_cash_used_for_investing_activities':list_investing_net_cash_used_for_investing_activities, 'financing_change_in_short_term_borrowing':list_financing_change_in_short_term_borrowing, 'financing_loans':list_financing_loans, 'financing_debt_issued':list_financing_debt_issued, 'financing_debt_repayment':list_financing_debt_repayment, 'financing_long_term_debt_issued':list_financing_long_term_debt_issued, 'financing_long_term_debt_repayment':list_financing_long_term_debt_repayment, 'financing_redemption_of_prefered_stock':list_financing_redemption_of_prefered_stock, 'financing_repurchases_of_treasury_stock':list_financing_repurchases_of_treasury_stock, 'financing_cash_dividends_paid':list_financing_cash_dividends_paid, 'financing_cash_paid_for_interest':list_financing_cash_paid_for_interest, 'financing_cash_paid_for_interest':list_financing_cash_paid_for_interest, 'financing_preferred_stock_issued':list_financing_preferred_stock_issued, 'financing_common_stock_issued':list_financing_common_stock_issued, 'financing_warrant_issued':list_financing_warrant_issued, 'financing_stock_based_compensation':list_financing_stock_based_compensation, 'financing_other_financing_activities':list_financing_other_financing_activities, 'financing_net_cash_used_for_financing_activities':list_financing_net_cash_used_for_financing_activities, 'net_change_in_cash':list_net_change_in_cash, 'cash_at_beginning_of_period':list_cash_at_beginning_of_period, 'cash_at_end_of_period':list_cash_at_end_of_period, 'operating_cash_flow':list_operating_cash_flow, 'capital_expenditure':list_capital_expenditure, 'free_cash_flow':list_free_cash_flow})

	companyCashFlow_df = companyCashFlow_df[['collection_date', 'sticker', 'companyName', 'date', 'operating_net_income', 'operating_inventory', 'operating_other_working_capital', 'operating_change_in_working_capital', 'operating_other_non_cash_items', 'operating_loss_from_discontinued_operations', 'operating_loss_from_disposision_of_businesses', 'operating_depreciation_and_amortisation', 'operating_amortisation_of_debt_discount_premium_and_issuance_cost', 'operating_payables', 'operating_receivable', 'operating_accounts_payable', 'operating_accounts_receivable', 'operating_accrued_liabilities', 'operating_cummulative_effect_of_accounting_change', 'operating_prepaid_expenses', 'operating_cash_paid_for_income_tax', 'operating_deferred_income_taxes', 'operating_deferred_tax_expense', 'operating_excess_tax_benefit_from_stock_based_compensation', 'operating_income_taxes_payable', 'operating_interest_payable', 'operating_effect_of_exchange_rate_changes', 'operating_other_assets_and_liabilities', 'operating_other_operating_activities', 'operating_net_cash_provided_by_operating_activities', 'investing_investments_in_property_plant_and_equipment', 'investing_property_and_equipments_net', 'investing_property_plant_and_equipment_reductions', 'investing_acquisitions_and_despositions', 'investing_acquisitions_net', 'investing_purchases_of_intangibles', 'investing_sale_of_intangibles', 'investing_purchases_of_investments', 'investing_common_stock_repurchased', 'investing_investment_asset_impairment_charges', 'investing_investment_losses_gains', 'investing_sale_maturities_of_investments','investing_other_investing_charges', 'investing_sale_maturities_of_fixed_maturity_and_equity_securities', 'investing_other_investing_activities', 'investing_net_cash_used_for_investing_activities', 'financing_change_in_short_term_borrowing', 'financing_loans', 'financing_debt_issued', 'financing_debt_repayment', 'financing_long_term_debt_issued', 'financing_long_term_debt_repayment', 'financing_redemption_of_prefered_stock', 'financing_repurchases_of_treasury_stock', 'financing_cash_dividends_paid', 'financing_cash_paid_for_interest', 'financing_cash_paid_for_interest', 'financing_preferred_stock_issued', 'financing_common_stock_issued', 'financing_warrant_issued', 'financing_stock_based_compensation', 'financing_other_financing_activities', 'financing_net_cash_used_for_financing_activities', 'net_change_in_cash', 'cash_at_beginning_of_period', 'cash_at_end_of_period', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow']]
	
	
	return companyCashFlow_df	
	

	

def main():

  # Entry job message
  print "\n Process of parsing Financial Statements started.." 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  html_collectionDate = datetime.datetime.today().strftime('%Y%m%d')
  #html_collectionDate = "2018-02-02 00:00:00"
  #html_collectionDate_filename = "20180202"
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_bucket = "athens_stock_exchange"
  companyIncomeStatement_csv_output = "company_Income_Statement_" + html_collectionDate + ".csv"
  companyBalanceSheet_csv_output = "company_Balance_Sheet_" + html_collectionDate + ".csv"
  companyCashFlow_csv_output = "company_Cash_Flow_" + html_collectionDate + ".csv"
  start_html_process = time.time()


  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  # Test script only for one symbol
  #company_symbol_list = ["FFGRP", "AEGN"]
  #company_name_list =  ["Folli Follie", "AEGEAN"]
  
  # Pull information from Morning Star and store in server
  df_Income_Statement, df_Balance_Sheet, df_Cash_Flow = pull_MorningStar_data(company_symbol_list, company_name_list, html_collectionDate)
  
  # Store information in Cloud Storage
  store_information_in_CloudStorage(df_Income_Statement, companyInformation_bucket, companyIncomeStatement_csv_output)
  store_information_in_CloudStorage(df_Balance_Sheet, companyInformation_bucket, companyBalanceSheet_csv_output)
  store_information_in_CloudStorage(df_Cash_Flow, companyInformation_bucket, companyCashFlow_csv_output)
  

  # Process end message
  print "\nProcessed completed succesfully after " + str(time.time()-start_html_process) + ' seconds.'


if __name__ == '__main__':
  main()
