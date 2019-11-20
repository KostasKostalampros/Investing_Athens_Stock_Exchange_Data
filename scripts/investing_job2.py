from google.cloud import storage, bigquery
from google.cloud.storage import Blob
from apiclient.discovery import build

from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials

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


def get_company_information_from_html(company_symbols, html_collectionDate):
	"""Parse HTML files and pulls company information"""

	list_website = []
	list_address = []
	list_sector = []
	list_industry = []
	list_businessDescription = []
	list_employees = []

	start_html_process = time.time()

	for symbol in company_symbols:

		start_html_process_each = time.time()

		htmlFilePath = "/home/Investing/athens_stock_exchange/datasources/" + symbol + "_" + html_collectionDate +".html"

		soup = BeautifulSoup(open(htmlFilePath), 'html.parser')

		business_description = soup.find("p", {"class":"r_txt6"}).getText().lstrip().rstrip().encode('utf-8').strip()

		r_table1 = soup.findAll("tr", {"class":"text3 padding_b_24px"})[1].findAll("td")
		sector = str(r_table1[2]).replace('<td>', '').replace('</td>', '').strip()
		industry = str(r_table1[4]).replace('<td>', '').replace('</td>', '').strip()

		r_table1_r_txt2 = soup.findAll("th", {"class":"row_lbl"})
		company_address = str(r_table1_r_txt2[0].findNextSiblings('td')[0]).replace('<td>', '').replace('</td>', '').strip()
		website = r_table1_r_txt2[4].findNextSiblings('td')[0].find('a', href=True)["href"].encode('utf-8').strip()

		number_of_employees = str(r_table1_r_txt2[11].findNextSiblings('td')[0]).replace('<td>', '').replace('</td>', '').strip()

		list_website.append(website)
		list_address.append(company_address)
		list_sector.append(sector)
		list_industry.append(industry)
		list_businessDescription.append(business_description)
		list_employees.append(number_of_employees)

	print "\nProcessed all html files in " + str(time.time()-start_html_process) + ' seconds.'


	return list_website, list_address, list_sector, list_industry, list_businessDescription, list_employees


def store_information_in_CloudStorage(companyInformation_df, html_collectionDate, companyInformation_bucket, companyInformation_csv_output):

  companyInformation_vm_filepath = "/home/Investing/athens_stock_exchange/company_information/companyInformation_" + html_collectionDate + ".csv"

  companyInformation_df.to_csv(companyInformation_vm_filepath, index=False)

  """Downloads a blob from the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(companyInformation_bucket)
  blob = bucket.blob(companyInformation_csv_output)

  blob.upload_from_filename(companyInformation_vm_filepath)


def main():

  # Entry job message
  print "\n Process of parsing company_informattion html files started.." 

  # Update the filename of the inBroker.com xlsx file
  inBrokerFilename = "companyInformation_inBroker.com_20171006.xlsx"

  # Set up project variables
  key_file_location = "Investing-654e951a61fa.json"
  html_collectionDate = "20171103"
  companyInformation_bucket = "athens_stock_exchange"
  companyInformation_xlsx_file = "datasources/companyInformation_inBroker.com/" + inBrokerFilename
  companyInformation_xlsx_vmPath = "/home/Investing/athens_stock_exchange/datasources/" + inBrokerFilename
  companyInformation_csv_output = "company_information/companyInformation_" + html_collectionDate + ".csv"

  dataset_name = "athens_stock_exchange"
  table_name = "companyInformation_" + html_collectionDate

  # Create list of dates with as many values as the number of symbols
  list_date = []
  list_date += 205 * [html_collectionDate]

  # Pull list of company symbols and company names
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)

  # Pull list of companys' website, address, sector, industry, business description, and number of employees
  list_website, list_address, list_sector, list_industry, list_businessDescription, list_employees = get_company_information_from_html(company_symbol_list, html_collectionDate)

  # Create pandas dataframe from tuples
  companyInformation_df = pd.DataFrame({'date':list_date, 'sticker':company_symbol_list, 'companyName': company_name_list, 'website':list_website, 'address':list_address,
  'sector':list_sector, 'industry':list_industry, 'businessDescription':list_businessDescription, 'employees':list_employees})

  companyInformation_df = companyInformation_df[['date', 'sticker', 'companyName', 'website', 'address', 'sector',
  'industry', 'businessDescription', 'employees']]

  # Transform dataframe to csv and store it in Cloud Storage
  store_information_in_CloudStorage(companyInformation_df, html_collectionDate, companyInformation_bucket, companyInformation_csv_output)

  # Process end message
  print "Process of parsing html files completed succesfully!"


if __name__ == '__main__':
  main()
