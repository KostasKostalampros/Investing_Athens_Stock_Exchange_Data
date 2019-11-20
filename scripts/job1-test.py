from google.cloud import storage
from google.cloud.storage import Blob

from apiclient.discovery import build

from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials

from openpyxl import load_workbook
import urllib2
import datetime

from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
  
  
def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob downloaded to {}.'.format(
        destination_file_name))  
  

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
	
	
def download_html_company_information(company_symbols, bucket_name):
	"""Downloads HTML files with company information and stores localy in the VM and upload in Cloud Storage"""
	
	client = storage.Client()
	bucket = client.get_bucket(bucket_name)
	
	# Set the Display for the VM to download the HTML COde
	display = Display(visible=0, size=(800, 600))
        display.start()
	
	print "Downloading the Company Information HTML files for: " + str(len(company_symbols)) + " symbols"
	
	company_symbols = ["AXON"]
	
	i=1
	for symbol in company_symbols:
		
		print "Downloading symbol " + str(i) + ": " + str(symbol) + " from the list"
		
		# Download locally in a vm specified folder
		url = "http://financials.morningstar.com/company-profile/c.action?t=" + symbol + "&region=GRC&culture=en_US"
		#htmlFile = urllib2.urlopen(url)
		htmlFilePath = "/home/project_investing/datasources/html_" + symbol + "_" + datetime.datetime.today().strftime('%Y%m%d') +".html"
		
		driver = webdriver.Chrome()
		#options = webdriver.ChromeOptions()
		#options.binary_location = '/opt/google/chrome/google-chrome'
		#service_log_path = "{}/chromedriver.log".format("/home/project_investing")
		#service_args = ['--verbose']
		#driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options, service_args=service_args, service_log_path=service_log_path)
		
		driver.get(url)

		#waiting for the page to load - TODO: change
		#wait = WebDriverWait(driver, 10)
		#wait.until(EC.visibility_of_element_located((By.ID, "content")))

		data = driver.page_source
		
		with open(htmlFilePath,'wb') as output:
			output.write(data)
			
		driver.close()	
			
			
		# Upload to Cloud Storage
		blob_fileName = "datasources/html_files/html_" + symbol + "_" + datetime.datetime.today().strftime('%Y%m%d') + ".html"		
		blob = Blob(blob_fileName, bucket)
		with open(htmlFilePath, 'rb') as input:
			blob.upload_from_file(input)
		
		i+=1
	
	print "All HTML files have been succesfully stored in Cloud Storage in the following path: gs://athens_stock_exchange/datasources/html_files/"
	
	

def get_company_information_from_html(company_symbols):
	"""Parse HTML files and pulls company information"""
	
	list_website = []
	list_address = []
	list_sector = []
	list_industry = []
	list_businessDescription = []
	list_employees = []
	
	company_symbols = ["AXON"]
	
	for symbol in company_symbols:
		
		htmlFilePath = "/home/project_investing/datasources/html_" + symbol + "_" + datetime.datetime.today().strftime('%Y%m%d') +".html"
		soup = BeautifulSoup(htmlFilePath, 'html.parser')

	

	return list_website, list_address, list_sector, list_industry, list_businessDescription, list_employees




	
    
def main():

  # Job definitions
  companyInformation_bucket = "athens_stock_exchange"
  companyInformation_xlsx_file = "datasources/companyInformation_inBroker.com/companyInformation_inBroker.com_20171006.xlsx"
  companyInformation_xlsx_vmPath = "/home/project_investing/datasources/companyInformation_inBroker.com_20171006.xlsx"

  #download_blob(companyInformation_bucket, companyInformation_xlsx_file, companyInformation_xlsx_vmPath)
  
  company_symbol_list, company_name_list = read_company_information_from_xlsx(companyInformation_xlsx_vmPath)
  
  download_html_company_information(company_symbol_list, companyInformation_bucket)
  


if __name__ == '__main__':
  main()  
