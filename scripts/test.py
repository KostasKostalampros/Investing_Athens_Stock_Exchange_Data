import urllib2
import csv
import pandas as pd
import os

url_income_statement = "http://financials.morningstar.com/ajax/ReportProcess4CSV.html?t=" + "FFGRP" + "&region=grc&reportType=is&period=12&dataType=A&order=asc&columnYear=5&number=3"

html_response = urllib2.urlopen(url_income_statement)
csv_reader = csv.reader(html_response, delimiter=',')
data = list(csv_reader)
print len(data)
for row in data:
	print row

print "************************"

html_response = urllib2.urlopen(url_income_statement)
csv_reader = csv.reader(html_response, delimiter=',')
j=0
for row in csv_reader:
	print row
	j+=1
print j
