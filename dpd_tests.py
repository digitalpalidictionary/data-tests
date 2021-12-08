#!/usr/bin/env python3.10
# coding: utf-8

import logging
import re
import pandas as pd
from pandas_ods_reader import read_ods
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", 'This pattern has match groups')

#setup time
now = datetime.now()
now = now.strftime("%Y/%m/%d %H:%M:%S")


#setup logger
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%I:%M:%S')

#convert ods to csv
from ods_to_csv import convert_dpd_ods_to_csv
yn = (input(f"convert ods to csv? "))
if yn == "y":
	convert_dpd_ods_to_csv()
if yn == "n":
	pass

#import dpd csv
logging.warning(f"opening dpd.csv as data frame")
csv_file = "/home/bhikkhu/Bodhirasa/Dropbox/dpd/dpd.csv"
pali_df = pd.read_csv (csv_file, sep="\t", dtype=str, skip_blank_lines=False)
pali_df.fillna("", inplace=True)

#import tests
logging.warning(f"opening tests.ods as data frame")
tests_csv_file = "/home/bhikkhu/Bodhirasa/Dropbox/dpd/dpd_data_tests/tests.csv"
sheet_index = 1
tests_df = pd.read_csv (tests_csv_file, sep="\t", dtype=str, skip_blank_lines=False)
tests_df.fillna("", inplace=True)

#open test_results.txt
logging.warning ("opening test_results.txt")
txt_file = open ("test_results.txt", 'w', encoding= "'utf-8")

test_column_count = tests_df.shape[0]

#define variables
logging.warning ("defining variables")

#print to text file
# logging.warning (f"{row}. {search_name}")
with open("test_results.txt", 'a') as txt_file:
	line_break = "~" * 40
	txt_file.write (f"DPD tests {now}\n")
	txt_file.write (f"{line_break}\n")
	txt_file.write (f"Notez in Anki\n")
	txt_file.write (f"Notez in Google Sheets\n")
	txt_file.write (f"Notez on page\n")

for row in range (0, test_column_count):
	search_name = (tests_df.iloc[row, 0])

	if search_name == (""):
		continue
	elif re.findall("^\!", search_name):
		continue

	search_column1 = (tests_df.iloc[row, 1])
	search_sign1 = (tests_df.iloc[row, 2])
	search_string1 = (tests_df.iloc[row, 3])

	search_column2 = (tests_df.iloc[row, 4])
	search_sign2 = (tests_df.iloc[row, 5])
	search_string2 = (tests_df.iloc[row, 6])

	search_column3 = (tests_df.iloc[row, 7])
	search_sign3 = (tests_df.iloc[row, 8])
	search_string3 = (tests_df.iloc[row, 9])

	search_column4 = (tests_df.iloc[row, 10])
	search_sign4 = (tests_df.iloc[row, 11])
	search_string4 = (tests_df.iloc[row, 12])

	search_column5 = (tests_df.iloc[row, 13])
	search_sign5 = (tests_df.iloc[row, 14])
	search_string5 = (tests_df.iloc[row, 15])

	search_column6 = (tests_df.iloc[row, 16])
	search_sign6 = (tests_df.iloc[row, 17])
	search_string6 = (tests_df.iloc[row, 18])

	print_column1 = (tests_df.iloc[row, 19])
	print_column2 = (tests_df.iloc[row, 20])
	print_column3 = (tests_df.iloc[row, 21])

	exceptions = (tests_df.iloc[row, 22])
	iterations = int(tests_df.iloc[row, 23])

	if exceptions == "":
		test_exceptions = pali_df["Pāli1"].str.contains(".+")
	if exceptions != "":
		test_exceptions = pali_df["Pāli1"].str.contains(exceptions) == False
	
	#search1
	if search_sign1 == "equals":
		test1 = pali_df[search_column1] == (search_string1)
	elif search_sign1 == "does not equal":
		test1 = pali_df[search_column1] != (search_string1)
	elif search_sign1 == "contains":
		test1 = pali_df[search_column1].str.contains(search_string1)
	elif search_sign1 == "does not contain":
		test1 = pali_df[search_column1].str.contains(search_string1) == False
	elif search_sign1 == "contains word":
		test1 = pali_df[search_column1].str.contains(fr"\b{search_string1}\b")
	elif search_sign1 == "does not contain word":
		test1 = pali_df[search_column1].str.contains(fr"\b{search_string1}\b") == False
	elif search_sign1 == "is empty":
		test1 = pali_df[search_column1] == ("")
	elif search_sign1 == "is not empty":
		test1 = pali_df[search_column1].str.contains(".+") == True
	elif search_sign1 == "":
		test1 = pali_df["Pāli1"].str.contains(".+")
	else:
		print(f"search1 error")

	#search2
	if search_sign2 == "equals":
		test2 = pali_df[search_column2] == (search_string2)
	elif search_sign2 == "does not equal":
		test2 = pali_df[search_column2] != (search_string2)
	elif search_sign2 == "contains":
		test2 = pali_df[search_column2].str.contains(search_string2)
	elif search_sign2 == "does not contain":
		test2 = pali_df[search_column2].str.contains(search_string2) == False
	elif search_sign2 == "contains word":
		test2 = pali_df[search_column2].str.contains(fr"\b{search_string2}\b")
	elif search_sign2 == "does not contain word":
		test2 = pali_df[search_column2].str.contains(fr"\b{search_string2}\b") == False
	elif search_sign2 == "is empty":
		test2 = pali_df[search_column2] == ("")
	elif search_sign2 == "is not empty":
		test2 = pali_df[search_column2].str.contains(".+") == True
	elif search_sign2 == "":
		test2 = pali_df["Pāli2"].str.contains(".+")
	else:
		print("search2 error")

	#search3
	if search_sign3 == "equals":
		test3 = pali_df[search_column3] == (search_string3)
	elif search_sign3 == "does not equal":
		test3 = pali_df[search_column3] != (search_string3)
	elif search_sign3 == "contains":
		test3 = pali_df[search_column3].str.contains(search_string3)
	elif search_sign3 == "does not contain":
		test3 = pali_df[search_column3].str.contains(search_string3) == False
	elif search_sign3 == "contains word":
		test3 = pali_df[search_column3].str.contains(fr"\b{search_string3}\b")
	elif search_sign3 == "does not contain word":
		test3 = pali_df[search_column3].str.contains(fr"\b{search_string3}\b") == False
	elif search_sign3 == "is empty":
		test3 = pali_df[search_column3] == ("")
	elif search_sign3 == "is not empty":
		test3 = pali_df[search_column3].str.contains(".+") == True
	elif search_sign3 == "":
		test3 = pali_df["Pāli1"].str.contains(".+")
	else:
		print("search3 error")

	#search4
	if search_sign4 == "equals":
		test4 = pali_df[search_column4] == (search_string4)
	elif search_sign4 == "does not equal":
		test4 = pali_df[search_column4] != (search_string4)
	elif search_sign4 == "contains":
		test4 = pali_df[search_column4].str.contains(search_string4)
	elif search_sign4 == "does not contain":
		test4 = pali_df[search_column4].str.contains(search_string4) == False
	elif search_sign4 == "contains word":
		test4 = pali_df[search_column4].str.contains(fr"\b{search_string4}\b")
	elif search_sign4 == "does not contain word":
		test4 = pali_df[search_column4].str.contains(fr"\b{search_string4}\b") == False
	elif search_sign4 == "is empty":
		test4 = pali_df[search_column4] == ("")
	elif search_sign4 == "is not empty":
		test4 = pali_df[search_column4].str.contains(".+") == True
	elif search_sign4 == "":
		test4 = pali_df["Pāli1"].str.contains(".+")
	else:
		print("search4 error")

	#search5
	if search_sign5 == "equals":
		test5 = pali_df[search_column5] == (search_string5)
	elif search_sign5 == "does not equal":
		test5 = pali_df[search_column5] != (search_string5)
	elif search_sign5 == "contains":
		test5 = pali_df[search_column5].str.contains(search_string5)
	elif search_sign5 == "does not contain":
		test5 = pali_df[search_column5].str.contains(search_string5) == False
	elif search_sign5 == "contains word":
		test5 = pali_df[search_column5].str.contains(fr"\b{search_string5}\b")
	elif search_sign5 == "does not contain word":
		test5 = pali_df[search_column5].str.contains(fr"\b{search_string5}\b") == False
	elif search_sign5 == "is empty":
		test5 = pali_df[search_column5] == ("")
	elif search_sign5 == "is not empty":
		test5 = pali_df[search_column5].str.contains(".+") == True
	elif search_sign5 == "":
		test5 = pali_df["Pāli1"].str.contains(".+")
	else:
		print("search5 error")

	#search6
	if search_sign6 == "equals":
		test6 = pali_df[search_column6] == (search_string6)
	elif search_sign6 == "does not equal":
		test6 = pali_df[search_column6] != (search_string6)
	elif search_sign6 == "contains":
		test6 = pali_df[search_column6].str.contains(search_string6)
	elif search_sign6 == "does not contain":
		test6 = pali_df[search_column6].str.contains(search_string6) == False
	elif search_sign6 == "contains word":
		test6 = pali_df[search_column6].str.contains(fr"\b{search_string6}\b")
	elif search_sign6 == "does not contain word":
		test6 = pali_df[search_column6].str.contains(fr"\b{search_string6}\b") == False
	elif search_sign6 == "is empty":
		test6 = pali_df[search_column6] == ("")
	elif search_sign6 == "is not empty":
		test6 = pali_df[search_column6].str.contains(".+") == True
	elif search_sign6 == "":
		test6 = pali_df["Pāli1"].str.contains(".+")
	else:
		print("search6 error")

	filter = test_exceptions & test1 & test2 & test3 & test4 & test5 & test6

	filtered_df = pali_df.loc[filter, [print_column1, print_column2, print_column3]]

	column_count2 = filtered_df.shape[0]
	filtered_df = filtered_df.head(iterations)
	column_count1 = filtered_df.shape[0]

	#print to text file
	logging.warning (f"{row}. {search_name}")

	if column_count1> 0:
		with open("test_results.txt", 'a') as txt_file:
			txt_file.write (f"{line_break}\n")
			txt_file.write (f"{row}. {search_name} ({column_count1} of {column_count2})\n")
			txt_file.write (f"{line_break}\n")
			filtered_df.to_csv(txt_file, header=False, index=False, sep="\t")
			# txt_file.write (f"\n")

with open("test_results.txt", 'a') as txt_file:
	headings = list(pali_df.columns.values)
	txt_file.write (f"\n")
	txt_file.write (f"{line_break}\n")
	txt_file.write (f"{headings}")

# report errors on wrong fields
# make a dataframe of the list names and count as you go along 