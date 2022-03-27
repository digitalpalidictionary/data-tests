#!/usr/bin/env python3
# coding: utf-8

import re
import pandas as pd
from pandas_ods_reader import *
from pandas.io.parsers import read_csv
from pandas_ods_reader import read_ods
from datetime import datetime
import warnings
import os
import time
import stat
import pickle

warnings.filterwarnings("ignore", 'This pattern has match groups')
warnings.simplefilter(action='ignore', category=FutureWarning)


now = datetime.now()
time_now = now.strftime("%Y/%m/%d %H:%M:%S")
date = now.strftime("%d")

line_break = "~" * 40

def timeis():
	global blue
	global yellow
	global green
	global red
	global white

	blue = "\033[38;5;33m" #blue
	green = "\033[38;5;34m" #green
	red= "\033[38;5;160m" #red
	yellow = "\033[38;5;220m" #yellow
	white = "\033[38;5;251m" #white
	now = datetime.now()
	current_time = now.strftime("%Y-%m-%d %H:%M:%S")
	return (f"{blue}{current_time}{white}")

def make_new_dpd_csv():
	global yn
	print(f"{timeis()} {line_break}")
	print(f"{timeis()} {yellow}dpd data integrity tests")
	print(f"{timeis()} {line_break}")

	dpd_csv_file = "../csvs/dpd.csv"
	dpd_csv_stats = os.stat ( dpd_csv_file )
	dpd_csv_mod_time = time.ctime ( dpd_csv_stats [stat.ST_MTIME ] )

	print(f"{timeis()} {green}dpd.csv last modified on \t\t{blue}{dpd_csv_mod_time}{white}") 
	yn = (input(f"{timeis()} convert ods to dpd.csv?(y/n)\t"))

	if yn == "y":
		print(f"{timeis()} {green}reading ods_file")
		ods_file = "../dpd.ods"
		csv_file = "../csvs/dpd.csv"
		sheet_index = 1
		df = read_ods (ods_file, sheet_index, headers = False)

		print(f"{timeis()} {green}cleaning up dataframe")
		df = df.drop(index=0) #removing first row
		new_header = df.iloc[0] #grab the first row for the header
		df = df[1:] #take the data less the header row
		df.columns = new_header #set the header row as the df header
		df.dropna(subset = ["Pāli1", "Date"], inplace = True) #remove messed up rows

		print(f"{timeis()} {green}writing data frame to {csv_file}")
		df.to_csv(csv_file, index = False, sep = "\t", encoding="utf-8")


def setup_dfs():
	print(f"{timeis()} {green}setting up df's")
	global dpd_df
	global dpd_df_length
	global tests_df
	global test_column_count
	global line

	dpd_df = pd.read_csv ("../csvs/dpd.csv", sep="\t", dtype=str, skip_blank_lines=False)
	dpd_df.fillna("", inplace=True)
	dpd_df_length = len(dpd_df)

	tests_df = pd.read_csv ("tests.csv", sep="\t", dtype=str, skip_blank_lines=False)
	tests_df.fillna("", inplace=True)

	test_column_count = tests_df.shape[0]

def tests_data_integrity_tests():
	print(f"{timeis()} {green}running data integrity tests")
	dpd_df_column_names = list(dpd_df.columns)
	dpd_df_column_names.append("")

	for row in range (0, test_column_count):
		line = row + 2
		search_name = (tests_df.iloc[row, 0])

		if search_name == (""):
			continue
		elif re.findall("^\!", search_name):
			continue

		search_column1 = (tests_df.iloc[row, 1])
		search_column2 = (tests_df.iloc[row, 4])
		search_column3 = (tests_df.iloc[row, 7])
		search_column4 = (tests_df.iloc[row, 10])
		search_column5 = (tests_df.iloc[row, 13])
		search_column6 = (tests_df.iloc[row, 16])
		print_column1 = (tests_df.iloc[row, 19])
		print_column2 = (tests_df.iloc[row, 20])
		print_column3 = (tests_df.iloc[row, 21])

		if search_column1 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 1 *{search_column1}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 1 *{search_column1}* does not exist")
		if search_column2 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 2 *{search_column2}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 2 *{search_column2}* does not exist")
		if search_column3 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 3 *{search_column3}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 3 *{search_column3}* does not exist")
		if search_column4 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 4 *{search_column4}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 4 *{search_column4}* does not exist")
		if search_column5 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 5 *{search_column5}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 5 *{search_column5}* does not exist")
		if search_column6 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} search column 6 *{search_column6}* does not exist")
			# txt_file.write (f"{line}. {search_name} search column 6 *{search_column6}* does not exist")
		if print_column1 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} print column 1 *{print_column1}* does not exist")
			# txt_file.write (f"{line}. {search_name} print column 1 *{print_column1}* does not exist")
		if print_column2 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} print column 2 *{print_column2}* does not exist")
			# txt_file.write (f"{line}. {search_name} print column 2 *{print_column2}* does not exist")
		if print_column3 not in dpd_df_column_names:
			print (f"{timeis()}{red}{line}. {search_name} print column 3 *{print_column3}* does not exist")
			# txt_file.write (f"{line}. {search_name} print column 3 *{print_column3}* does not exist")

		search_sign1 = (tests_df.iloc[row, 2])
		search_sign2 = (tests_df.iloc[row, 5])
		search_sign3 = (tests_df.iloc[row, 8])
		search_sign4 = (tests_df.iloc[row, 11])
		search_sign5 = (tests_df.iloc[row, 14])
		search_sign6 = (tests_df.iloc[row, 17])

		if search_sign1 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign1 *{search_sign1}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign1 *{search_sign1}* does not exist")

		if search_sign2 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign2 *{search_sign2}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign2 *{search_sign2}* does not exist")

		if search_sign3 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign3 *{search_sign3}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign3 *{search_sign3}* does not exist")

		if search_sign4 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign4 *{search_sign4}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign4 *{search_sign4}* does not exist")

		if search_sign5 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign5 *{search_sign5}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign5 *{search_sign5}* does not exist")

		if search_sign6 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red}{line}. {search_name} search_sign6 *{search_sign6}* does not exist")
			# txt_file.write (f"{line}. {search_name} search_sign6 *{search_sign6}* does not exist")

		line += 1

def generate_test_results():
	print(f"{timeis()} {green}generating test results")
	txt_file = open ("test_results.txt", 'w', encoding= "'utf-8")
	txt_file2 = open ("test_results_all.txt", 'w', encoding= "'utf-8")

	with open("test_results.txt", 'a') as txt_file:
		txt_file.write (f"DPD tests {time_now}\n")
		txt_file.write (f"{line_break}\n")
		txt_file.write (f"Notez in Anki\n")
		txt_file.write (f"Notez in Google Sheets\n")
		txt_file.write (f"Notez in Notebook\n")

	# define variables
	for row in range (0, test_column_count):
		line = row + 2
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
			test_exceptions = dpd_df["Pāli1"].str.contains(".+")
		if exceptions != "":
			test_exceptions = dpd_df["Pāli1"].str.contains(exceptions) == False
		
		# search1
		if search_sign1 == "equals":
			test1 = dpd_df[search_column1] == (search_string1)
		elif search_sign1 == "does not equal":
			test1 = dpd_df[search_column1] != (search_string1)
		elif search_sign1 == "contains":
			test1 = dpd_df[search_column1].str.contains(search_string1)
		elif search_sign1 == "does not contain":
			test1 = dpd_df[search_column1].str.contains(search_string1) == False
		elif search_sign1 == "contains word":
			test1 = dpd_df[search_column1].str.contains(fr"\b{search_string1}\b")
		elif search_sign1 == "does not contain word":
			test1 = dpd_df[search_column1].str.contains(fr"\b{search_string1}\b") == False
		elif search_sign1 == "is empty":
			test1 = dpd_df[search_column1] == ("")
		elif search_sign1 == "is not empty":
			test1 = dpd_df[search_column1].str.contains(".+") == True
		elif search_sign1 == "":
			test1 = dpd_df["Pāli1"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search1 error")

		# search2
		if search_sign2 == "equals":
			test2 = dpd_df[search_column2] == (search_string2)
		elif search_sign2 == "does not equal":
			test2 = dpd_df[search_column2] != (search_string2)
		elif search_sign2 == "contains":
			test2 = dpd_df[search_column2].str.contains(search_string2)
		elif search_sign2 == "does not contain":
			test2 = dpd_df[search_column2].str.contains(search_string2) == False
		elif search_sign2 == "contains word":
			test2 = dpd_df[search_column2].str.contains(fr"\b{search_string2}\b")
		elif search_sign2 == "does not contain word":
			test2 = dpd_df[search_column2].str.contains(fr"\b{search_string2}\b") == False
		elif search_sign2 == "is empty":
			test2 = dpd_df[search_column2] == ("")
		elif search_sign2 == "is not empty":
			test2 = dpd_df[search_column2].str.contains(".+") == True
		elif search_sign2 == "":
			test2 = dpd_df["Pāli2"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search2 error")

		# search3
		if search_sign3 == "equals":
			test3 = dpd_df[search_column3] == (search_string3)
		elif search_sign3 == "does not equal":
			test3 = dpd_df[search_column3] != (search_string3)
		elif search_sign3 == "contains":
			test3 = dpd_df[search_column3].str.contains(search_string3)
		elif search_sign3 == "does not contain":
			test3 = dpd_df[search_column3].str.contains(search_string3) == False
		elif search_sign3 == "contains word":
			test3 = dpd_df[search_column3].str.contains(fr"\b{search_string3}\b")
		elif search_sign3 == "does not contain word":
			test3 = dpd_df[search_column3].str.contains(fr"\b{search_string3}\b") == False
		elif search_sign3 == "is empty":
			test3 = dpd_df[search_column3] == ("")
		elif search_sign3 == "is not empty":
			test3 = dpd_df[search_column3].str.contains(".+") == True
		elif search_sign3 == "":
			test3 = dpd_df["Pāli1"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search3 error")

		# search4
		if search_sign4 == "equals":
			test4 = dpd_df[search_column4] == (search_string4)
		elif search_sign4 == "does not equal":
			test4 = dpd_df[search_column4] != (search_string4)
		elif search_sign4 == "contains":
			test4 = dpd_df[search_column4].str.contains(search_string4)
		elif search_sign4 == "does not contain":
			test4 = dpd_df[search_column4].str.contains(search_string4) == False
		elif search_sign4 == "contains word":
			test4 = dpd_df[search_column4].str.contains(fr"\b{search_string4}\b")
		elif search_sign4 == "does not contain word":
			test4 = dpd_df[search_column4].str.contains(fr"\b{search_string4}\b") == False
		elif search_sign4 == "is empty":
			test4 = dpd_df[search_column4] == ("")
		elif search_sign4 == "is not empty":
			test4 = dpd_df[search_column4].str.contains(".+") == True
		elif search_sign4 == "":
			test4 = dpd_df["Pāli1"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search4 error")

		# search5
		if search_sign5 == "equals":
			test5 = dpd_df[search_column5] == (search_string5)
		elif search_sign5 == "does not equal":
			test5 = dpd_df[search_column5] != (search_string5)
		elif search_sign5 == "contains":
			test5 = dpd_df[search_column5].str.contains(search_string5)
		elif search_sign5 == "does not contain":
			test5 = dpd_df[search_column5].str.contains(search_string5) == False
		elif search_sign5 == "contains word":
			test5 = dpd_df[search_column5].str.contains(fr"\b{search_string5}\b")
		elif search_sign5 == "does not contain word":
			test5 = dpd_df[search_column5].str.contains(fr"\b{search_string5}\b") == False
		elif search_sign5 == "is empty":
			test5 = dpd_df[search_column5] == ("")
		elif search_sign5 == "is not empty":
			test5 = dpd_df[search_column5].str.contains(".+") == True
		elif search_sign5 == "":
			test5 = dpd_df["Pāli1"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search5 error")

		# search6
		if search_sign6 == "equals":
			test6 = dpd_df[search_column6] == (search_string6)
		elif search_sign6 == "does not equal":
			test6 = dpd_df[search_column6] != (search_string6)
		elif search_sign6 == "contains":
			test6 = dpd_df[search_column6].str.contains(search_string6)
		elif search_sign6 == "does not contain":
			test6 = dpd_df[search_column6].str.contains(search_string6) == False
		elif search_sign6 == "contains word":
			test6 = dpd_df[search_column6].str.contains(fr"\b{search_string6}\b")
		elif search_sign6 == "does not contain word":
			test6 = dpd_df[search_column6].str.contains(fr"\b{search_string6}\b") == False
		elif search_sign6 == "is empty":
			test6 = dpd_df[search_column6] == ("")
		elif search_sign6 == "is not empty":
			test6 = dpd_df[search_column6].str.contains(".+") == True
		elif search_sign6 == "":
			test6 = dpd_df["Pāli1"].str.contains(".+")
		else:
			print(f"{timeis()} {red}search6 error")

		filter = test_exceptions & test1 & test2 & test3 & test4 & test5 & test6

		filtered_df = dpd_df.loc[filter, [print_column1, print_column2, print_column3]]
		all_tests_df = dpd_df.loc[filter, [print_column1, print_column2, print_column3]]

		column_count2 = filtered_df.shape[0]
		filtered_df = filtered_df.head(iterations)
		column_count1 = filtered_df.shape[0]

		column_count3 = all_tests_df.shape[0]

		# print to text file

		if column_count1 > 0:
			with open("test_results.txt", 'a') as txt_file:
				txt_file.write (f"{line_break}\n")
				txt_file.write (f"{line}. {search_name} ({column_count1} of {column_count2})\n")
				txt_file.write (f"{line_break}\n")
				filtered_df.to_csv(txt_file, header=False, index=False, sep="\t")

		if column_count1 > 0:
			with open("test_results_all.txt", 'a') as txt_file2:
				txt_file2.write (f"{line_break}\n")
				txt_file2.write (f"{line}. {search_name} ({column_count3})\n")
				txt_file2.write (f"{line_break}\n")
				all_tests_df.to_csv(txt_file2, header=False, index=False, sep="\t")

		line += 1

	txt_file.close()
	txt_file2.close()


def test_words_construction_are_headwords():
	print(f"{timeis()} {green}test if words in constructiona are headwords")
	headwords_list = dpd_df["Pāli1"].str.replace(" \d*", "").tolist()
	exceptions_list = ["ika", "iya", "ena", "*ya"]
	count = 0
	text_string = ""

	for row in range(len(dpd_df)): #len(dpd_df)
		headword = dpd_df.loc[row, "Pāli1"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		pos = dpd_df.loc[row, "POS"]
		construction = dpd_df.loc[row, "Construction"]
		construction = re.sub(">.+\ +", " + ", construction)
		construction = re.sub(r"\+", "", construction)
		construction = re.sub(">", "", construction)
		construction = re.sub("\(", "", construction)
		construction = re.sub("\)", "", construction)
		construction = re.sub("\n.+", "", construction)
		construction_list = construction.split()
		pali_root = dpd_df.loc[row, "Pāli Root"]

		if meaning != "" and pali_root == "" and pos != "sandhi" and pos != "idiom":
			for item in construction_list:
				if item in headwords_list:
					pass
				if item not in headwords_list and item not in exceptions_list:
					if count <= 10:
						text_string += f"{headword}. {item} not in heawords\n"
					count += 1

	with open("test_results.txt", 'a') as txt_file:
		txt_file.write (f"{line_break}\n")
		txt_file.write (f"construction not in headword (10/{count})\n")
		txt_file.write (f"{line_break}\n")
		txt_file.write(text_string)
		txt_file.write (f"{line_break}\n")


def test_headword_in_inflections():
	print(f"{timeis()} {green}test if headword in inflection table")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')
	
	ignore_pos = ["idiom", "abs", "ger", "ind", "sandhi", "inf", "prefix", "prp", "abbrev", "cs", "letter", "suffix", "prefix"]
	ignore_patterns = [
		"ar masc",
		"ar2 masc",
		"as masc",
		"arahant masc",
		"bhavant masc",
		"ar fem",
		"mātar fem",
		"ant adj",
		"anta adj",
		"ant masc",
		"tvaṃ pron",
		"amu pron",
		"ta pron",
		"ima pron",
		"kaci pron",
		"ka pron",
		"ubha pron"
		]

	# fixme test these ignore patterns with re.sub
	
	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \d*", "", headword)
		pos = dpd_df.loc[row, "POS"]
		pattern = dpd_df.loc[row, "Pattern"]
		metadata = dpd_df.loc[row, "Metadata"]
		grammar = dpd_df.loc[row, "Grammar"]
		
		if not re.findall("irreg form of", grammar) and pos not in ignore_pos and pattern not in ignore_patterns and metadata == "":
			try:
				with open(f"../inflection generator/output/inflections in table/{headword}", "rb") as f:
					inflections = pickle.load(f)
					match = False
					for word in inflections:
						if word == headword_clean:
							match = True
					if match == False:
						counter += 1
						if counter == 1:
							txt_file1.write (f"{line_break}\nheadword not in inflection table\n{line_break}\n")
							txt_file2.write (f"{line_break}\nheadword not in inflection table\n{line_break}\n")
						if counter <= 10:
							txt_file1.write (f"{headword}\n")
						txt_file2.write (f"{headword}\n")
			except:
				print(f"{timeis()} {red}{headword} not found")

	txt_file1.close()
	txt_file2.close()


def test_suffix_matches_pāli1():
	print(f"{timeis()} {green}test if suffix matches pāli1")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')

	exceptions = ["adhipa", "bavh", "labbhā", "munī"]

	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword = re.sub(" \d*", "", headword)
		headword_last = headword[len(headword)-1]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		suffix = dpd_df.loc[row, "Suffix"]
		suffix_len = len(suffix)
		if suffix_len > 0:
			suffix_last_letter = suffix[suffix_len-1]
		else:
			suffix_last_letter = ""
		# print(f"{headword}, {suffix}, {suffix_len}, {suffix_last_letter}")
		
		if suffix != "" and headword not in exceptions:
			if headword_last != suffix_last_letter:
				counter += 1
				if counter == 1:
					txt_file1.write (f"{line_break}\nsuffix does not match Pāli1\n{line_break}\n")
					txt_file2.write (f"{line_break}\nsuffix does not match Pāli1\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {suffix}\n")
				txt_file2.write (f"{headword} / {suffix}\n")

	txt_file1.close()
	txt_file2.close()


def test_construction_line1_matches_pāli1():
	print(f"{timeis()} {green}test if construction line1 matches pāli1")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')

	exceptions = ["adhipa"]

	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \d*", "", headword)
		headword_last = headword_clean[len(headword_clean)-1]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		construction = dpd_df.loc[row, "Construction"]
		
		if len(construction) > 0:
			construction = re.sub ("\n.+", "", construction)
			construction_last = construction[len(construction)-1]
			
			if meaning != "" and construction != "" and headword not in exceptions:
				if headword_last != construction_last:
					counter += 1
					if counter == 1:
						txt_file1.write (f"{line_break}\nconstruction line1 does not match Pāli1\n{line_break}\n")
						txt_file2.write (f"{line_break}\nconstruction line1 does not match Pāli1\n{line_break}\n")
					if counter <= 10:
						txt_file1.write (f"{headword} / {construction}\n")
					txt_file2.write (f"{headword} / {construction}\n")

	txt_file1.close()
	txt_file2.close()

def test_construction_line2_matches_pāli1():
	print(f"{timeis()} {green}test if construction line2 matches pāli1")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')

	exceptions = []
	
	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \d*", "", headword)
		headword_last = headword_clean[len(headword_clean)-1]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		construction = dpd_df.loc[row, "Construction"]
		
		if re.findall("\n", construction):
			construction = re.sub (".+\n", "", construction)
			if len(construction) > 0:
				construction_last = construction[len(construction)-1]
				
				if meaning != "" and construction != "" and headword not in exceptions:
					if headword_last != construction_last:
						counter += 1
						if counter == 1:
							txt_file1.write (f"{line_break}\nconstruction line2 does not match Pāli1\n{line_break}\n")
							txt_file2.write (f"{line_break}\nconstruction line2 does not match Pāli1\n{line_break}\n")
						if counter <= 10:
							txt_file1.write (f"{headword} / {construction}\n")
						txt_file2.write (f"{headword} / {construction}\n")

	txt_file1.close()
	txt_file2.close()

def missing_number():
	print(f"{timeis()} {green}test if pāli1 is missing a number")
	global clean_headwords_list

	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')
	
	dpd_df['Pāli3'] = dpd_df['Pāli1'].str.replace(" \d{1,2}", "")
	clean_headwords_list =  dpd_df["Pāli3"].tolist()

	everyx = int(date) % 14
	counter = 0

	if  everyx != 0:
		print(f"{timeis()} ... will run again in {blue}{everyx}{white} days")
	
	else:
		for row in range(dpd_df_length):
			headword = dpd_df.loc[row, "Pāli1"]
			headword_clean = re.sub(" \d{1,2}", "", headword)
			count = clean_headwords_list.count(headword_clean)

			if row % 5000 == 0:
				print(f"{timeis()} {row}/{dpd_df_length}\t{headword}")
			
			if not re.findall("\d", headword) and not re.findall(" ", headword_clean) and count > 1:
				counter += 1
				if counter == 1:
					txt_file1.write (f"{line_break}\nPāli1 is missing a number\n{line_break}\n")
					txt_file2.write (f"{line_break}\nPāli1 is missing a number\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {count}\n")
				txt_file2.write (f"{headword} / {count}\n")

	txt_file1.close()
	txt_file2.close()

def extra_number():
	print(f"{timeis()} {green}test if pāli1 contains an extra number")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')
	everyx = int(date) % 15
	counter = 0

	if  everyx != 0: 
		print(f"{timeis()} ... will run again in {blue}{everyx}{white} days")
	
	else:
		for row in range(dpd_df_length):
			headword = dpd_df.loc[row, "Pāli1"]
			headword_clean = re.sub(" \d{1,2}", "", headword)
			count = clean_headwords_list.count(headword_clean)

			if row % 5000 == 0:
				print(f"{timeis()} {row}/{dpd_df_length}\t{headword}")
			
			if re.findall("\d", headword) and count == 1:
				counter += 1
				if counter == 1:
					txt_file1.write (f"{line_break}\nPāli1 contains an extra number\n{line_break}\n")
					txt_file2.write (f"{line_break}\nPāli1 contains an extra number\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {count}\n")
				txt_file2.write (f"{headword} / {count}\n")

	txt_file1.close()
	txt_file2.close()

def derived_from_in_headwords():
	print(f"{timeis()} {green}test if derived from is in pāli1")
	txt_file1 = open("test_results.txt", 'a')
	txt_file2 = open ("test_results_all.txt", 'a')
	
	global root_families_list
	root_families_list = list(set(dpd_df["Family"].tolist()))
	root_families_list.remove("")

	counter = 0

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		derived_from = dpd_df.loc[row, "Derived from"]

		if meaning != "" and derived_from != "" and not derived_from in clean_headwords_list and derived_from not in root_families_list:
			counter += 1
			if counter == 1:
				txt_file1.write (f"{line_break}\nderived from not in Pāli1\n{line_break}\n")
				txt_file2.write (f"{line_break}\nderived from not in Pāli1\n{line_break}\n")
			if counter <= 10:
				txt_file1.write (f"{headword} / {derived_from}\n")
			txt_file2.write (f"{headword} / {derived_from}\n")

	txt_file1.close()
	txt_file2.close()

def print_columns():
	with open("test_results.txt", 'a') as txt_file:
		headings = list(dpd_df.columns.values)
		txt_file.write (f"{line_break}\n{headings}\n")

def open_test_results():
	print(f"{timeis()} {green}opening results")
	import os
	os.popen('code "test_results.txt"')
	import os
	os.popen('code "test_results_all.txt"')
	print(f"{timeis()} {line_break}")


make_new_dpd_csv()
setup_dfs()
tests_data_integrity_tests()
generate_test_results()
# test_words_construction_are_headwords()
test_headword_in_inflections()
test_suffix_matches_pāli1()
test_construction_line1_matches_pāli1()
test_construction_line2_matches_pāli1()
missing_number()
extra_number()
derived_from_in_headwords()
print_columns()
open_test_results()