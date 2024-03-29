#!/usr/bin/env python3.10
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
import random
import json

from timeis import timeis, yellow, blue, white, green, red, line, tic, toc
from test_formulas import setup_dpd_df, test_formulas, export_ods_with_formulas
from family2_test import construction_does_not_equal_family2

warnings.filterwarnings("ignore", 'This pattern has match groups')
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

now = datetime.now()
time_now = now.strftime("%Y/%m/%d %H:%M:%S")
date = now.strftime("%d")
line_break = line


def time_difference(now, then):
	difference = now - then
	difference_seconds = difference.total_seconds()
	days = divmod(difference_seconds, 86400)
	return days[0]

print(f"{timeis()} {line_break}")
print(f"{timeis()} {yellow}dpd data integrity tests")
print(f"{timeis()} {line_break}")


def make_new_dpd_csv():
	global yn

	dpd_csv_file = "../csvs/dpd.csv"
	dpd_csv_stats = os.stat(dpd_csv_file)
	dpd_csv_mod_time = time.ctime(dpd_csv_stats[stat.ST_MTIME])

	print(f"{timeis()} {green}dpd.csv last modified on \t\t{blue}{dpd_csv_mod_time}{white}")
	yn = (input(f"{timeis()} convert ods to dpd.csv? (y/n)\t{blue}"))

	if yn == "y":
		print(f"{timeis()} {green}reading ods_file")
		ods_file = "../dpd.ods"
		csv_file = "../csvs/dpd.csv"
		sheet_index = 1
		# df = pd.read_excel(ods_file, sheet_name="PALI", engine="odf")
		df = read_ods(ods_file, sheet_index, headers=False)

		print(f"{timeis()} {green}cleaning up dataframe")
		df = df.drop(index=0)  # removing first row
		new_header = df.iloc[0]  # grab the first row for the header
		df = df[1:]  # take the data less the header row
		df.columns = new_header  # set the header row as the df header
		df.dropna(subset=["Pāli1", "Stem"], inplace=True)  # remove messed up rows

		print(f"{timeis()} {green}writing data frame to {csv_file}")
		df.to_csv(csv_file, index=False, sep="\t", encoding="utf-8")


def setup_dfs():
	print(f"{timeis()} {green}setting up dataframes")
	global dpd_df
	global dpd_df_length
	global tests_df
	global test_column_count
	global line

	print(f"{timeis()} {green}importing dpd.csv", end = " ")
	dpd_df = pd.read_csv ("../csvs/dpd.csv", sep="\t", dtype=str, skip_blank_lines=False)
	dpd_df.fillna("", inplace=True)
	dpd_df_length = len(dpd_df)
	print(f"{white}{dpd_df.shape}")

	print(f"{timeis()} {green}importing tests.csv", end=" ")
	tests_df = pd.read_csv ("tests.csv", sep="\t", dtype=str, skip_blank_lines=False)
	tests_df.fillna("", inplace=True)
	test_column_count = tests_df.shape[0]
	print(f"{white}{tests_df.shape}")

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
			print (f"{timeis()}{red} {line}. {search_name} search column 1 *{search_column1}* does not exist")
		if search_column2 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} search column 2 *{search_column2}* does not exist")
		if search_column3 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} search column 3 *{search_column3}* does not exist")
		if search_column4 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} search column 4 *{search_column4}* does not exist")
		if search_column5 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} search column 5 *{search_column5}* does not exist")
		if search_column6 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} search column 6 *{search_column6}* does not exist")
		if print_column1 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} print column 1 *{print_column1}* does not exist")
		if print_column2 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} print column 2 *{print_column2}* does not exist")
		if print_column3 not in dpd_df_column_names:
			print (f"{timeis()}{red} {line}. {search_name} print column 3 *{print_column3}* does not exist")

		search_sign1 = (tests_df.iloc[row, 2])
		search_sign2 = (tests_df.iloc[row, 5])
		search_sign3 = (tests_df.iloc[row, 8])
		search_sign4 = (tests_df.iloc[row, 11])
		search_sign5 = (tests_df.iloc[row, 14])
		search_sign6 = (tests_df.iloc[row, 17])

		if search_sign1 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign1 *{search_sign1}* does not exist")

		if search_sign2 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign2 *{search_sign2}* does not exist")

		if search_sign3 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign3 *{search_sign3}* does not exist")

		if search_sign4 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign4 *{search_sign4}* does not exist")

		if search_sign5 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign5 *{search_sign5}* does not exist")

		if search_sign6 not in ["equals", "does not equal", "contains", "does not contain", "contains word", "does not contain word", "is empty", "is not empty", ""]:
			print (f"{timeis()}{red} {line}. {search_name} search_sign6 *{search_sign6}* does not exist")

		line += 1

def generate_test_results():
	print(f"{timeis()} {green}generating test results")
	txt_file = open ("output/test_results.txt", 'w', encoding= "'utf-8")
	txt_file2 = open ("output/test_results_all.txt", 'w', encoding= "'utf-8")

	with open("output/test_results.txt", 'a') as txt_file:
		txt_file.write (f"DPD tests {time_now}\n")
		txt_file.write (f"{line_break}\n")


	# define variables
	for row in range(0, test_column_count):  # test_column_count
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
		filtered_df = filtered_df.head(iterations)
		all_tests_df = dpd_df.loc[filter, [print_column1, print_column2, print_column3]]

		column_count1 = filtered_df.shape[0]
		column_count2 = filtered_df.shape[0]
		column_count3 = all_tests_df.shape[0]

		allwords = all_tests_df['Pāli1'].to_list()

		# print to text file

		if column_count1 > 0:
			with open("output/test_results.txt", 'a') as txt_file:
				txt_file.write (f"{line_break}\n")
				txt_file.write (f"{line}. {search_name} ({column_count1} of {column_count2})\n")
				txt_file.write (f"{line_break}\n")
				filtered_df.to_csv(txt_file, header=False, index=False, sep="\t")
				txt_file.write(f"\n")
				for word in allwords[:iterations]:
					if word == allwords[0]:
						txt_file.write(f"^({word}")
					else:
						txt_file.write(f"|{word}")
				txt_file.write(f")$\n\n")

		if column_count1 > 0:
			with open("output/test_results_all.txt", 'a') as txt_file2:
				txt_file2.write (f"{line_break}\n")
				txt_file2.write (f"{line}. {search_name} ({column_count3})\n")
				txt_file2.write (f"{line_break}\n")
				all_tests_df.to_csv(txt_file2, header=False, index=False, sep="\t")
				txt_file2.write(f"\n")
				for word in allwords:
					if word == allwords[0]:
						txt_file2.write(f"^({word}")
					else:
						txt_file2.write(f"|{word}")
				txt_file2.write(f")$\n\n")

		line += 1

	txt_file.close()
	txt_file2.close()


def test_words_construction_are_headwords():
	print(f"{timeis()} {green}test if words in constructions are headwords")
	headwords_list = dpd_df["Pāli1"].str.replace(" \\d.*$", "").tolist()
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

	with open("output/test_results.txt", 'a') as txt_file:
		txt_file.write (f"{line_break}\n")
		txt_file.write (f"construction not in headword (10/{count})\n")
		txt_file.write (f"{line_break}\n")
		txt_file.write(text_string)
		txt_file.write (f"{line_break}\n")


def write_all_words(allwords, txt_file1, txt_file2):

	if allwords != []:
		txt_file1.write(f"\n^(")
		for word in allwords[:10]:
			if word == allwords[0]:
				txt_file1.write(f"{word}")
			else:
				txt_file1.write(f"|{word}")
		txt_file1.write(f")$\n")

		txt_file2.write(f"\n^(")
		for word in allwords:
			if word == allwords[0]:
				txt_file2.write(f"{word}")
			else:
				txt_file2.write(f"|{word}")
		txt_file2.write(f")$\n")

def duplicate_meanings():
	print(f"{timeis()} {green}finding duplicate meanings in family2", end=" ")
	family2_list = dpd_df["Family2"].to_list()

	# make a list of all words with double meaniings

	duplicate_meaning_list = []
	for sublist in family2_list:
		word_list = sublist.split(" ")
		for word in word_list:
			if re.findall("\d", word):
				word = re.sub("\d", "", word)
				duplicate_meaning_list.append(word)
	duplicate_meaning_list = sorted(set(duplicate_meaning_list))
	try:
		duplicate_meaning_list.remove("")
	except ValueError:
		pass

	# write dupes to file

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	allwords = []
	counter = 0

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		family2 = dpd_df.loc[row, "Family2"]
		family2_list = family2.split(" ")
		for word in family2_list:
			if word in duplicate_meaning_list:
				counter += 1
				allwords.append(headword)
				if counter == 1:
					txt_file1.write(f"{line_break}\nwords with duplicate meanings family2\n{line_break}\n")
					txt_file2.write(f"{line_break}\nwords with duplicate meanings family2\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword}\t{word}\n")
				txt_file2.write(f"{headword}\t{word}\n")
			
	write_all_words(allwords, txt_file1, txt_file2)

	if counter == 0:
		print(f"{white} ok")
	else:
		print(f"{white}{len(allwords)}")

	txt_file1.close()
	txt_file2.close()

def test_headword_in_inflections():
	print(f"{timeis()} {green}test if headword in inflection table")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')

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
		
	allwords = []

	# fixme test these ignore patterns with re.sub
	with open("../inflection generator/output/all inflections dict", "rb") as f:
		all_inflections_dict = pickle.load(f)
	
	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pos = dpd_df.loc[row, "POS"]
		pattern = dpd_df.loc[row, "Pattern"]
		grammar = dpd_df.loc[row, "Grammar"]
		
		if not re.findall("irreg form of", grammar) and \
		pos not in ignore_pos and \
		pattern not in ignore_patterns:
			if headword in all_inflections_dict and \
			headword_clean not in all_inflections_dict[headword]["inflections"]:
				allwords.append(headword)
				if counter == 1:
					txt_file1.write(f"{line_break}\nheadword not in inflection table\n{line_break}\n")
					txt_file2.write(f"{line_break}\nheadword not in inflection table\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword}\n")
				txt_file2.write (f"{headword}\n")
				counter += 1
	
	write_all_words(allwords, txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()



def test_suffix_matches_pāli1():
	print(f"{timeis()} {green}test if suffix matches pāli1")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')

	exceptions = ["adhipa", "bavh", "labbhā", "munī", "gatī", "visesi", "khantī"]

	allwords= []

	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword = re.sub(" \\d.*$", "", headword)
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
				allwords.append(headword)
				if counter == 1:
					txt_file1.write (f"{line_break}\nsuffix does not match Pāli1\n{line_break}\n")
					txt_file2.write (f"{line_break}\nsuffix does not match Pāli1\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {suffix}\n")
				txt_file2.write (f"{headword} / {suffix}\n")

	write_all_words(allwords, txt_file1, txt_file2)
	
	txt_file1.close()
	txt_file2.close()


def test_construction_line1_matches_pāli1():
	print(f"{timeis()} {green}test if construction line1 matches pāli1")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')

	exceptions = [
		"abhijaññā", "acc", "adhipa", "aññā 2","aññā 3", "anujaññā", 
		"anupādā", "attanī", "chettu", "devāna", "dubbalī", "gāmaṇḍala 2.1",
		"gatī", "jaññā 2", "kayirā", "khaṇitti", "koṭṭhāsa 1", "koṭṭhāsa 2",
		"koṭṭhāsa 3", "labbhā", "lokasmi", "munī", "nājjhosa", "nānujaññā", 
		"nāsiṃsatī", "nāsīsatī", "natthī", "paralokavajjabhayadassāvine", 
		"paresa", "pariññā 2", "paṭivadeyyu", "phuseyyu", "sabbadhammāna", 
		"saḷ", "sat 1", "sat 2", "upādā", "vijaññā", "visesi", "govinda", 
		"sivathikā", "sīvathikā", "sakad"]

	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		headword_last = headword_clean[len(headword_clean)-1]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		construction = dpd_df.loc[row, "Construction"]
		grammar = dpd_df.loc[row, "Grammar"]
		
		if len(construction) > 0:
			construction = re.sub ("\n.+", "", construction)
			construction_last = construction[len(construction)-1]
			
			if meaning != "" and \
			construction != "" and \
			not re.findall ("\bcomp\b", grammar) and \
			headword not in exceptions and \
			headword_last != construction_last:
					allwords.append(headword)
					counter += 1
					if counter == 1:
						txt_file1.write (f"{line_break}\nconstruction line1 does not match Pāli1\n{line_break}\n")
						txt_file2.write (f"{line_break}\nconstruction line1 does not match Pāli1\n{line_break}\n")
					if counter <= 10:
						txt_file1.write (f"{headword} / {construction}\n")
					txt_file2.write (f"{headword} / {construction}\n")

	write_all_words(allwords, txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()

def test_construction_line2_matches_pāli1():
	print(f"{timeis()} {green}test if construction line2 matches pāli1")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')

	exceptions = []
	allwords = []
	
	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
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
						allwords.append(headword)
						if counter == 1:
							txt_file1.write (f"{line_break}\nconstruction line2 does not match Pāli1\n{line_break}\n")
							txt_file2.write (f"{line_break}\nconstruction line2 does not match Pāli1\n{line_break}\n")
						if counter <= 10:
							txt_file1.write (f"{headword} / {construction}\n")
						txt_file2.write (f"{headword} / {construction}\n")
	
	write_all_words(allwords, txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()

def missing_number():
	print(f"{timeis()} {green}test if pāli1 is missing a number")
	global clean_headwords_list

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')
	
	dpd_df['Pāli3'] = dpd_df['Pāli1'].str.replace(" \d{1,2}", "")
	clean_headwords_list =  dpd_df["Pāli3"].tolist()

	allwords = []

	with open("output/missing_number_lastrun", "rb") as p:
		then = pickle.load(p)
	days = time_difference(now, then)


	if days < 7:
		print(f"{timeis()} {green}...will run again in {blue}{7-days:.0f}{green} days")
	
	else:
		print(f"{timeis()} {green}...last ran {blue}{10-days:.0f}{green} days ago")
		counter=0
		for row in range(dpd_df_length):
			headword = dpd_df.loc[row, "Pāli1"]
			headword_clean = re.sub(" \d{1,2}", "", headword)
			count = clean_headwords_list.count(headword_clean)

			if row % 10000 == 0:
				print(f"{timeis()} {row}/{dpd_df_length}\t{headword}")
			
			if not re.findall("\d", headword) and not re.findall(" ", headword_clean) and count > 1:
				counter += 1
				allwords.append(headword)
				if counter == 1:
					txt_file1.write (f"{line_break}\nPāli1 is missing a number\n{line_break}\n")
					txt_file2.write (f"{line_break}\nPāli1 is missing a number\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {count}\n")
				txt_file2.write (f"{headword} / {count}\n")
		with open("output/missing_number_lastrun", "wb") as p:
			pickle.dump(now, p)

	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def extra_number():
	print(f"{timeis()} {green}test if pāli1 contains an extra number")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	with open("output/extra_number_lastrun", "rb") as p:
		then = pickle.load(p)
	days = time_difference(now, then)
	
	if days < 5:
		print(f"{timeis()} {green}...will run again in {blue}{5-days:.0f}{green} days")
	
	else:
		print(f"{timeis()} {green}last ran {blue}{days:.0f}{green} days ago")
		for row in range(dpd_df_length):
			headword = dpd_df.loc[row, "Pāli1"]
			headword_clean = re.sub(" \d{1,2}", "", headword)
			count = clean_headwords_list.count(headword_clean)

			if row % 10000 == 0:
				print(f"{timeis()} {row}/{dpd_df_length}\t{headword}")
			
			if re.findall("\d", headword) and count == 1:
				counter += 1
				allwords.append(headword)
				if counter == 1:
					txt_file1.write (f"{line_break}\nPāli1 contains an extra number\n{line_break}\n")
					txt_file2.write (f"{line_break}\nPāli1 contains an extra number\n{line_break}\n")
				if counter <= 10:
					txt_file1.write (f"{headword} / {count}\n")
				txt_file2.write (f"{headword} / {count}\n")

		with open("output/extra_number_lastrun", "wb") as p:
			pickle.dump(now, p)
	
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def derived_from_in_headwords():
	print(f"{timeis()} {green}test if derived from is in pāli1")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')
	
	global root_families_list
	root_families_list = list(set(dpd_df["Family"].tolist()))
	root_families_list.remove("")

	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		derived_from = dpd_df.loc[row, "Derived from"]
		grammar = dpd_df.loc[row, "Grammar"]

		if meaning != "" \
				and derived_from != "" \
				and not derived_from in clean_headwords_list \
				and derived_from not in root_families_list \
				and not re.findall("irreg form of", grammar) \
				and not re.findall(fr"\bcomp\b", grammar) \
				and not re.findall("√", derived_from):
			counter += 1
			allwords.append(headword)
			if counter == 1:
				txt_file1.write (f"{line_break}\nderived from not in Pāli1 - test in CST and BJ\n{line_break}\n")
				txt_file2.write (f"{line_break}\nderived from not in Pāli1 - test in CST and BJ\n{line_break}\n")
			if counter <= 10:
				txt_file1.write (f"{headword} / {derived_from}\n")
			txt_file2.write (f"{headword} / {derived_from}\n")

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def pali_words_in_english_meaning():
	print(f"{timeis()} {green}test if pāli words in english meanings")

	allwords = []
	exceptions_set = {"i", "me"}
	# exceptions_set = {"a", "abhidhamma", "ajātasattu", "ala", "an", "ana", "anuruddha", "anāthapiṇḍika", "apadāna", "arahant", "are", "assapura", "avanti", "aya", "aṅga", "aṅguttara", "aṭṭhakathā", "aṭṭhakavagga", "bhagga", "bhoja", "bhāradvāja", "bhātaragāma", "bhū", "bimbisāra", "bodhi", "bodhisatta", "brahma"}

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open ("output/test_results_all.txt", 'a')
	pali_word_string = ""
	english_word_string = ""
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pali_word_string += headword_clean + " "
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		meaning = meaning.lower()
		meaning_clean = re.sub("[^A-Za-zāīūṭḍḷñṅṇṃ1234567890\-'’ ]", "", meaning)
		english_word_string += meaning_clean + ";"
		
	pali_word_set = set(pali_word_string.split(" "))
	pali_word_set.discard("")

	english_word_string = re.sub("; ", ";", english_word_string)
	english_word_set = set(english_word_string.split(";"))
	english_word_set.discard("")
	english_word_set = english_word_set - exceptions_set
	
	results = sorted(pali_word_set & english_word_set)
	results_length = len(results)

	if results_length > 0:
		txt_file1.write (f"{line_break}\npāḷi words in english meanings ({results_length})\n{line_break}\n")
		txt_file2.write (f"{line_break}\npāḷi words in english meanings ({results_length})\n{line_break}\n")
		counter = 0
		allwords.append(headword)
		for item in results:
			if counter < 10:
				txt_file1.write (f"{item}\n")
			txt_file2.write (f"{item}\n")
			counter+=1
	
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def test_derived_from_in_family2():

	print(f"{timeis()} {green}test derived from in family 2")
	
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	exceptions = ["ana 1", "ana 2", "assā 2", "ato", "atta 2", "abhiṅkharitvā", "dhammani", "daddabhāyati", "pakudhaka", "vakkali", "vammika", "vammīka", "koliyā", "bhaggava", "bhāradvāja 1", "cicciṭa", "cicciṭāyati", "kambojā", "sahali", "vāsiṭṭha", "yāmuna", "bhesajja", "ciṅgulaka", "soṇḍi", "sudinna 2",
	]
	allwords = []
	
	counter = 0
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		pos = dpd_df.loc[row, "POS"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		derived_from = dpd_df.loc[row, "Derived from"]
		root = dpd_df.loc[row, "Pāli Root"]
		grammar = dpd_df.loc[row, "Grammar"]
		word_family = dpd_df.loc[row, "Word Family"]
		family2 = dpd_df.loc[row, "Family2"]
		family2_list = family2.split(" ")

		test1 = headword not in exceptions
		test2 = root == ""
		test3 = pos != "pron"
		test4 = meaning != ""
		test5 = derived_from != ""
		test6 = not re.findall(r"\bcomp\b", grammar)
		test7 = family2 == ""
		test8 = word_family == ""

		if test1 & test2 & test3 & test4 & test5 & test6 & test7 & test8:
			allwords.append(headword)
			if counter == 0:
				txt_file1.write(
					f"\n{line_break}\nderived from not in family2\n{line_break}\n")
				txt_file2.write(
					f"\n{line_break}\nderived from not in family2\n{line_break}\n")
			if counter <= 10:
				txt_file1.write(f"{headword} / {derived_from}\n")
			txt_file2.write(f"{headword} / {derived_from}\n")
			counter += 1
	
	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def print_columns():
	with open("output/test_results.txt", 'a') as txt_file:
		headings = list(dpd_df.columns.values)
		txt_file.write (f"\n{line_break}\n{headings}\n")
		txt_file.write(f"{line_break}\n")
		txt_file.write(f"Notez in Anki\n")
		txt_file.write(f"Notez in Google Sheets\n")
		txt_file.write(f"Notez in Notebook\n")
		txt_file.write(f"Bodhidhamma notes\n")


def open_test_results():
	print(f"{timeis()} {green}opening results")
	import os
	os.popen('code "output/test_results.txt"')
	import os
	os.popen('code "output/test_results_all.txt"')

def run_test_formulas():
	print(f"{timeis()} {green}testing formulas")

	with open("output/test_formulas_lastrun", "rb") as p:
		then = pickle.load(p)
	days = time_difference(now, then)

	if days > 15:
		print(f"{timeis()} {green}last ran {blue}{days:.0f}{green} days ago")
		export_ods_with_formulas()
		setup_dpd_df()
		test_formulas()
		with open("output/test_formulas_lastrun", "wb") as p:
			pickle.dump(now, p)
	else:
		print(f"{timeis()} {green}...will run again in {blue}{15-days:.0f} {green}days time")

def pos_does_not_equal_gram():
	print(f"{timeis()} {green}testing pos equals gram")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	exceptions_list = ["dve 2"]

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		pos = dpd_df.loc[row, "POS"]
		grammar = dpd_df.loc[row, "Grammar"]
		grammar_clean = re.sub("( |,).+$", "", grammar)
		if (headword not in exceptions_list
		and pos != grammar_clean):
			allwords.append(headword)
			if counter == 0:
				txt_file1.write(f"\n{line_break}\npos does not equal gram\n{line_break}\n")
				txt_file2.write(
					f"\n{line_break}\npos does not equal gram\n{line_break}\n")
			if counter <= 10:
				txt_file1.write(f"{headword} {pos} ≠ {grammar}\n")
			txt_file2.write(f"{headword} {pos} ≠ {grammar}\n")
			counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def pos_does_not_equal_pattern():
	print(f"{timeis()} {green}testing pos equals pattern")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	pos_exceptions = ['abbrev', 'abs', 'cs', 'fut', 'ger', 'idiom', 'imp', 'ind', 'inf', 'letter', 'root', 'opt', 'prefix', 'sandhi', 'suffix', 've', 'var']
	headword_exceptions = ["paṭṭhitago", "dve 2"]

	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		pos = dpd_df.loc[row, "POS"]
		pattern = dpd_df.loc[row, "Pattern"]
		if len(re.findall(" ", pattern)) == 1:
			pattern_clean=re.sub(".* ", "", pattern)
		elif len(re.findall(" ", pattern)) == 2:
			pattern_clean = re.sub(".* (.+) .*", "\\1", pattern)
		else:
			pattern_clean = pattern

		if pos not in pos_exceptions:
			if headword not in headword_exceptions:
				if pos != pattern_clean:
					allwords.append(headword)
					if counter == 0:
						txt_file1.write(f"\n{line_break}\npos does not equal pattern\n{line_break}\n")
						txt_file2.write(
							f"\n{line_break}\npos does not equal pattern\n{line_break}\n")
					if counter <= 10:
						txt_file1.write(f"{headword} {pos} ≠ {pattern}\n")
					txt_file2.write(f"{headword} {pos} ≠ {pattern}\n")
					counter += 1
	
	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def reset_lastrun():
	then = datetime(2012, 3, 5, 23, 8, 15)
	with open("output/extra_number_lastrun", "wb") as p:
		pickle.dump(then, p)
	with open("output/missing_number_lastrun", "wb") as p:
		pickle.dump(then, p)
	with open("output/test_formulas_lastrun", "wb") as p:
		pickle.dump(then, p)


def derivatives_in_compounds():
	print(f"{timeis()} {green}testing for derivatives in compounds")
	family2_list = set(dpd_df['Family2'].str.split(" "))
	
	test1 = dpd_df['Grammar'].str.contains('\bcomp\b')
	test2 = dpd_df['Derivative'].empty()
	test3 = dpd_df['Pāli Root'].empty()
	test4 = ~dpd_df['Grammar'].str.contains('\bpl\b')
	test5 = ~dpd_df['Grammar'].str.contains('\b(nom|acc|instr|dat|abl|gen|loc|voc)\b')
	filter = test1 & test2 & test3 & test4 & test5
	filtered_df = dpd_df[filter]
	construction_list = set(filtered_df['Construction'].str.split(" + "))

	not_in_family2_list = []
	for row in range(len(filtered_df)):
		family2words = filtered_df.loc['Family2'].str.split(" ")
		construction_words = filtered_df.loc['Construction'].str.split(" + ")
		for construction_word in construction_words:
			if construction_word not in family2words:
				not_in_family2_list.append(construction_word)
	
	print(not_in_family2_list)
	# for row in range(len(dpd_df)):

def bases_contains_star():
	print(f"{timeis()} {green}testing for extra *'s in bases")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root = dpd_df.loc[row, "Pāli Root"]
		root_clean = re.sub("√", "", root)
		base = dpd_df.loc[row, "Base"]
		if base != "":
			if re.findall(f" > {root_clean}", base) and \
			re.findall(r"\*", base) and \
			not re.findall(">.+>", base):
				# print(headword, base)
				allwords.append(headword)
				if counter == 0:
					txt_file1.write(f"\n{line_break}\ndelete * from base\n{line_break}\n")
					txt_file2.write(f"\n{line_break}\ndelete * from base\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword} {base}\n")
				txt_file2.write(f"{headword} {base}\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def vuddhi(root):
	root = re.sub("a", "ā", root)
	root = re.sub("i", "ī", root)
	root = re.sub("u", "ū", root)
	return root

def bases_needs_star():
	print(f"{timeis()} {green}testing for no *'s in bases")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root = dpd_df.loc[row, "Pāli Root"]
		root_clean = re.sub("√", "", root)
		root_vuddhi = vuddhi(root_clean)
		sign = dpd_df.loc[row, "Sgn"]
		base = dpd_df.loc[row, "Base"]

		if base != "":
			if re.findall("a|u|i", root) and \
			re.findall(r"\*", sign) and \
			re.findall(f" > {root_vuddhi}", base) and \
			not re.findall(r"\*", base) and \
			not re.findall(">.+>", base):
				# print(headword, root_vuddhi, base)
				allwords.append(headword)
				if counter == 0:
					txt_file1.write(f"\n{line_break}\nadd * to base\n{line_break}\n")
					txt_file2.write(f"\n{line_break}\nadd * to base\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword} {base}\n")
				txt_file2.write(f"{headword} {base}\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def root_family_mismatch():
	print(f"{timeis()} {green}testing for root = family")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root = dpd_df.loc[row, "Pāli Root"]
		root_clean = re.sub(" \\d.*$", "", root)
		root_clean = re.sub("√", "", root_clean)
		family = dpd_df.loc[row, "Family"]
		if family == "":
			family = "[empty]"
		family_clean = re.sub(".*√", "", family)
		if root_clean != family_clean and root != "":
			allwords.append(headword)
			if counter == 0:
				txt_file1.write(f"\n{line_break}\nroot family mismatch\n{line_break}\n")
				txt_file2.write(f"\n{line_break}\nroot family mismatch\n{line_break}\n")
			if counter <= 10:
				txt_file1.write(f"{headword}. {root} ≠ {family}\n")
			txt_file2.write(f"{headword}. {root} ≠ {family}\n")
			counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def root_construction_mismatch():
	print(f"{timeis()} {green}testing for root = construction")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root = dpd_df.loc[row, "Pāli Root"]
		root_clean = re.sub(" \\d.*$", "", root)
		construction = dpd_df.loc[row, "Construction"]
		if re.findall ("√", construction):
			construction_line1 = re.sub("\n.+", "", construction)
			construction_clean = re.sub("(.*)(√.[^ ]*)(.*)", "\\2", construction_line1)
			if root_clean != construction_clean and root != "":
				allwords.append(headword)
				if counter == 0:
					txt_file1.write(f"\n{line_break}\nroot construction mismatch\n{line_break}\n")
					txt_file2.write(f"\n{line_break}\nroot construction mismatch\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword}. {root_clean} ≠ {construction_line1}\n")
				txt_file2.write(f"{headword}. {root_clean} ≠ {construction_line1}\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def family_construction_mismatch():
	print(f"{timeis()} {green}testing for family = construction")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		family = dpd_df.loc[row, "Family"]
		family_clean = re.sub(".*√", "√", family)
		construction = dpd_df.loc[row, "Construction"]
		if re.findall ("√", construction):
			construction_line1 = re.sub("\n.+", "", construction)
			construction_clean = re.sub("(.*)(√.[^ ]*)(.*)", "\\2", construction_line1)
			if family_clean != construction_clean:
				allwords.append(headword)
				# print(f"h: {headword} /n f: {family}\nc: {construction_line1}\nfc: {family_clean}\ncc: {construction_clean}\n")
				if counter == 0:
					txt_file1.write(f"\n{line_break}\nfamily construction mismatch\n{line_break}\n")
					txt_file2.write(f"\n{line_break}\nfamily construction mismatch\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword}. {family} ≠ {construction_line1}\n")
				txt_file2.write(f"{headword}. {family} ≠ {construction_line1}\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def root_base_mismatch():
	print(f"{timeis()} {green}testing for root = base")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root = dpd_df.loc[row, "Pāli Root"]
		root_clean = re.sub(" \\d.*$", "", root)
		if root == "":
			root = "[empty]"
		base = dpd_df.loc[row, "Base"]
		base_clean = re.sub("(√.[^ ]*)(.*)", "\\1", base)
		if base != "":
			if root_clean != base_clean:
				allwords.append(headword)

				if counter == 0:
					txt_file1.write(
						f"\n{line_break}\nroot base mismatch\n{line_break}\n")
					txt_file2.write(
						f"\n{line_break}\nroot base mismatch\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword}. {root_clean} ≠ {base}\n")
				txt_file2.write(
                f"{headword}. {root_clean} ≠ {base}\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def root_sign_base_mismatch():
	print(f"{timeis()} {green}testing for root sign = base")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	counter = 0
	allwords = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		root_sign = dpd_df.loc[row, "Sgn"]
		root_sign = re.sub("\\*", "", root_sign)
		root_sign = re.sub("\\+", "", root_sign)
		base = dpd_df.loc[row, "Base"]
		base = re.sub("\\*", "", base)
		base = re.sub("\\+", "", base)

		if base != "" and \
		not re.findall("intens|desid|perf|fut", base):
			if not re.findall(root_sign, base):
				allwords.append(headword)
				# print(f"h: {headword}\nb: {base}\nr: {root_sign}\nbc: {base}\n""")

				if counter == 0:
					txt_file1.write(
						f"\n{line_break}\nroot sign base mismatch\n{line_break}\n")
					txt_file2.write(
						f"\n{line_break}\nroot sign base mismatch\n{line_break}\n")
				if counter <= 10:
					txt_file1.write(f"{headword}. '{root_sign}' ≠ '{base}'\n")
				txt_file2.write(f"{headword}. '{root_sign}' ≠ '{base}'\n")
				counter += 1

	if counter != 0:
		txt_file2.write(f"[{counter}]\n")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def complete_root_matrix():
	print(f"{timeis()} {green}finding roots that need adding")

	# ods_file = "../dpd.ods"
	# csv_file = "../csvs/roots2.csv"
	# # roots_df = pd.read_excel(ods_file, sheet_name="Roots", engine="odf")
	# print(roots_df)
	
	roots_df = pd.read_csv("../csvs/roots.csv", sep="\t")
	roots_df.fillna("", inplace=True)

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	 
	# print(roots_df)

	mid_df = roots_df.sort_values(["Count", "Root"])
	test1 = mid_df["Count"] != 0
	test2 = mid_df["matrix test"] != "√"
	mid_df = mid_df[test1 & test2]
	mid_df = mid_df.reset_index()
	mid_df_half = len(mid_df)/2
	mid_df_middle = mid_df.loc[mid_df_half-15:mid_df_half+5, ["Root", "Meaning", "Count"]]
	mid_df_string = str(mid_df_middle.to_string(header=None))

	txt_file1.write(f"\n{line_break}\nadd info to roots ({len(mid_df_middle)}/{len(mid_df)})\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nadd info to roots\n{line_break}\n")
	txt_file1.write(f"{mid_df_string}\n")
	txt_file2.write(f"{mid_df_string}\n")
	
	txt_file1.close()
	txt_file2.close()

def random_words():
	print(f"{timeis()} {green}twenty words - root or compound?")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	
	pos = ['idiom', 'sandhi', 'prefix', 'pron']
	
	test1 = dpd_df['Meaning IN CONTEXT'] == ""
	test2 = dpd_df['Pāli Root'] == ""
	test3 = ~dpd_df['Grammar'].str.contains("\\bcomp\\b")
	test4 = ~dpd_df['Grammar'].str.contains("\\bfrom .+$")
	test5 = ~dpd_df['POS'].isin(pos)
	filter = test1 & test2 & test3 & test4 & test5
	
	filter_df = dpd_df.loc[filter, ["Pāli1", "POS", "Buddhadatta"]]
	filter_df_len = len(filter_df)
	x = random.randint(0, filter_df_len-20)
	y = x+20
	filter_df = filter_df.iloc[x:y]
	
	txt_file1.write(f"\n{line_break}\nroot or compound?\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nroot or compound?\n{line_break}\n")

	allwords = []
	counter = 0
	for row in range(20):
		headword = filter_df.iloc[row,0]
		pos = filter_df.iloc[row, 1]
		meaning = filter_df.iloc[row, 2]
		allwords.append(headword)

		if counter < 10:
			txt_file1.write(f"{headword}\t{pos}\t{meaning}\n")
		txt_file2.write(f"{headword}\t{pos}\t{meaning}\n")
	
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def identical_meanings():
	print(f"{timeis()} {green}meanings identical")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	dpd_df['Meaning IN CONTEXT'] = dpd_df['Meaning IN CONTEXT'].str.replace("\\(.+\\)", "")
	dpd_df['Buddhadatta'] = dpd_df['Buddhadatta'].str.replace("\\(.+\\)", "")

	test1 = dpd_df['Meaning IN CONTEXT'] == dpd_df['Buddhadatta']
	test2 = dpd_df['Meaning IN CONTEXT'].str.contains(";| ")
	filter = test1 & test2
	filter_df = dpd_df.loc[filter, ["Pāli1", "Meaning IN CONTEXT", "Buddhadatta"]]
	print(filter_df)
	filter_df_len = len(filter_df)

	txt_file1.write(f"\n{line_break}\nmeanings identical\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nrmeanings identical\n{line_break}\n")

	allwords = []
	counter = 0
	for row in range(filter_df_len):
		headword = filter_df.iloc[row, 0]
		meaning1 = filter_df.iloc[row, 1]
		meaning2 = filter_df.iloc[row, 2]
		allwords.append(headword)

		if counter < 10:
			txt_file1.write(f"{headword}\t{meaning1}\t{meaning2}\n")
		txt_file2.write(f"{headword}\t{meaning1}\t{meaning2}\n")
		counter+=1
		
	txt_file2.write(f"{counter}")
	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def add_family2():
	print(f"{timeis()} {green}add family2")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	
	test1 = dpd_df['Meaning IN CONTEXT'] != ""
	test2 = dpd_df['Pāli Root'] == ""
	test3 = ~dpd_df['Pāli2'].str.contains("suttaṃ")
	test4 = dpd_df['Word Family'] == ""
	test5 = dpd_df['Pāli1'].str.len() < 33
	
	filter = test1 & test2 & test3 & test4 & test5
	filter_df = dpd_df.loc[filter]
	filter_df = filter_df.reset_index()
	filter_df_len = len(filter_df)
	
	exceptions = [
		"", "ā", "a", "tta", "tā", "vasena", "ādi", "aṃ", "āni", "ka", "na", "ena", "ta", "na > a", "eta", "ya", "ima", "ehi", "saṃ", "tumha", "sa", "ka > ki", "e", "āya", "mhā", "ū", "so", "i", "āhi", "me", "smā", "vant", "issā", "eyya", "nagaravinda", "esānaṃ", "ssaṃ", "ākaṃ", "esaṃ", "*a", "[d]", "vāja", "sara", "kuṇapa", "bhara", "[t]", "sāriputta", "ssa", "muru", "bhallika", "ānaṃ > aṃ", "[n]", "tara", "tha", "to", "tā", "udāyī", "uṃ", "biḷi", "esānaṃ > esaṃ", "na > an", "ra", "dhā", "mhi", "bhara > bhāra", "ci", "ayā", "[h]", "esu", "kāḷī", "paññāya", "siṅgālaka", "pa", "vi", "su", "mati", "masāra", "vassa", "kuvera", "ā > a", "(n)a"
		]
	
	no_thanks = ["sutta", "sikkhāpada", "ti", "smiṃ",
              "m", "ssā", "pāḷi", "nikāya", "saṃyutta"]
	wic_dict = {}
	
	for row in range(filter_df_len):
		flag = False
		headword = filter_df.loc[row, "Pāli1"]
		family2 = filter_df.loc[row, "Family2"]
		construction = filter_df.loc[row, "Construction"]
		if family2 == "":
			construction = re.sub("\n.+", "", construction)
			words_in_constr = construction.split(" + ")

			for word_in_constr in words_in_constr:
				if word_in_constr in no_thanks:
					flag = True

			for word in words_in_constr:
				if flag != True:
					if word not in wic_dict:
						wic_dict[word] = 1
					if word in wic_dict:
						wic_dict[word] += 1

	for exception in exceptions:
		try:
			wic_dict.pop(exception)
		except:
			print(f"{timeis()} {red}{exception}")

	wic_df = pd.DataFrame.from_dict(wic_dict, orient='index')
	wic_df.sort_values(by=[0], inplace=True, ascending=False)

	txt_file1.write(f"\n{line_break}\nadd family2\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nadd family2\n{line_break}\n")

	for index in range(10):
		searchword = wic_df.index[index]
		allwords= []
		for row in range(filter_df_len):
			headword = filter_df.loc[row, "Pāli1"]
			family2 = filter_df.loc[row, "Family2"]
			construction = filter_df.loc[row, "Construction"]

			if re.findall(f"\\b{searchword}\\b", construction):
				if not re.findall(f"\\b{searchword}\\d*\\b", family2):
					allwords.append(headword)
	
		txt_file1.write(f"{searchword} {len(allwords)}\n")
		txt_file2.write(f"{searchword} {len(allwords)}\n")
		txt_file1.write(f"^(")
		txt_file2.write(f"^(")
		for word in allwords:
			if word not in exceptions:
				if word != allwords[-1]:
					txt_file1.write(f"{word}|")
					txt_file2.write(f"{word}|")
				else:
					txt_file1.write(f"{word})$\n\n")
					txt_file2.write(f"{word})$\n\n")

	txt_file1.close()
	txt_file2.close()


def test_family2(dpd_df, dpd_df_length):
	print(f"{timeis()} {green}testing if words in construction are in family2", end=" ")

	exceptions = ["accha", "an", "ana", "asa", "asati", "atta", "attha", "aṭṭha", "du", "dha", "dhā", "dur", "iṃ", "jāni", "jāniya", "kaccha", "kha", "kuttaka", "kā", "mi", "nettika", "nā", "ma", "pa", "anāsava", "pati", "paṭi", "eyya", "sa", "revata", "santi", "satimant", "subhaddā", "bhūma", "niggahita", "nālaka", "sammatta", "seṭṭhi", "ssa", "bhisakka", "sumaṅgala", "susaṇṭhita", "asita", "tara", "teja", "tha", "to", "tā", "tāla", "udāyī", "ukkhittaka", "upanisā", "upanāyika", "upasagga", "uṃ", "vaca", "veyyākaraṇa", "veṇḍu", "vīta", "īya", "ṇaya", "ṇu", "vaṭṭaka", "vibhūta", "vimuttatta", "vāya", "advejjha", "ketuṃ", "sabbato", "selā", "siṅgālaka 1", "kāḷī", "paññāya", "siṅgālaka", "bhaggava", "kalyāṇī", "kalyāṇaṃ", "aṃ", "gayā", "ima", "kukkuka", "madhura", "māgaṇḍiya", "na", "naḷeru", "bhoti", "ra", "hatthaka", "satthi", "sāketa", "soṇḍī", "upadhāna", "vanatha 1", "vaṅganta", "venāga", "visākhā", "atthi", "koṇḍañña", "abhi", "adhi", "ci", "lohitaka", "pari", "saddha", "satī", "ud", "upa", "upakāḷā", "upatissa", "vasī", "sakka", "upakāḷa"
	]
	
	failures = construction_does_not_equal_family2(
		dpd_df, dpd_df_length, exceptions)
	print(f"{white}{len(failures)}")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	txt_file1.write(f"\n{line_break}\nwords in comps not in family2\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nwords in comps not in family2\n{line_break}\n")
	txt_file1.write(f"1. Pāli1 contains x\n2. OR construction contains x\n\n")

	counter = 0
	for word in failures:
		if counter < 10:
			txt_file1.write(f"\\b{word}\\b\n")
		txt_file2.write(f"\\b{word}\\b\n")
		counter += 1
	txt_file1.write(f"10/{counter}")
	txt_file2.write(f"{counter}")

	txt_file1.close()
	txt_file2.close()


def test_base_eqals_construction():
	print(f"{timeis()} {green}test base equals construction")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	test1 = dpd_df['Base'] != ""
	test2 = dpd_df['Construction'] != ""
	test3 = dpd_df['Meaning IN CONTEXT'] != ""
	filter = test1 & test2 & test3
	filter_df = dpd_df.loc[filter]
	filter_df = filter_df.reset_index()
	filter_df_len = len(filter_df)

	allwords = []
	counter = 0

	txt_file1.write(f"\n{line_break}\nbase ≠ construction\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nrbase ≠ construction\n{line_break}\n")

	for row in range(filter_df_len):
		headword = filter_df.loc[row, "Pāli1"]
		base = filter_df.loc[row, "Base"]
		base_clean = re.sub("^.+> ", "", base)
		base_clean = re.sub(" \\(.+$", "", base_clean)
		construction = filter_df.loc[row, "Construction"]
		
		if re.findall(f"(^| ){base_clean} ", construction) == []:
			allwords.append(headword)
			if counter < 10:
				txt_file1.write(f"{headword}. {base_clean}. {construction}\n")
			txt_file2.write(f"{headword}. {base_clean}. {construction}\n")
			counter+=1

	if len(allwords) != 0:
		txt_file1.write(f"{len(allwords)}\n\n")
		txt_file2.write(f"{len(allwords)}\n\n")

		counter = 0
		txt_file1.write(f"^(")
		txt_file2.write(f"^(")
		for word in allwords:
			if word != allwords[-1]:
				if counter < 10:
					txt_file1.write(f"{word}|")
				txt_file2.write(f"{word}|")
			else:
				txt_file1.write(f"{word})$\n\n")
				txt_file2.write(f"{word})$\n\n")
			counter += 1

	txt_file1.close()
	txt_file2.close()


def add_commentary_definitions():
	print(f"{timeis()} {green}add commentary defintions")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	test1 = dpd_df['Meaning IN CONTEXT'] != ""
	test2 = dpd_df['Commentary'] == ""
	test3 = dpd_df['Grammar'].str.contains("\\bcomp\\b")
	filter = test1 & test2 & test3

	filter_df = dpd_df.loc[filter, ["Pāli1", "POS", "Meaning IN CONTEXT"]]
	filter_df_len = len(filter_df)
	x = random.randint(0, filter_df_len-20)
	y = x+10
	filter_df = filter_df.iloc[x:y]
	filter_df.reset_index(inplace=True, drop=True)

	txt_file1.write(f"\n{line_break}\nadd commentary definitions\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nadd commentary definitions\n{line_break}\n")

	allwords = []
	counter = 0
	for row in range(10):
		headword = filter_df.iloc[row, 0]
		pos = filter_df.iloc[row, 1]
		meaning = filter_df.iloc[row, 2]
		allwords.append(headword)

		if counter < 10:
			txt_file1.write(f"{headword}\t{pos}\t{meaning}\n")
		txt_file2.write(f"{headword}\t{pos}\t{meaning}\n")

	write_all_words(allwords, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def family2_no_meaning():
	print(f"{timeis()} {green}family2 no meaning")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	# list of all family2 words
	all_family2 = []

	for row in range(dpd_df_length):
		family2 = dpd_df.loc[row, "Family2"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		
		test1 = family2 != ""
		test2 = meaning != ""

		if test1 and test2:
			all_family2 += (family2.split(" "))

	all_family2 = set(all_family2)
	
	# check family2

	exceptions = []
	needs_meaning = []
	counter = 0
	
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" //d*$", "", headword)
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]

		test1 = headword_clean in all_family2
		test2 = meaning == ""
		test3 = headword not in exceptions

		if test1 and test2 and test3:
			needs_meaning += [headword]

			if len(needs_meaning) == 1:
				txt_file1.write(f"\n{line_break}\nfamily2 no meaning\n{line_break}\n")
				txt_file2.write(f"\n{line_break}\nfamily2 no meaning\n{line_break}\n")
				txt_file1.write(f"1. Pāli1 contains x\n2. OR Family2 contains x\n\n")

			if counter < 5:
				txt_file1.write(f"\\b{headword}\\b\n")
			txt_file2.write(f"\\b{headword}\\b\n")
	
			counter+=1

	txt_file1.write(f"{len(needs_meaning)}\n")
	# write_all_words(needs_meaning, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def family2_is_empty():
	print(f"{timeis()} {green}family2 is empty")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	test1 = dpd_df["Meaning IN CONTEXT"] != ""
	test2 = dpd_df["Construction"].str.contains("\\+")
	test3 = dpd_df["Family2"] == ""
	test4 = dpd_df["Pāli Root"] == ""
	test5 =~dpd_df["Grammar"].str.contains("\\b(nom|acc|instr|dat|abl|gen|loc|voc)\\b")
	test6 = dpd_df["Word Family"] == ""
	test7 = ~dpd_df["Construction"].str.contains("(sikkhāpada|sutta|saṃyutta)\\b")
	test8 = dpd_df["Pāli1"].str.len() < 30
	test9 = ~dpd_df["Grammar"].str.contains("onom")
	test10 = ~dpd_df["Category"].str.contains("names")
	test11 = ~dpd_df["Grammar"].str.contains("\\bcomp\\b")
	test12 = ~dpd_df["Pāli1"].str.contains("māṇavapucchā")

	filter = test1 & test2 & test3 & test4 & test5 & test6 & test7 & test8 & test9 & test10 & test11 & test12

	filter_df = dpd_df[filter]
	filter_df = filter_df.reset_index()

	counter = 0
	all_words = []
	exceptions = ["bhabbhara", "bharabhara", "avadāniya", "bhaggā", "bhārukacchaka", "bumū", "bhesajja", "avadāniya", "bhaggā", "bhārukacchaka", "bhesajja", "bumū", "dhanvana", "avadāniya", "bhaggā", "bhārukacchaka", "bhesajja", "bumū", "dhanvana", "avadāniya", "hisi", "hiti", "icchānaṅkala", "kakaṇṭaka", "issatta", "kā 2", "kāni", "kassa 1", "kassa 2", "kururu", "masāraka", "mattaluṅga", "saṅkasāyati", "sanantana", "setabyaka", "thūlū", "no 1.1", "vāsiṭṭha", "ussaṅkha"
	
	]

	for row in range(len(filter_df)):
		headword = filter_df.loc[row, "Pāli1"]
		construction = filter_df.loc[row, "Construction"]
		meaning = filter_df.loc[row, "Meaning IN CONTEXT"]

		if counter == 0 and \
		headword not in exceptions:
			txt_file1.write(f"\n{line_break}\nfamily2 is empty ({len(filter_df)})\n{line_break}\n")
			txt_file2.write(f"\n{line_break}\nfamily2 is empty ({len(filter_df)})\n{line_break}\n")

		if counter < 10 and \
		headword not in exceptions:
			txt_file1.write(f"{headword}\t{construction}\t{meaning}\n")
			all_words.append(headword)
			counter += 1
		txt_file2.write(f"{headword}\t{construction}\t{meaning}\n")

	write_all_words(all_words, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def find_variants_and_synonyms():
	print(f"{timeis()} {green}finding variants and synonyms")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	txt_file1.write(f"\n{line_break}\nvariants and synonyms\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nvariants and synonyms\n{line_break}\n")

	all_meanigns = {}
	exceptions = [
		"-", "name of a Licchavi layman", "name of a monk", "name of a group of deities", "name of a privately enlightened Buddha", "name of a naked ascetic", "name of a river; one of the five great rivers of ancient India", "name of an ascetic teacher", "name of a Brahman; one of three brothers", "name of a deity", "name of a country", "that; such; so and so", "oh!; oh no!; oh dear!; wow!", "black antelope hide", "modern; related to today", "nothing", "name of a nun", "measure of volume", "name of a heavenly city", "name of a village", "name of a pagoda", "name of a wandering ascetic", "unrelated; not blood-related", "followed; walked after", "compassionate; kind; concerned", "not seeing", "small; insignificant; minute; tiny", "name of a Brahman", "name of one of the three daughters of Death", "name of a king", "enlightened being; fourth stage of the path", "name of a layman", "name of a people", "epithet of a class of devas", "name of a town", "example of a low name", "kind of weapon", "compassionate; kind; concerned", "I am eternal; I am everlasting", "name of an arahant", "name of a holy river", "name of a god", "name of a torture", "name of a Licchavi", "name of a chieftain", "family name", "name of one the seven Bharata kings", "name of a village in Sri Lanka", "name of a prince", "venerable; reverend", "type of couch", "and inclined towards; and wanting; accompanied with lust", "and disinclined; and averse; and accompanied with aversion", "name of a royal family of serpents", "family name; example of a low name", "name of a type of elephant", "name of a young Brahman", "name of a minister", "name of a demigod", "name of a monk", "name of a goddess", "name of a spirit", 'name of a monk', "name of a forest", "name of a Sakyan layman", "name of a householder", "name of an ascetic", "name of a wood", "name of a Brahman woman", "name of an elephant", "name of a lay disciple", 'the word "thus"', "name of an arahant monk", "type of ascetic", "name of a princess"
		]

	# builds a dict of meaning: {pos:[headwords], pos:[headwords]}

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pos = dpd_df.loc[row, "POS"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		meaning_clean = re.sub(" \\(.*?\\)", "", meaning)
		synonyms = dpd_df.loc[row, "Synonyms – different word"]
		variants = dpd_df.loc[row, "Variant – same constr or diff reading"]

		test1 = meaning != ""
		test2 = meaning not in exceptions

		if test1 & test2:
			if meaning_clean not in all_meanigns:
				all_meanigns[meaning_clean] = {
					pos: {'headwords': [headword], 'clean headwords': [headword_clean], 'synonyms': [], 'variants': []}}
			else:
				if pos not in all_meanigns[meaning_clean]:
					all_meanigns[meaning_clean] = {
					pos: {'headwords': [headword], 'clean headwords': [headword_clean], 'synonyms': [], 'variants': []}}
				else:
					all_meanigns[meaning_clean][pos]['headwords'] += [headword]
					all_meanigns[meaning_clean][pos]['clean headwords'] += [headword_clean]
		
			if synonyms != "":
				for synonym in synonyms.split(", "):
					all_meanigns[meaning_clean][pos]['synonyms'] += [synonym]
					
			if variants != "":
				for variant in variants.split(", "):
					all_meanigns[meaning_clean][pos]['variants'] += [variant]

		# structure
		# meaning : pos
		# pos : headwords + synonyms + variants

	counter = 0
	var_counter = 0
	for meaning, poss in all_meanigns.items():
		for pos, i in poss.items():
			headwords = i['headwords']
			clean_headwords = i['clean headwords']
			synonyms = i['synonyms']
			variants = i['variants']
			if len(headwords) > 1:
				if set(clean_headwords) != set(synonyms) and \
					not set(clean_headwords).issubset(set(synonyms)) and \
					set(clean_headwords) != set(variants) and \
					not set(clean_headwords).issubset(set(variants)):
					var_counter += 1
					if counter < 10:
						txt_file1.write(f"{meaning}\n")
						txt_file1.write(f"^(")
						for headword in headwords:
							if headword != headwords[-1]:
								txt_file1.write(f"{headword}|")
							else:
								txt_file1.write(f"{headword})$\n")
						for headword in headwords:
							headword_clean = re.sub(" \\d.*$", "", headword)
							if headword != headwords[-1]:
								txt_file1.write(f"{headword_clean}, ")
							else:
								txt_file1.write(f"{headword_clean}")
						txt_file1.write(f"\n\n")
						counter += 1

	txt_file1.write(f"{var_counter}")
	txt_file1.close()
	txt_file2.close()


def find_word_families():
	print(f"{timeis()} {green}finding word families", end=" ")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	txt_file1.write(f"\n{line_break}\nfind word families\n{line_break}\n")
	txt_file2.write(f"\n{line_break}\nfind word families\n{line_break}\n")

	families_dict = {}
	exceptions = [
	]
    
	# build a dict of family2 with 
	# 1. no comp in gramnar
	# 2. no root: 
	# family2: [headword1, headword2, etc]

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pos = dpd_df.loc[row, "POS"]
		grammar = dpd_df.loc[row, "Grammar"]
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		root = dpd_df.loc[row, "Pāli Root"]
		family2 = dpd_df.loc[row, "Family2"]
		family2_list = family2.split(" ")
		word_family = dpd_df.loc[row, "Word Family"]

		test1 = word_family == ""
		test2 = family2 != ""
		test3 = not re.findall("\\bcomp\\b", grammar)
		test4 = root == ""
		test5 = not any(item in family2_list for item in exceptions)
		test6 = not re.findall("idiom|sandhi", pos)
		# test7 = meaning != ""

		if test1 & test2 & test3 & test4 & test5 & test6: # & test7:
			for fam2item in family2_list:
				if fam2item not in families_dict:
					families_dict[fam2item] = [headword]
				else:
					families_dict[fam2item] += [headword]
	
	print(f"{white}{len(families_dict)}")
	txt_file1.write(f"1. make sure none of these words are compounds\n")
	txt_file1.write(f"2. check words must remain in family2\n")
	txt_file1.write(f"3. only one word in family2 if not a compound\n\n")

	for i in range(5):
		top_word = max(families_dict, key=lambda x: len(families_dict[x]))
		
		txt_file1.write(f"{top_word}\n^(")
		txt_file2.write(f"{top_word}\n^(")
		for item in families_dict[top_word]:
			if item != families_dict[top_word][-1]:
				txt_file1.write(f"{item}|")
				txt_file2.write(f"{item}|")
			else:
				txt_file1.write(f"{item})$\n\n")
				txt_file2.write(f"{item})$\n\n")

		families_dict.pop(top_word)

	txt_file1.write(f"{len(families_dict)}\n\n")
	txt_file2.write(f"{len(families_dict)}\n\n")
	txt_file1.close()
	txt_file2.close()


def find_word_families2():
	print(f"{timeis()} {green}finding more word families")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	txt_file1.write(f"\n{line_break}\nfinding more word families\n{line_break}\nPāli1 contains x\n\n")
	txt_file2.write(f"\n{line_break}\nfinding more word families\n{line_break}\n")

	word_families_set = []
	exceptions = [
		"adhi",
		"ati",
		"anu",
		"pa",
		"saṃ",
		"ka",
		"vi",
		"pati",
		"ta",
		"ya",
		"upa",
		"ti",
		"ava",
		"na",
		"ku",
		"kasāya",
		"dhanvana",
		"dhanu",
		"soṇita",
		"sa",
		"soṇa",
		"ā",
		"saṅkha",
		"ud",

		
	]

	# make a list all word families

	for row in range(dpd_df_length):
		word_families = dpd_df.loc[row, "Word Family"].split()
		for word in word_families:
			word = re.sub(" \\d.*$", "", word)
			word_families_set.append(word)
	word_families_set = set(word_families_set)
	word_families_set = word_families_set - set(exceptions)

	# find all headwords where
	# 1. not compound
	# 2. word familiy is in construction
	# 3. root is empty

	counter = 0

	for word in word_families_set:
		if counter < 1000:
			test1 = ~dpd_df["Grammar"].str.contains("\\bcomp\\b")
			test2 = dpd_df["Construction"].str.contains(f"\\b{word}\\b")
			test3 = dpd_df["Word Family"] == ""
			test4 = dpd_df["Pāli Root"] == ""
			test5 = ~dpd_df["Grammar"].str.contains("\\bdeno\\b")
			test6 = ~dpd_df["POS"].str.contains("sandhi|idiom")

			filter = test1 & test2 & test3 & test4 & test5 & test6
			filtered_df = dpd_df[filter]
			headwords = filtered_df["Pāli1"].to_list()

			if len(headwords) != 0:
				txt_file1.write(f"{word}\n")
				txt_file1.write(f"\\b{word}\\b|^(")
				for headword in headwords:
					if headword != headwords[-1]:
						txt_file1.write(f"{headword}|")
					else:
						txt_file1.write(f"{headword})$\n\n")
				counter+= 1

	txt_file1.close()
	txt_file2.close()


def find_word_families3():
	print(f"{timeis()} {green}finding more word families, the word itself")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	txt_file1.write(
		f"\n{line_break}\nfinding more word families, the word itself\n{line_break}\n")
	txt_file2.write(
		f"\n{line_break}\nfinding more word families, the word itself\n{line_break}\n")

	word_families_set = []
	exceptions = []

	# make a list all word families
	
	for row in range(dpd_df_length):
		word_families = dpd_df.loc[row, "Word Family"].split()
		for word in word_families:
			word = re.sub(" \\d.*$", "", word)
			word_families_set.append(word)
	word_families_set = set(word_families_set)
	word_families_set = word_families_set - set(exceptions)

	# check if headword has word family
	missing_word_family = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		word_family = dpd_df.loc[row, "Word Family"]

		test1 = headword_clean in word_families_set
		test2 = word_family == ""

		if test1 & test2:
			missing_word_family += [headword]
	
	if len(missing_word_family) >= 10:
		length = 10
	else:
		length = len(missing_word_family)

	counter = 0
	if len(missing_word_family) > 0:
		txt_file1.write(f"\\b(")
	for word in missing_word_family:
		if counter < length:
			txt_file1.write(f"{word}|")
			counter += 1
		if counter == length:
			txt_file1.write(f"{word})\\b")
			counter += 1
	txt_file1.write(f"\n")

	txt_file1.close()
	txt_file2.close()


def find_word_family_loners():
	print(f"{timeis()} {green}finding word family loners")
	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	wf_dict = {}

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		word_family = dpd_df.loc[row, "Word Family"]
		word_family = re.sub("(.+ )(.[^ ]*$)", "\\2", word_family)
		if word_family != "":
			if word_family not in wf_dict:
				wf_dict[word_family] = 1
			else:
				wf_dict[word_family] += 1
	
	counter = 0
	all_words = []

	for word in wf_dict:
		if wf_dict[word] == 1:
			all_words += [word]
			if counter == 0:
				txt_file1.write(f"\n{line}\nword family loners\n{line}\n")
				txt_file2.write(f"\n{line}\nword family loners\n{line}\n")
			txt_file1.write(f"{word}\n")
			txt_file2.write(f"{word}\n")
			counter+=1
	
	write_all_words(all_words, txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()


def allowable_prefixes():
	print(f"{timeis()} {green}finding irregular prefixes")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	allowable_prefixes = ['abhi', 'adhi', 'anu', 'apa', 'api', 'ati', 'ava', 'ni', 'nī', 'pa', 'pari', 'parā', 'pati', 'prati', 'sad', 'saṃ', 'ud', 'upa', 'vi', 'ā', "√"]
	errors = {}

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		family = dpd_df.loc[row, "Family"]
		family_split = family.split()
		for split in family_split:
			t1 = not "√" in split
			t2 = split not in allowable_prefixes
			if t1 & t2:
				errors[headword] = family

	all_words = []
	
	if len(errors) > 0:
		txt_file1.write(
		f"\n{line_break}\nroot family prefix errors\n{line_break}\n")
		txt_file2.write(
		f"\n{line_break}\nroot family prefix errors\n{line_break}\n")
		for headword, family in errors.items():
			txt_file1.write(f"{headword}\t{family}\n")
			txt_file2.write(f"{headword}\t{family}\n")
			all_words.append(headword)

	write_all_words(all_words, txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()


def words_in_family2_not_in_dpd():
	print(f"{timeis()} {green}finding words in family2 not in dpd headwords")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')
	
	all_headwords = set()
	all_family2 = set()
	errors = set()
	exceptions = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		all_headwords.add(headword_clean)
		
		family2 = dpd_df.loc[row, "Family2"]
		family2_split = family2.split()
		
		for split in family2_split:
			split_clean = re.sub("\\d.*$", "", split)
			all_family2.add(split_clean)

	for word in all_family2:
		if word not in all_headwords:
			if word not in exceptions:
				errors.add(word)
	
	all_words = []
	counter = 0

	if len(errors) > 0:
		txt_file1.write(
            f"\n{line_break}\nfamily2 not in dpd\n{line_break}\n")
		txt_file2.write(
            f"\n{line_break}\nfamily2 not in dpd\n{line_break}\n")
	for word in errors:
		if counter < 10:
			txt_file1.write(f"\\b({word})\\b\n")
		txt_file2.write(f"\\b({word})\\b\n")
		all_words.append(word)
		counter +=1

	txt_file1.write(f"{len(errors)}")
	txt_file2.write(f"{len(errors)}")
	write_all_words(list(errors), txt_file1, txt_file2)

	txt_file1.close()
	txt_file2.close()


def find_fem_abstr_comps():
	print(f"{timeis()} {green}finding fem abstr comps")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	# make a list of all fem abstr words that are not compounds
	# look for compounds ending in those words

	all_fem_abstr = set()

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pos = dpd_df.loc[row, "POS"]
		grammar = dpd_df.loc[row, "Grammar"]

		test1 = pos == "fem"
		test2 = not re.findall("\\bcomp\\b", grammar)
		test3 = bool(re.findall("\\babstr\\b", grammar))

		if test1 & test2 & test3:
			all_fem_abstr.add(headword_clean)
	
	counter = 0
	all_words = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		grammar  = dpd_df.loc[row, "Grammar"]
		pos = dpd_df.loc[row, "POS"]
		construction = dpd_df.loc[row, "Construction"]
		construction = re.sub("\\n.+", "", construction)
		construction_last_word = re.sub("(.+) (.[^ ]*$)", "\\2", construction)

		test1 = construction_last_word in all_fem_abstr
		test2 = pos == "fem"
		test3 = not re.findall("\\babstr\\b", grammar)

		if test1:
			if test2:
				if test3:
					if counter == 0:
						txt_file1.write(f"\n{line_break}\nfem abstr compounds\n{line_break}\n")
						txt_file2.write(f"\n{line_break}\nfem abstr compounds\n{line_break}\n")
					if counter < 20:
						txt_file1.write(f"{headword}\t{construction_last_word}\n")
						all_words.append(headword)
					txt_file2.write(f"{headword}\t{construction_last_word}\n")
					counter += 1
		
	txt_file1.write(f"{counter}\n")
	txt_file2.write(f"{counter}\n")
	write_all_words(all_words, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()


def find_nt_abstr_comps():
	print(f"{timeis()} {green}finding nt abstr comps")

	txt_file1 = open("output/test_results.txt", 'a')
	txt_file2 = open("output/test_results_all.txt", 'a')

	# make a list of all nt abstr words that are not compounds
	# look for compounds ending in those words

	all_nt_abstr = set()

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		headword_clean = re.sub(" \\d.*$", "", headword)
		pos = dpd_df.loc[row, "POS"]
		grammar = dpd_df.loc[row, "Grammar"]

		test1 = pos == "nt"
		test2 = not re.findall("\\bcomp\\b", grammar)
		test3 = bool(re.findall("\\babstr\\b", grammar))

		if test1 & test2 & test3:
			all_nt_abstr.add(headword_clean)

	counter = 0
	all_words = []

	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		grammar = dpd_df.loc[row, "Grammar"]
		pos = dpd_df.loc[row, "POS"]
		construction = dpd_df.loc[row, "Construction"]
		construction = re.sub("\\n.+", "", construction)
		construction_last_word = re.sub("(.+) (.[^ ]*$)", "\\2", construction)

		test1 = construction_last_word in all_nt_abstr
		test2 = pos == "nt"
		test3 = not re.findall("\\babstr\\b", grammar)

		if test1:
			if test2:
				if test3:
					if counter == 0:
						txt_file1.write(f"\n{line_break}\nnt abstr compounds\n{line_break}\n")
						txt_file2.write(f"\n{line_break}\nnt abstr compounds\n{line_break}\n")
					if counter < 20:
						txt_file1.write(f"{headword}\t{construction_last_word}\n")
						all_words.append(headword)
					txt_file2.write(f"{headword}\t{construction_last_word}\n")
					counter += 1

	txt_file1.write(f"{counter}\n")
	txt_file2.write(f"{counter}\n")
	write_all_words(all_words, txt_file1, txt_file2)
	txt_file1.close()
	txt_file2.close()

def renumbering():
	print(f"{timeis()} {green}renumbering", end=" ")

	# go through all the words with numbers
	# print them all out hw, pos, meaning, buddhadatta
	# break into groups according to root / word family
	# once done hit enter to continue
	# save done in a json
	# escape to exit

	numbered = set()
	renumbered = set()

	try:
		with open("output/renumbered", "rb") as f:
			renumbered = pickle.load(f)
	except:
		with open("output/renumbered", "wb") as f:
			pickle.dump(renumbered, f)
			
	for row in range(dpd_df_length):
		headword = dpd_df.loc[row, "Pāli1"]
		pos = dpd_df.loc[row, "POS"]
		if re.findall("\\d", headword):
			if pos != "idiom" and pos != "sandhi":
				headword_clean = re.sub(" \\d.*$", "", headword)
				numbered.add(headword_clean)
	
	print(f"{white}{len(numbered)}")
	
	for word in renumbered:
		numbered.discard(word)

	# test if all the words in that df have the same root / root meaning / word family / construction
	# if not only then display

	for word in numbered.copy():

		test1 = dpd_df["Pāli1"].str.contains(f"\\b{word}\\b")
		test2 = dpd_df["POS"] != "sandhi"
		test3 = dpd_df["POS"] != "idiom"
		test4 = ~dpd_df["Pāli1"].str.contains(f"\\.\\d")
		filtered = test1 & test2 & test3 & test4
		df_filtered = dpd_df[filtered]
		print(df_filtered)

		test1 = df_filtered["Family"].nunique()
		test2 = df_filtered["Root Meaning"].nunique()
		test3 = df_filtered["Word Family"].nunique()
		test4 = df_filtered["Construction"].nunique()

		if test1 == 1 and test2 ==1 and test3 == 1 and test4 == 1:
			renumbered.add(word)
			numbered.discard(word)
		else:	
			inputter = input(
				f"{timeis()} {white}{word}\t{len(numbered)}\t{green}\\b{word}\\b\t{test1}{test2}{test3}{test4} ")
			if inputter == "x" or inputter == "exit":
				break
			else:
				renumbered.add(word)
				numbered.discard(word)

	with open("output/renumbered", "wb") as f:
		print(f"{timeis()} {green}saving renumbered to pickle {white}{len(renumbered)}")
		pickle.dump(renumbered, f)
	

tic()
make_new_dpd_csv()
setup_dfs()
tests_data_integrity_tests()
generate_test_results()
duplicate_meanings()
test_headword_in_inflections()
test_suffix_matches_pāli1()
test_construction_line1_matches_pāli1()
test_construction_line2_matches_pāli1()
missing_number()
extra_number()
pali_words_in_english_meaning()
run_test_formulas()
test_derived_from_in_family2()
pos_does_not_equal_gram()
pos_does_not_equal_pattern()
bases_contains_star()
bases_needs_star()
root_family_mismatch()
root_construction_mismatch()
family_construction_mismatch()
root_base_mismatch()
root_sign_base_mismatch()
complete_root_matrix()
random_words()
# add_family2()
test_family2(dpd_df, dpd_df_length)
family2_no_meaning()
family2_is_empty()
words_in_family2_not_in_dpd()
test_base_eqals_construction()
add_commentary_definitions()
find_variants_and_synonyms()
find_word_families()
find_word_families2()
find_word_families3()
find_word_family_loners()
allowable_prefixes()
find_fem_abstr_comps()
find_nt_abstr_comps()
print_columns()
open_test_results()
toc()

renumbering()

# test_words_construction_are_headwords()
# reset_lastrun()
# derivatives_in_compounds()
# identical_meanings()
# derived_from_in_headwords()

# ¹²³⁴⁵⁶⁷⁸⁹⁰·
