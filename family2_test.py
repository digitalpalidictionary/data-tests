#!/usr/bin/env python3.10
# coding: utf-8

import pandas as pd
import pickle
import re

from timeis import timeis, yellow, line, green, red, white, blue, tic, toc


def get_dpd_csv():
	"""open dpd csv as pandas df"""
	print(f"{timeis()} {green}opening dpd csv", end=" ")

	dpd_df = pd.read_csv("../csvs/dpd-full.csv", sep="\t", dtype = "str")
	dpd_df = dpd_df.fillna("")
	dpd_df_len = len(dpd_df)

	print(f"{white}{len(dpd_df)}")
	return dpd_df, dpd_df_len


def generate_lists(dpd_df, dpd_df_len, exceptions):
	"""make a list of all compound words with meanings"""
	if __name__ == "__main__":
		print(f"{timeis()} {green}making list of all compound words", end=" ")

	all_words_in_compounds = []
	all_family2 = []
	all_clean_headwords = []
	all_empty_family2 = [] 
	all_compounds_words = []
	
	for row in range(dpd_df_len):
		pali = dpd_df.loc[row, "Pāli1"]
		pali_clean = re.sub (r" \d*$", "", pali)
		grammar = dpd_df.loc[row, "Grammar"]
		construction = dpd_df.loc[row, "Construction"]
		construction = re.sub("<br\\/>.+", "", construction)  # strip line2
		if re.findall(">", construction):
			construction = re.sub(" >.+?( \\+)", "\\1", construction) # remove > ... +
		if re.findall("\\[", construction):
			construction = re.sub(" \\+ \\[.+?( \\+)", "\\1", construction)  # remove [] ... +
			construction = re.sub("^\\[.+?( \\+ )", "", construction)  # remove [] at beginning
		if re.findall("\\?", construction):
			construction = re.sub("\\?\\? ", "", construction)  # remove ??
		meaning = dpd_df.loc[row, "Meaning IN CONTEXT"]
		family2 = dpd_df.loc[row, "Family2"]

		# list of all words in construction

		test1 = meaning != ""
		test2 = re.findall("\\bcomp\\b", grammar) != []
		test3 = re.findall("\\+", construction) != []
		test4 = family2 != ""

		if test1 and test2 and test3 and test4:
			all_words_in_compounds += construction.split(" + ")

		# list of all words in fam2

		test1 = meaning != ""
		test2 = family2 != ""

		if test1 and test2:
			all_family2 += family2.split(" ")

		# list of all clean headwords

		test1 = meaning != ""

		if test1 :
			all_clean_headwords += [pali_clean]

		# list of all empty family2

		test1 = meaning != ""
		test2 = family2 == ""

		if test1 and test2:
			all_empty_family2 += [pali_clean]

		# list of all compounds

		test1 = meaning != ""
		test2 = re.findall("\\bcomp\\b", grammar)

		if test1 and test2:
			all_compounds_words += [pali_clean]

	all_words_in_compounds = sorted(set(all_words_in_compounds))
	all_family2 = sorted(set(all_family2))
	all_clean_headwords = sorted(set(all_clean_headwords))
	all_empty_family2 = sorted(set(all_empty_family2))
	all_compounds_words = sorted(set(all_compounds_words))

	if __name__ == "__main__":
		print(f"{white}ok")
	return all_words_in_compounds, all_family2, all_clean_headwords, all_empty_family2, all_compounds_words


def test_it(all_words_in_compounds, all_family2, all_clean_headwords, all_empty_family2, all_compounds_words, exceptions):
	"""testing all words to see if conditions are met"""
	if __name__ == "__main__":
		print(f"{timeis()} {green}testing for failures", end= "" )

	# for each individual words in compound
	# not in fam2 of compound
	# exists as a clean_headwords
	# clean headword fam2 is empty

	# need
	# list of all words in constructions
	# list of all words in family 2
	# list of all clean_headwords
	# list of all words with empty fam2

	failures = []
	for word in all_empty_family2:
		if word in all_words_in_compounds and \
		word not in all_family2 and \
		word in all_clean_headwords and \
		word not in all_compounds_words and \
		word not in exceptions:
			failures.append(word)

	# for word in failures: print(word, end = " ")
	if __name__ == "__main__":
		print(f"{white}{len(failures)}")
	return failures


def construction_does_not_equal_family2(dpd_df, dpd_df_length, exceptions):
	"""testing if words in construction can be found in family2"""
	all_words_in_compounds, all_family2, all_clean_headwords, all_empty_family2, all_compounds_words = generate_lists(
		dpd_df, dpd_df_length, exceptions)
	failures = test_it(all_words_in_compounds, all_family2,
	        all_clean_headwords, all_empty_family2, all_compounds_words, exceptions)
	return failures


if __name__ == "__main__":
	print(f"{timeis()} {line}")
	print(f"{timeis()} {yellow}construction does not equal family2 ")
	print(f"{timeis()} {line}")

	tic()
	exceptions = ""
	dpd_df, dpd_df_len = get_dpd_csv()
	all_words_in_compounds, all_family2, all_clean_headwords, all_empty_family2, all_compounds_words = generate_lists(
		dpd_df, dpd_df_len, exceptions)
	test_it(all_words_in_compounds, all_family2,
	        all_clean_headwords, all_empty_family2, all_compounds_words, exceptions)
	toc()









