import pandas as pd
from timeis import *
import re

def setup_dpd_df():
	print(f"{timeis()} {yellow}testing root formulas")
	print(f"{timeis()} {line}")
	
	input (f"{timeis()} {white}please save a copy of dpd.ods to csv with formulas {blue} ")
	print(f"{timeis()} {green}setting up dpd formulas dataframe")
	global df

	df = pd.read_csv("../csvs/dpd-formulas.csv", sep = "\t", dtype = str, header=None)
	df.fillna("", inplace=True)
	# df = df.drop(index=0) #removing first row
	new_header = df.loc[1] #grab the first row for the header
	df = df[2:] #take the data less the header row
	df.columns = new_header #set the header row as the df header
	df.reset_index(inplace = True, drop=True)


def test_formulas():
	print(f"{timeis()} {green}testing root formulas")
	length = len(df)
	for row in range(length):
		headword = df.loc[row, "Pāli1"]
		sk_root = df.loc[row, "Sk Root"]
		sk_root_mn= df.loc[row, "Sk Root Mn"]
		sk_root_cl = df.loc[row, "Cl"]
		pali_root = df.loc[row, "Pāli Root"]
		pali_root_incomps = df.loc[row, "Root In Comps"]
		pali_root_v = df.loc[row, "V"]
		pali_root_grp = df.loc[row, "Grp"]
		pali_root_mn = df.loc[row, "Root Meaning"]

		sk_root = re.sub(fr"\=\$Roots\.\$J\$", "", sk_root)
		sk_root_mn= re.sub(fr"\=\$Roots\.\$K\$", "", sk_root_mn)
		sk_root_cl = re.sub(fr"\=\$Roots\.\$L\$", "", sk_root_cl)
		pali_root = re.sub(fr"\=\$Roots\.\$C\$", "", pali_root)
		pali_root_incomps = re.sub(fr"\=\$Roots\.\$D\$", "", pali_root_incomps)
		pali_root_v = re.sub(fr"\=\$Roots\.\$E\$", "", pali_root_v)
		pali_root_grp = re.sub(fr"\=\$Roots\.\$F\$", "", pali_root_grp)
		pali_root_mn = re.sub(fr"\=\$Roots\.\$I\$", "", pali_root_mn)

		if row%5000 == 0:
			print(f"{timeis()} {row}/{length}\t{headword}")
		
		if sk_root_mn != "" and \
		(pali_root != sk_root or \
		pali_root != sk_root_mn or \
		pali_root != sk_root_cl or \
		pali_root != pali_root_incomps or \
		pali_root != pali_root_v or \
		pali_root != pali_root_grp or \
		pali_root != pali_root_mn):
			print(f"{timeis()} {red}{row}/{length}\t{headword} error")
			print(f"{timeis()} {red}{row}/{length}\t{sk_root} {sk_root_mn} {sk_root_cl} {pali_root} {pali_root_incomps} {pali_root_v} {pali_root_grp} {pali_root_mn}")

# setup_dpd_df()
# test_formulas()