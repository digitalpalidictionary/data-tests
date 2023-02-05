
import pandas as pd
from timeis import *
import re

def setup_dpd_df():
	print(f"{timeis()} {green}importing dpd.csv", end=" ")
	dpd_df = pd.read_csv("../csvs/dpd.csv", sep="\t", dtype=str, skip_blank_lines=False)
	dpd_df.fillna("", inplace=True)
	dpd_df['Pāli3'] = dpd_df['Pāli1'].str.replace(" \\d.*$", "", regex=True)
	dpd_df_length = len(dpd_df)
	print(f"{white}{dpd_df.shape}")
	return dpd_df

dpd_df = setup_dpd_df()

def make_list_of_words_from_clean_roots():
	print(f"{timeis()} {green}making list of words from clean roots", end=" ")

	test1 = dpd_df["Pāli Root"] != ""
	test2 = dpd_df["Family"].str.contains("^√")
	filter = test1 & test2
	test3 = ~dpd_df["Neg"].eq("neg")
	filter = test1 & test2 & test3


	dpd_filtered = dpd_df[filter]
	words_clean_roots = dpd_filtered["Pāli3"].tolist()
	words_clean_roots = sorted(set(words_clean_roots))

	print(f"{white}{len(words_clean_roots)}")
	return words_clean_roots

words_clean_roots = make_list_of_words_from_clean_roots()

def find_missing_roots1():
	exceptions = ["pabbata 1", "pabbata 2", "pabbata 3",
               "athabbaṇa", "āthabbaṇa", "ataccha 1", "ataccha 2", "kaccha 1", "kaccha 2", "kaccha 3", "maccha", "paccha", "taccha 1", "taccha 2", "taraccha", "tiraccha", "tiraccha", "vaccha 1", "vaccha 2", "vaccha 3", "pacchi", "kacci", "agada", "avisārada", "aññadā", "ekadā 1", "ekadā 2", "gadā", "kadā", "kokanadā", "sabbadā", "sadā", "tadā", "yadā"]
	txtfile = open("output/missingword.tsv", "w")
	counter=0
	for word in words_clean_roots:
		if counter < 200:
		# if counter %100==0:
		# 	print(f"{timeis()} {counter} / {len(words_clean_roots)} {word}")

			test1 = dpd_df["Pāli1"].str.contains(f"{word}( |$)")
			test2 = dpd_df["Pāli3"] != word
			test3 = ~dpd_df["Grammar"].str.contains(r"\bcomp\b")
			test4 = dpd_df["Pāli Root"] == ""
			test5 = ~dpd_df["Pāli1"].isin(exceptions)
			test6 = ~dpd_df["POS"].str.contains("idiom")
			test7 = ~dpd_df["Grammar"].str.contains("deno")
			test8 = ~dpd_df["Verb"].str.contains("deno")
			filter = test1 & test2 & test3 & test4 & test5 & test6 & test7 & test8
			dpd_filtered = dpd_df[filter]
			candidates = dpd_filtered["Pāli1"].tolist()
			
			if candidates != []:
				txtfile.write(f"{word}\t^(")
				for candidate in candidates:
					if candidate == candidates[0]:
						txtfile.write(f"{candidate}")
					else:
						txtfile.write(f"|{candidate}")
				txtfile.write(")$\n")
				counter+=1
	txtfile.close()

find_missing_roots1()





