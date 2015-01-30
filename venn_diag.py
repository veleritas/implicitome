import sys
sys.path.append("/home/toby/grader/preprocess/")
sys.path.append("/home/toby/useful_util/")
sys.path.append("/home/toby/implicitome/")

import mysql.connector
from useful import const
import time
from collections import defaultdict

import util
import convert
import re

from useful.read_ids import read_ids
from useful.get_implicitome_tuples import get_raw_tuples



def load_morbidmap():
	dmim_cuis = dict()
	gene_ids = dict()

	file_loc = "/home/toby/semmed/data/converted_morbidmap.txt"
	with open(file_loc, "r") as file:
		cur_dmim = "";
		for i, line in enumerate(file):
			line = line.rstrip('\n')
			line = line.lstrip('\t')

			# skip the dmims with no cuis...
			# since there's no way to look it up anyways
			if i % 2 == 0: #dmim
				vals = line.split('|')
				if len(vals) == 1: # no cuis
					cur_dmim = ""
					continue
				else:
					cur_dmim = vals[0]
					dmim_cuis[cur_dmim] = vals[1:]
			else: #gene ids
				if not cur_dmim: # previous line had no cuis
					continue
				else:
					gene_ids[cur_dmim] = line.split('|')

	return (dmim_cuis, gene_ids)

def get_semmed_tuples():
	cnx = mysql.connector.connect(database = "semmeddb",
		**const.DB_INFO)

	if cnx.is_connected():
		cur = cnx.cursor()

		query = ("SELECT DISTINCT PID, SID, PMID, s_cui, s_name, predicate, o_cui, o_name "
			"FROM PREDICATION_AGGREGATE "
			"WHERE s_type IN ('gngm', 'aapp') "
			"AND o_type IN ('dsyn', 'neop', 'cgab', 'mobd');")

		cur.execute(query)
		with open("semmed_fps.txt", "w") as out:
			for row in cur:
				out.write("{0}#{1}#{2}#{3}#{4}#{5}#{6}#{7}\n".format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))

		cur.close()

	cnx.close()

def get_cached_info():
	semmed_tuples = set()
	name = dict()
	identifiers = defaultdict(list)

	for line in util.read_file("semmed_fps.txt", "/home/toby/semmed/"):
		vals = line.split('#')

#		o_cui are all C1234567 (no gene ids)
		sub_ids = vals[3].split('|')
		sub_names = vals[4].split('|')

		semmed_tuples |= set([(val, vals[6]) for val in sub_ids])

		for val in sub_ids:
			identifiers[(val, vals[6])].append((vals[5], vals[0], vals[1], vals[2]))

		name[vals[6]] = vals[7] # object
		for sub, s_name in zip(sub_ids, sub_names):
			name[sub] = s_name


	return (semmed_tuples, name, identifiers)

def get_name(cui):
	uid = convert.cui_to_uid(cui)

	req = "esummary.fcgi?db=medgen&id=" + uid
	xml = convert.query_ncbi(req)

	res = re.findall(r'<Title>.+</Title>', xml)
	print res
	assert len(res) == 1, ">1 name {0}".format(cui)
	return res[0][7:-8]



def main():
	dmim_cuis, gene_ids = load_morbidmap()

	all_omim_cuis = set()
	for dmim, cuis in dmim_cuis.items():
		all_omim_cuis |= set(cuis)

#	format: (geneID, CUI)
	omim_tuples = set() # 8456 items
	for dmim, cuis in dmim_cuis.items():
		omim_tuples |= set([(gid, cui) for cui in cuis for gid in gene_ids[dmim]])

	semmed_tuples, name, semmed_info = get_cached_info()

#	format: (geneID or CUI, CUI)
	filtered_semmed_tuples = set([pair for pair in semmed_tuples if pair[1] in all_omim_cuis])

	print "filtered semmed", len(filtered_semmed_tuples)
	print "intersect", len(filtered_semmed_tuples & omim_tuples)


	sem_only = filtered_semmed_tuples - omim_tuples

	gid_sem_only = set([pair for pair in sem_only if pair[0][0] != 'C'])




	print "gid sem", len(gid_sem_only)


	max_tuple_id = "10000"
	tuples = get_raw_tuples(max_tuple_id)

	sub_ids, obj_ids = read_ids()
	implicitome_tuples = set()
	for raw_tuple in tuples:
		sub_id = str(raw_tuple[1])
		obj_id = str(raw_tuple[2])

		if "EG" in sub_ids[sub_id] and "UMLS" in obj_ids[obj_id]:
			implicitome_tuples |= set([(gid, cui) for gid in sub_ids[sub_id]["EG"] for cui in obj_ids[obj_id]["UMLS"]])

	print "super intersect", len(implicitome_tuples & omim_tuples & filtered_semmed_tuples)


	print "semmed & implicitome", len(implicitome_tuples & filtered_semmed_tuples)
	print "omim & implicitome", len(implicitome_tuples & omim_tuples)
	print "semmed & omim", len(omim_tuples & filtered_semmed_tuples)

	print "semmed", len(filtered_semmed_tuples)
	print "implicitome", len(implicitome_tuples)
	print "omim", len(omim_tuples)





if __name__ == "__main__":
	main()
	print "done"
