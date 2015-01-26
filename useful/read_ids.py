# last updated 2015-01-26 toby
import sys
sys.path.append("/home/toby/useful_util/")
import util

def read_ids():
	sub_ids = dict()
	for line in util.read_file("/home/toby/implicitome/", "uniq_sub_id_converted.txt"):
		sub_id, id_type, id_val = line.split('|')

		if sub_id not in sub_ids:
			sub_ids[sub_id] = {}

		if id_type not in sub_ids[sub_id]:
			sub_ids[sub_id][id_type] = set()

		sub_ids[sub_id][id_type].add(id_val)

	obj_ids = dict()
	for line in util.read_file("/home/toby/implicitome/", "uniq_obj_id_converted.txt"):
		obj_id, id_type, id_val = line.split('|')

		if obj_id not in obj_ids:
			obj_ids[obj_id] = {}

		if id_type not in obj_ids[obj_id]:
			obj_ids[obj_id][id_type] = set()

		obj_ids[obj_id][id_type].add(id_val)

	return (sub_ids, obj_ids)
