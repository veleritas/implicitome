from useful.read_ids import read_ids
from useful.get_implicitome_tuples import get_raw_tuples

import sys
sys.path.append("/home/toby/grader/preprocess/")
from parse_morbidmap import parse_morbidmap

def main():
	sub_ids, obj_ids = read_ids()

	max_tuple_id = "1500000"
	tuples = get_raw_tuples(max_tuple_id)

	genes = parse_morbidmap()

#	5006 unique (gmim, dmim) tuples
	omim_tuples = set()
	for dmim, gmims in genes.items():
		omim_tuples |= set([(gmim, dmim) for gmim in gmims])

	print "everything loaded correctly!"

	implicitome_tuples = set()
	for raw_tuple in tuples:
		sub_id = str(raw_tuple[1])
		obj_id = str(raw_tuple[2])

		if "OM" in sub_ids[sub_id] and "OM" in obj_ids[obj_id]:
			gmims = sub_ids[sub_id]["OM"]
			dmims = obj_ids[obj_id]["OM"]
			implicitome_tuples |= set([(gmim, dmim) for gmim in gmims for dmim in dmims])

	print "range", max_tuple_id
	print "size implicitome tuples", len(implicitome_tuples)

	print "intersect", len(omim_tuples & implicitome_tuples)


if __name__ == "__main__":
	main()
	print "done"
