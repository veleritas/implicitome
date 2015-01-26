# last updated 2015-01-26 toby
import mysql.connector
import const

import sys
sys.path.append("/home/toby/useful_util/")
import util

import os

def get_db_raw_tuples(tuple_range):
#	returns all tuples with min_percentile <= percentile <= max percentile
#	also returns the tuple_id for easy indexing

	cnx = mysql.connector.connect(database = "implicitome",
		**const.DB_INFO)

	if cnx.is_connected():
		cur = cnx.cursor()

		query = ("SELECT tuple_id, sub_id, obj_id FROM tuples "
			"WHERE percentile BETWEEN %s AND %s;")

		cur.execute(query, (tuple_range[0], tuple_range[1]))

		raw_tuples = set()
		for row in cur:
			raw_tuples.add((row[0], row[1], row[2]))

		cur.close()

	cnx.close()
	return raw_tuples

def cache_filename(tuple_range):
	cache_loc = "/home/toby/implicitome/data/"
	return cache_loc + str(tuple_range[0]) + "_" + str(tuple_range[1]) + ".txt"

def get_tuples(tuple_range):
	fname = cache_filename(tuple_range)
	if os.path.exists(fname):
		print "reading from cache"
		raw_tuples = set()
		for line in util.read_file(fname):
			tuple_id, sub_id, obj_id = line.split("|")
			raw_tuples.add((tuple_id, sub_id, obj_id))

		return raw_tuples

#	otherwise query, cache, and return
	print "caching"
	raw_tuples = get_db_raw_tuples(tuple_range)
	with open(fname, "w") as out:
		for raw_tuple in raw_tuples:
			out.write("{0}|{1}|{2}\n".format(raw_tuple[0], raw_tuple[1], raw_tuple[2]))

	return raw_tuples

def main():
	print "getting tuples"
	tuples = get_tuples((99.99, 100))
	print len(tuples)
	print "done"

if __name__ == "__main__":
	main()
