import mysql.connector
from useful import const

import time

def work():
	cnx = mysql.connector.connect(database = "implicitome",
		**const.DB_INFO)

	if cnx.is_connected():
		cur = cnx.cursor()

#		query = ("SELECT uniq_table.obj_id, dblink.dbid, dblink.id "
#			"FROM (SELECT DISTINCT obj_id FROM tuples) "
#			"AS uniq_table "
#			"LEFT JOIN dblink "
#			"ON uniq_table.obj_id = dblink.conceptid "
#			"WHERE dblink.dbid IN ('OM', 'UMLS');")


		query = ("SELECT uniq_table.sub_id, dblink.dbid, dblink.id "
			"FROM (SELECT DISTINCT sub_id FROM tuples) "
			"AS uniq_table "
			"LEFT JOIN dblink "
			"ON uniq_table.sub_id = dblink.conceptid "
			"WHERE dblink.dbid IN ('OM', 'EG');")

		cur.execute(query)

		with open("uniq_sub_id_converted.txt", "w") as out:
			out.write("sub_id|dbid|id\n")
			for row in cur:
				out.write("{0}|{1}|{2}\n".format(row[0], row[1], row[2]))

		cur.close()
	cnx.close()

def main():
	print "working"
	start_time = time.time()
	work()
	stop_time = time.time()

	print "the program took {0} seconds to run".format(stop_time - start_time)

if __name__ == "__main__":
	main()
	print "done"
