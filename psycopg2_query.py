import os
# from typing import Tuple
import psycopg2


def print_query(sql):
    pghost = os.getenv('PG_HOSTNAME')
    pguser = os.getenv('PG_USER')
    pgpass = os.getenv('PG_PASSWD')
    pgdb = 'az_tempe_publicsafety'  # os.getenv('PG_DATABASE')
    pgdsn = "dbname='%s' host='%s' user='%s' password='%s'" % (pgdb, pghost, pguser, pgpass)
    conn = psycopg2.connect(dsn=pgdsn)
    cur = conn.cursor()
    cur.execute(sql)
    colnames = [desc[0] for desc in cur.description]
    while True:
        record = cur.fetchone()
        if record is None:
            break
        record_dict = dict(zip(colnames, record))
        print(record_dict)

    print("\n")
    print("rows returned: ", cur.rowcount)
    cur.close()
    conn.close()


if __name__ == '__main__':
    # print_query("SELECT call_count, TRIM(place_name) as place_name, cfstype FROM vw_cfs_cfstypes_counts WHERE cfstype
    #  ilike '%THEFT%' AND place_name like '%TARGET%' ORDER BY call_count DESC LIMIT 10")
    query_args_tuple = ('%THEFT%', '%TARGET%')
    print_query("""
    SELECT * 
    FROM 
        calls_for_service_csv 
    WHERE
        cfstype ilike '%s' 
        AND place_name like '%s' 
    ORDER BY 
        occ_year DESC, occ_dt DESC
    LIMIT 
        1000
    """ % query_args_tuple)
