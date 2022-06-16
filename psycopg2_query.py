import psycopg2


def print_query(sql):
    conn = psycopg2.connect(dsn="dbname='az_tempe_publicsafety' host='localhost' user='USER' password='PASSWORD'")
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
    print_query("""
    SELECT * 
    FROM 
        calls_for_service_csv 
    WHERE
        cfstype ilike '%OFFENSE%' 
        AND place_name like '%SOMEHWERE%' 
    ORDER BY 
        occ_year DESC, occ_dt DESC
    LIMIT 
        1000
    """)
