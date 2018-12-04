import logging
from datetime import datetime
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

KEYSPACE = "testkeyspace"

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        log.info("dropping existing keyspace...")
        session.execute("DROP KEYSPACE " + KEYSPACE)

    log.info("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    log.info("KEYSPACE")
    session.set_keyspace(KEYSPACE)

    log.info("TABLE:")
    session.execute("""
        CREATE TABLE mytable (
            primkey text,
            column1 text,
            column2 timestamp,
            PRIMARY KEY (primkey, column1)
        )
        """)

    query = SimpleStatement("""
        INSERT INTO mytable (primkey, column1, column2)
        VALUES (%(key)s, %(a)s, %(b)s)
        """, consistency_level=ConsistencyLevel.ONE)

    prepared = session.prepare("""
        INSERT INTO mytable (primkey, column1, column2)
        VALUES (?, ?, ?)
        """)

    timestamp = int(time.time())
    now = datetime.utcnow()
    log.info("original time: %s", now)

    session.execute(query, dict(key="simple", a='a', b=timestamp))
    session.execute(prepared.bind(("prepared", 'a', timestamp)))

    # insert using datetime
    session.execute(query, dict(key="d_simple", a='a', b=now))
    session.execute(prepared.bind(("d_prepared", 'a', now)))

    future = session.execute_async("SELECT * FROM mytable")
    log.info("key\t|\tcolumn1\t|\tcolumn2")
    log.info("---\t----\t----")

    try:
        rows = future.result()
    except Exception:
        log.exception()

    for row in rows:
        log.info('\t'.join([str(c) for c in row]))

    session.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main()