from neo4j import GraphDatabase
import random
from datetime import datetime

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "pere654321"))
session = driver.session()

# INITIALIZE DATABASE
# LINEITEM

for x in range(0, 6, 1):
    q = random.randint(1, 20)
    p = random.randint(1, 100)
    d = random.randint(1, 5)
    t = random.randint(1, 18)
    r = random.randint(1, 25)
    l = random.randint(1, 2)
    s = datetime.now()
    session.run("CREATE (line%s:LineItem {quantity:%d, extendedprice:%d, discount:%d, tax:%d, returnflag:%d, linestatus:%d, shipdate:%s})" % (x+1,q,p,d,t,r,l,s.strftime("%m/%d/%Y")))

session.run("CREATE INDEX on:LineItem(quantity)")

def print_friends_of(tx, name):
    for record in tx.run("MATCH (l:LineItem)-[:TE_ORDER]->(f) "
                         "WHERE f.ship_priority = {name} "
                         "RETURN l, f", name=name):
        print(record)


#with driver.session() as session:
    #session.read_transaction(print_friends_of, 1)
