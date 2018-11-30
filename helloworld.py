import string

from neo4j import GraphDatabase
import random
from datetime import date

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "pere654321"))


# INITIALIZE DATABASE
# LINEITEM


def random_date():
    date_rand = date(random.randint(2016, 2020), random.randint(1, 12), random.randint(1, 28))
    return date_rand


# INITIALIZE DATABASE
# LINE_ITEM
def insert_line_items(length):
    session = driver.session()
    for x in range(0, length, 1):
        q = random.randint(1, 20)
        p = random.randint(1, 100)
        d = random.randint(1, 5)
        t = random.randint(1, 18)
        r = ''.join(random.choice('AB') for _ in range(1))
        l = random.randint(1, 2)
        s = str(random_date())
        session.run(
            "CREATE (Line%d:LineItem {key: \'Line%d\', quantity:%d, extendedprice:%d, discount:%d, tax:%d, returnflag:\'%s\', "
            "linestatus:%d, shipdate:\"%s\"})" % (
                x + 1, x + 1, q, p, d, t, r, l, s))

# ORDERS
def insert_order(length):
    session = driver.session()
    for x in range(0, length, 1):
        d = str(random_date())
        p = random.randint(1, 100)
        session.run("CREATE (Order%d:Order {key: \'Order%d\', orderdate:\"%s\", shippriority:%d})" % (
            x + 1, x + 1, d, p))


# CUSTOMER
def insert_customer(length):
    session = driver.session()
    strs = ["Agricultura", "Mascotes", "Alimentacio", "Moda", "Tecnologia"]
    for x in range(0, length, 1):
        i = random.randint(1, 5)
        session.run("CREATE (Customer%d:Customer {key: \'Customer%d\', mktsegment: \'%s\'})" % (x + 1, x + 1, strs[i - 1]))


# NATION
def insert_nation(length):
    session = driver.session()
    for x in range(0, length, 1):
        session.run("CREATE (Nation%d: Nation {name: \'Nation%d\'})" % (x + 1, x + 1))


# REGION
def insert_region(length):
    session = driver.session()
    for x in range(0, length, 1):
        session.run("CREATE (Region%d: Region {name: \'Region%d\'})" % (x + 1, x + 1))


# SUPPLIER
def insert_supplier(length):
    session = driver.session()
    addrs = ["Pl Cat", "Diagonal", "Campus Nord", "Galicia", "Bombai", "Sant Jaume", "Corts"]
    for x in range(0, length, 1):
        a = random.randint(1, len(addrs))
        act = random.randint(1, 20)
        p = random.randint(1, 1000)
        session.run("CREATE (Supplier%d: Supplier {name: \'Supplier%d\', address: \'%s\', phone: %d, "
                    "acctbal:  %d, comment: \'Comment%d\'})" % (x + 1, x + 1, addrs[a - 1], p, act, p))


# PART
def insert_part(length):
    session = driver.session()
    for x in range(0, length, 1):
        s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        t = ["Super", "Extra", "Gegant", "Mini", "Little", "Giant", "Big", "Small", "XXL", "XXS"]
        i = random.randint(1, len(t))
        size = random.randint(1, 200)
        session.run("CREATE (Part%d:Part {name: \'Part%d\', mfgr: \'%s\', type: \'%s\', size: %d})" % (
            x + 1, x + 1, s, t[i - 1], size))


# PART_SUP
def insert_partsup(length):
    session = driver.session()
    for x in range(0, length, 1):
        c = random.randint(1, 200)
        session.run("CREATE (PartSup%d:PartSup {key: \'PartSup%d\', supplycost: %d})" % (x + 1, x + 1, c))


# RELATIONS

# LINE ITEMS -> ORDERS
# Length must be equal to Number of ListItems
def insert_relation_line_order(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        session.run("MATCH (l:LineItem) "
                    "WITH l "
                    "MATCH (o:Order) "
                    "WHERE l.key = \"Line%d\" and o.key = \"Order%d\" "
                    "CREATE (l)-[:LINE_O]->(o);" % (x + 1, order))


# ORDERS -> CUSTOMERS
def insert_relation_or_cust(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        rand = random.randint(1, lo)
        session.run("MATCH (o:Order) "
                    "WITH o "
                    "MATCH (c:Customer) "
                    "WHERE o.key = \"Order%d\" and c.key = \"Customer%d\" "
                    "CREATE (o)-[:OR_CUS]->(c);" % (x + 1, rand))


# CUSTOMER -> NATION
def insert_relation_cust_nat(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        rand = random.randint(1, lo)
        session.run("MATCH (c:Customer) "
                    "WITH c "
                    "MATCH (n:Nation) "
                    "WHERE c.key = \"Customer%d\" and n.name = \"Nation%d\" "
                    "CREATE (c)-[:NATIONALITY]->(n);" % (x + 1, rand))


# CUSTOMER -> NATION
def insert_relation_nat_reg(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        rand = random.randint(1, lo)
        session.run("MATCH (n:Nation) "
                    "WITH n "
                    "MATCH (r:Region) "
                    "WHERE n.name = \"Nation%d\" and r.name = \"Region%d\" "
                    "CREATE (n)-[:REGIONALITY]->(r);" % (x + 1, rand))


# LINE ITEMS -> PARTSUP
def insert_relation_line_ps(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        session.run("MATCH (l:LineItem) "
                    "WITH l "
                    "MATCH (ps:PartSup) "
                    "WHERE l.key = \"Line%d\" and ps.key = \"PartSup%d\" "
                    "CREATE (l)-[:LINE_PS]->(ps);" % (x + 1, order))


# PARTSUP -> PART
def insert_relation_ps_part(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        session.run("MATCH (ps:PartSup) "
                    "WITH ps "
                    "MATCH (p:Part) "
                    "WHERE ps.key = \"PartSup%d\" and p.name = \"Part%d\" "
                    "CREATE (ps)-[:PS_PART]->(p);" % (x + 1, order))


# PARTSUP -> SUPPLIER
def insert_relation_ps_supp(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        session.run("MATCH (ps:PartSup) "
                    "WITH ps "
                    "MATCH (s:Supplier) "
                    "WHERE ps.key = \"PartSup%d\" and s.name = \"Supplier%d\" "
                    "CREATE (ps)-[:PS_SUPP]->(s);" % (x + 1, order))


# SUPPLIER -> NATION
def insert_relation_supp_nat(length, lo):
    session = driver.session()
    for x in range(0, length, 1):
        rand = random.randint(1, lo)
        session.run("MATCH (s:Supplier) "
                    "WITH s "
                    "MATCH (n:Nation) "
                    "WHERE s.name = \"Supplier%d\" and n.name = \"Nation%d\" "
                    "CREATE (s)-[:NATIONALITY]->(n);" % (x + 1, rand))



def print_friends_of():
    print('3 \n')
    for record in driver.session().run("MATCH (l:LineItem) " +
                                       "WHERE l.shipdate <= '2018-01-01'" +
                                       "WITH " +
                                       "l.returnflag AS l_returnflag," +
                                       "l.linestatus AS l_linestatus," +
                                       "SUM(l.quantity) AS sum_qty, " +
                                       "SUM(l.extendedprice) AS sum_base_price, " +
                                       "SUM(l.extendedprice * ( 1 - l.l_discount)) AS sum_disc_price, " +
                                       "SUM(l.extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge, " +
                                       "AVG(l.quantity) AS avg_qty, " +
                                       "AVG(l.extendedprice) AS avg_price, " +
                                       "AVG(l.discount) AS avg_disc, " +
                                       "COUNT(*) AS count_order " +
                                       "RETURN " +
                                       "l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, " +
                                       "sum_charge, avg_qty, avg_price, avg_disc, count_order " +
                                       "ORDER BY l_returnflag, l_linestatus "):
        print(record)


def run(l, o, c, n, r, s, p, ps):
    print('Starting\n')

    insert_line_items(l)
    print("Insert line items")
    insert_order(o)
    print("Insert order")
    insert_customer(c)
    print("Insert customer")
    insert_nation(n)
    print("Insert nation")
    insert_region(r)
    print("Insert region")
    insert_supplier(s)
    print("Insert supplier")
    insert_part(p)
    print("Insert part")
    insert_partsup(ps)
    print("Insert partsup")

    print("Create Relations")
    insert_relation_line_order(l, o)
    insert_relation_or_cust(o, c)
    insert_relation_cust_nat(c, n)
    insert_relation_nat_reg(n, r)
    insert_relation_line_ps(l, ps)
    insert_relation_ps_part(ps, p)
    insert_relation_ps_supp(ps, s)
    insert_relation_supp_nat(s, n)
    print("Finish creating relations")

    print_friends_of()

    print('THE END')


# line_item, order, customer, nation, region, supplier, part, partsup
if __name__ == '__main__':
    run(600, 150, 15, 6, 2, 2, 20, 80)

# with driver.session() as session:
# session.read_transaction(print_friends_of, 1)