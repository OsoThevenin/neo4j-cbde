import string

from neo4j import GraphDatabase
import random
from datetime import datetime
from datetime import timedelta

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "pere654321"))


# DATE FUNCTIONS
def random_date():
    d1 = datetime.strptime('1/1/2018 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('1/1/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

    return random_date_range(d1, d2)


def random_date_range(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


# INITIALIZE DATABASE
# LINE_ITEM
def insert_line_items(length):
    session = driver.session()
    for x in range(0, length, 1):
        q = random.randint(1, 20)
        p = random.randint(1, 100)
        d = random.randint(1, 5)
        t = random.randint(1, 18)
        r = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(1))
        l = random.randint(1, 2)
        s = str(random_date())
        session.run(
            "CREATE (Line%s:LineItem {quantity:%d, extendedprice:%d, discount:%d, tax:%d, returnflag:\'%s\', "
            "linestatus:%d, shipdate:\"%s\"})" % (
                x + 1, q, p, d, t, r, l, s))


# ORDERS
def insert_order(length):
    session = driver.session()
    for x in range(0, length, 1):
        d = str(random_date())
        p = random.randint(1, 100)
        session.run("CREATE (Order%s:Order {orderdate:\"%s\", shippriority:%d})" % (
            x + 1, d, p))


# CUSTOMER
def insert_customer(length):
    session = driver.session()
    strs = ["Agricultura", "Mascotes", "Alimentacio", "Moda", "Tecnologia"]
    for x in range(0, length, 1):
        i = random.randint(1, 5)
        session.run("CREATE (Customer%d:Customer {mktsegment: \'%s\'})" % (x + 1, strs[i-1]))


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
                    "acctbal:  %d, comment: \'Comment%d\'})" % (x + 1, x + 1, addrs[a-1], p, act, p))


# PART
def insert_part(length):
    session = driver.session()
    for x in range(0, length, 1):
        s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        t = ["Super", "Extra", "Gegant", "Mini", "Little", "Giant", "Big", "Small", "XXL", "XXS"]
        i = random.randint(1, len(t))
        size = random.randint(1, 200)
        session.run("CREATE (Part%d:Part {name: \'Part%d\', mfgr: \'%s\', type: \'%s\', size: %d})" %(x + 1, x + size, s, t[i-1], size))


# PART_SUP
def insert_partsup(length):
    session = driver.session()
    for x in range(0, length, 1):
        c = random.randint(1, 200)
        session.run("CREATE (PartSup%d:PartSup {supplycost: %d})" % (x + 1, c))


# RELATIONS

# LINE ITEMS -> ORDERS
# Length must be equal to Number of ListItems
def insert_relation_line_order(length, lo):
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        print("CREATE (Line%d)-[:LINE_OR]->(Order%d)" % (x + 1, order))


# Main function
def run():
    insert_line_items(10)
    print("Insert line items")
    insert_order(4)
    print("Insert order")
    insert_customer(4)
    print("Insert customer")
    insert_nation(6)
    print("Insert nation")
    insert_region(4)
    print("Insert region")
    insert_supplier(10)
    print("Insert supplier")
    insert_part(7)
    print("Insert part")
    insert_partsup(4)
    print("Insert partsup")

    print('THE END')


if __name__ == '__main__':
    run()
