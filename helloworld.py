from neo4j import GraphDatabase
import random
from datetime import datetime
from datetime import timedelta

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "pere654321"))
session = driver.session()


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
    for x in range(0, length, 1):
        q = random.randint(1, 20)
        p = random.randint(1, 100)
        d = random.randint(1, 5)
        t = random.randint(1, 18)
        r = random.randint(1, 25)
        l = random.randint(1, 2)
        s = str(random_date())
        session.run(
            "CREATE (Line%s:LineItem {quantity:%d, extendedprice:%d, discount:%d, tax:%d, returnflag:%d, linestatus:%d, shipdate:\"%s\"})" % (
                x + 1, q, p, d, t, r, l, s))
        session.run("CREATE (Line%d)-[:LINE_OR]->(Order%d)" % (x + 1, 3))


# ORDERS
def insert_order(length):
    for x in range(0, length, 1):
        d = str(random_date())
        p = random.randint(1, 100)
        session.run("CREATE (Order%s:Order {orderdate:\"%s\", shippriority:%d})" % (
            x + 1, d, p))



# CUSTOMER
def insert_customer(length):
    strs = ["Agricultura", "Mascotes", "Alimentacio", "Moda", "Tecnologia"]
    for x in range(0, length, 1):
        i = random.randint(0, 5)
        print("CREATE (Customer%s:Customer {mktsegment: %s})" %(x + 1, strs[i]))


# RELATIONS


# LINE ITEMS -> ORDERS
# Length must be equal to Number of ListItems
def insert_relation_line_order(length, lo):
    for x in range(0, length, 1):
        order = random.randint(1, lo)
        session.run("CREATE (Line%d)-[:LINE_OR]->(Order%d)" % (x + 1, order))


# Main function
def run():
    print('CBDE\n')
    insert_line_items(10)
    insert_order(4)

    print('THE END')


if __name__ == '__main__':
    run()
