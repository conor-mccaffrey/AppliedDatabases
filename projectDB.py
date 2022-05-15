import pymysql
import datetime
import pymysql.cursors

from neo4j import GraphDatabase
from neo4j import exceptions

conn = None  # none is same as null in mysql
driver = None   # setting this global lets us use it on other files also


def connect():
    global conn
    conn = pymysql.connect(host='localhost', user='root', password='root', db='employees', cursorclass = pymysql.cursors.DictCursor)
   
def connect_Neo():
    global driver
    uri = 'neo4j://localhost:7687'
    graphDB_Driver = GraphDatabase.driver(uri, auth=("neo4j", "conor"), max_connection_lifetime=1000)   
    return graphDB_Driver
    
def get_experience(number):
    if (not conn):
        connect();
    else:
        print('already connected')
        
    query = 'SELECT * FROM teacher WHERE experience < %s'
    
    with conn:
        cursor = conn.cursor()
        cursor.execute(query, (number))
        x = cursor.fetchall()
        return x

def show_names():
    if (not conn):
        connect();

    
    query = '''

    select e.name AS Emp_Name, d.name AS Dep_Name
    from employee e
    inner join dept d
    on e.did = d.did
    order by d.did asc, e.name asc
    ;
'''
    cursor = conn.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    rr(records)

def rr(records):

    while True:
        try:
            print(records[0]['Emp_Name'],'|', records[0]['Dep_Name'])
            print(records[1]['Emp_Name'],'|', records[1]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[2]['Emp_Name'],'|', records[2]['Dep_Name'])
            print(records[3]['Emp_Name'],'|', records[3]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[4]['Emp_Name'],'|', records[4]['Dep_Name'])
            print(records[5]['Emp_Name'],'|', records[5]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[6]['Emp_Name'],'|', records[6]['Dep_Name'])
            print(records[7]['Emp_Name'],'|', records[7]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[8]['Emp_Name'],'|', records[8]['Dep_Name'])
            print(records[9]['Emp_Name'],'|', records[9]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[10]['Emp_Name'],'|', records[10]['Dep_Name'])
            print(records[11]['Emp_Name'],'|', records[11]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[12]['Emp_Name'],'|', records[12]['Dep_Name'])
            print(records[13]['Emp_Name'],'|', records[13]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[14]['Emp_Name'],'|', records[14]['Dep_Name'])
            print(records[15]['Emp_Name'],'|', records[15]['Dep_Name'])
        except StopIteration:
            break
        x = input("-- Quit (q) --")
        if x == 'q':
            break
        try:
            print(records[16]['Emp_Name'],'|', records[16]['Dep_Name'])
            print(records[17]['Emp_Name'],'|', records[17]['Dep_Name'])
            x = input("-- Quit (q) --")
            if x == 'q':
                break
            else:
                x = input("-- Quit (q) --")
                x = input("-- Quit (q) --")
                x = input("-- Quit (q) --")
                x = input("-- Quit (q) --")
                x = input("-- Quit (q) --")
                x = input("-- Quit (q) --")
                break
        except:
            if x == 'q':
                break
            else:
                x = input("-- Quit (q) --")


def salary_details(employeeID):
    if (not conn):
       connect();
    else:
       print('already connected')
       
    query = 'SELECT format(min(salary),2) AS Min, format(avg(salary),2) AS Avg, format(max(salary),2) AS Max FROM salary WHERE eid = %s;'
    
    with conn: # helps with error handling
        
        cursor = conn.cursor()
        cursor.execute(query, (employeeID))
        x = cursor.fetchall()
        return x

     

def month_of_birth(datetime_object):
    if (not conn):
       connect();
    else:
       print('already connected') 
    
    query = ' SELECT eid, name, dob  FROM employee WHERE month(dob) = %s;'
    
    with conn: # helps with error handling
        
            cursor = conn.cursor()
            cursor.execute(query, (datetime_object))
            x = cursor.fetchall()
            return x

def add_employee(EID, Name, DOB, DeptID):
    if (not conn):
       connect();
    else:
       print('already connected') 
    
    query = ' INSERT INTO employee VALUES (%s, %s, %s, %s)'
    
    with conn: # helps with error handling
    
        cursor = conn.cursor()
        cursor.execute(query, (EID, Name, DOB, DeptID))
        x = cursor.fetchall()
        return x
        
def emp_dep(dept):
    if (not conn):
       connect();
    
    query = 'select did AS Department, FORMAT(budget,0) AS Budget FROM dept where did = %s;'
    
    cursor = conn.cursor()
    try:
        cursor.execute(query, (dept))
        x = cursor.fetchall()
        return x
    except (pymysql.Error, pymysql.Warning) as e:
        print(f'error! {e}') 

def emp(tx, eid):  
    query = 'MATCH(e:Employee{eid:$eid})-[:MANAGES]->(d) RETURN d.did as Department ORDER BY Department'
    associations = []
    results = tx.run(query, eid=eid)
    for result in results:
        associations.append(result['Department'])   
    return associations
    
    
def neo(eid):  
    graphDB_Driver = connect_Neo()
    with graphDB_Driver.session() as session:  # driver is connection to db
    # first arugment is function you want to run. Accepts 1 parameter WHICH IS EID
        values = session.read_transaction(emp, eid)
        return values

def checkSQL(enter_EID,enter_DID):
    if (not conn):
        connect();
    
        query = 'select eid AS EID, did AS DID FROM employee where EID = %s AND DID = %s;'
        ass = []
        cursor = conn.cursor()
        try:
            cursor.execute(query, (enter_EID,enter_DID))
            x = cursor.fetchall()
            for i in x:
                ass.append(x[0]['EID'])
                ass.append(x[0]['DID'])
            return ass
            

        except Exception as e:
            print(e) 

def neo2(d,f):  
    graphDB_Driver = connect_Neo()
    with graphDB_Driver.session() as session:  # driver is connection to db
    # first arugment is function you want to run. Accepts 1 parameter WHICH IS EID
        try:
            values = session.write_transaction(add_toNeo4j, d,f)
        except exceptions.ConstraintError as e:
            print('Department', f , 'already managed by', d)
        return values





def add_toNeo4j(tx, d,f):
  
    query = 'MERGE (e:Employee{eid:$eid}) MERGE (d:Department {did:$did}) MERGE (e)-[:MANAGES]->(d) RETURN e.name'
    ass = []
    results = tx.run(query, eid=d,did=f)
    for result in results:
        ass.append(result)
        
    return ass
 
    

def emp(tx, eid):  
    query = 'MATCH(e:Employee{eid:$eid})-[:MANAGES]->(d) RETURN d.did as Department ORDER BY Department'
    associations = []
    results = tx.run(query, eid=eid)
    for result in results:
        associations.append(result['Department'])   
    return associations
    





def add_Manager(tx, EID,DID):  #tx is always there.  get data from db
    query = 'MATCH(e:Employee{eid:$eid})-[:MANAGES]->(d) RETURN d.did as Department ORDER BY Department'
    associations = []
    results = tx.run(query, eid=eid)
    for result in results:
        associations.append(result['Department'])   
    return associations



    
def show_Dep():
    connect();
    query = 'call viewDep'
    
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        x = cursor.fetchall()
        return x
        
    except (pymysql.Error, pymysql.Warning) as e:
        print(f'error! {e}') 

       