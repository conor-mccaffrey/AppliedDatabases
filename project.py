# Project2022
import projectDB
import pymysql
import datetime
import struct
from neo4j import GraphDatabase
from neo4j import exceptions


def main():
    while True:
        display_Menu()
        choice = (input('Choice:'))
        
        if (choice=='x'):
            break
        elif(choice=='1'):
            q = 'q'
            details = projectDB.show_names()
        elif(choice=='2'):
            employeeID = input('Enter EID : ')
            print('Salary Details For Employee: ', employeeID)
            print('---------------------------')
            print('Minimum','|','Average','|','Maximum', '|')
            details = projectDB.salary_details(employeeID)
            for detail in details:
                if (detail['Min']) != None:
                    print(detail['Min'],'|', detail['Avg'], '|', detail['Max'])
                else:
                    print('')
        elif(choice=='3'):
            while True:
                month = input('Enter Month:')
                try:
                    if len(month) >= 3:
                        datetime_object = datetime.datetime.strptime(month[:3], '%b').month     
                    elif len(month) < 3:
                        datetime_object = int(month)
                    
                    else:
                        continue
                    details = projectDB.month_of_birth(datetime_object)
                    for detail in details:
                        if (detail['eid']) != None:
                            print(detail['eid'],'|', detail['name'], '|', detail['dob'])
                        else:
                            print('Error')
                    break
                except Exception as e:
                    continue

        elif(choice=='4'):
            print('Add New Employee')
            print('------------------')
            EID = input('EID : ')
            Name = input('Name : ')
            DOB = input('DOB : ')
            DeptID = input('Dept ID :')
            try:
                details = projectDB.add_employee(EID, Name, DOB, DeptID)
                print('Employee successfully added')
                
            except pymysql.err.IntegrityError as e:
                print('*** ERROR ***:', EID ,'already exists', 'or' ,'*** ERROR ***: Department', DeptID, 'does not exist') 
            except pymysql.err.DatabaseError as e:
                print('*** ERROR ***: Invalid DOB: ', DOB)
            except Exception as e:
                print('*** ERROR ***: Department', DeptID, 'does not exist')
                
        elif(choice=='5'):
            eid = input("Enter EID: ")
            associations = projectDB.neo(eid)
            print(f"""\nDepartments Managed by:  {eid} \n{'-' * 20}""")
            print('Department', '|', 'Budget')
            #print([0]["Department"], [0]["Budget"])
            if not associations:
                display_Menu 
            for value in associations:
                q = projectDB.emp_dep(value)
                print(q[0]["Department"], '|', q[0]["Budget"])
                
        elif(choice=='6'):
            while True:
                
                enter_EID = input('Enter EID :')
                enter_DID = input('Enter DID :')
                try:
                    ass = projectDB.checkSQL(enter_EID,enter_DID)
                    d = ass[0] # THESE ARE NOW STRINGS
                    f = ass[1]
                    if len(ass) != 0:
                        q = projectDB.neo2(d,f)
                        
                        if not q:
                            print(f"""Employee {enter_EID} does not exist \nDepartment {enter_DID} does not exist""")
                        else:
                            print(f'''Employee {d} now manages Department {f}''')
                            break
                        display_Menu
                    
                except:        
                    print(f"""Employee {enter_EID} does not exist \nDepartment {enter_DID} does not exist""")
                    continue
                

                            

     
        
             
            
        elif(choice=='7'):
            details = projectDB.show_Dep()
            print('Did','|','Name','|','Location','|','Budget',)
            for detail in details:
                print(detail["Did"], '|', detail["Name"],'|',detail["Location"], '|', detail["Budget"])
 
 
        else:
            print('Pick a valid option please :')
           

def display_Menu():
    print('Employees')
    print('')
    print('MENU')
    print('====')
    print('1 - View Employees & Departments')
    print('2 - View Salary Details')
    print('3 - View by Month of Birth')
    print('4 - Add New Employee')
    print('5 - View Departments managed by Employee')
    print('6 - Add Manager to Department')
    print('7 - View Departments')
    print('x - Exit Application')
   

if __name__=='__main__':
    main()