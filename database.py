import abc
import mysql.connector
import datetime

class GenericDatabaseProcessor:
    def __init__(self):
        pass
    @abc.abstractmethod
    def ConnectToDatabase(self):
        pass
    @abc.abstractmethod
    def RetrieveDataFromDatabase(self):
        pass
    @abc.abstractmethod
    def CloseDatabase(self):
        pass
    @abc.abstractmethod
    def CreateTable(self):
        pass
    @abc.abstractmethod
    def PopulateTable(self):
        pass

class MySQLDatabaseProcessor(GenericDatabaseProcessor):
    def __init__(self):
        self.db = None
    def ConnectToDatabase(self):
        try:
            self.db = mysql.connector.connect(user='root', password='th3g0ggl35d0n0th1ng!',\
                database='dbo')
        except mysql.connector.errors.ProgrammingError as err:
            print('Error in connecting to database --', err)
            self.db.rollback()
    def RetrieveDataFromDatabase(self, request):
        cursor = self.db.cursor()
        try:
            cursor.execute(request)
            results = cursor.fetchall()
            yield results
        except mysql.connector.errors.ProgrammingError as err:
            print('Error in retrieving data from database --', err)
            self.db.rollback()
    def CloseDatabase(self):
        self.db.close()
    def CreateTable(self):
        sql = '''CREATE TABLE dbo.employees(
                EmployeeID int NOT NULL,
                EmployeeName varchar(64) NOT NULL,
                FirstName varchar(64) NOT NULL,
                LastName varchar(64) NOT NULL,
                PayRate decimal(4,2) NOT NULL,
                HoursWorked decimal(4,2) NOT NULL,
                Fico decimal(4,2) NOT NULL,
                Exemptions int NOT NULL,
                HireDate date NOT NULL,
                Gender varchar(1) NOT NULL,
                DateEntered date NOT NULL,
                IsPasswordReset int NOT NULL,
                Password varchar(64) NOT NULL);'''
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            self.db.commit()
        except mysql.connector.errors.ProgrammingError as err:
            print('Error in creating table:', err)
            self.db.rollback()
    def DropTable(self):
        sql = 'DROP TABLE IF EXISTS dbo.employees'
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            self.db.commit()
        except mysql.connector.errors.ProgrammingError as err:
            print('Error in dropping table:', err)
            self.db.rollback()
    def PopulateTable(self):
        try:
            cursor = self.db.cursor()
            employeefile = open('employees.csv', 'r')
            employeefile.readline() # skip first line
            for line in employeefile:
                (empid, full, fn, ln, pr, hw, fico, ex, hd, g, de, ipr, pw) = line.split(',')
                hd = datetime.datetime.strptime(hd, '%m-%d-%Y').strftime('%Y-%m-%d')
                de = datetime.datetime.strptime(de, '%m-%d-%Y').strftime('%Y-%m-%d')
                insert = '''INSERT INTO employees (EmployeeID, EmployeeName, FirstName, 
                    LastName, PayRate, HoursWorked, Fico, Exemptions, HireDate, Gender, DateEntered, 
                    IsPasswordReset, Password) 
                    VALUES '''
                value = '''('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', 
                    '{10}', '{11}', '{12}');'''
                sql = insert + value.format(empid, full, fn, ln, pr, hw, fico, ex, hd, g,\
                    de, ipr, pw)
                cursor.execute(sql)
                self.db.commit()
        except IndexError as err:
            print('Error in populating table:', err)
            self.db.rollback()
        except ValueError as err:
            print('Error in populating table:', err)
            self.db.rollback()
        except FileNotFoundError as err:
            print('Error in populating tables', err)
            self.db.rollback()
        except mysql.connector.errors.ProgrammingError as err:
            print('Error in populating table:', err)
            self.db.rollback()

if __name__ == '__main__':
    c = MySQLDatabaseProcessor()
    c.ConnectToDatabase()
    #c.DropTable() # only need to run once
    #c.CreateTable() # only need to run once
    #c.PopulateTable() # only need to run once

    # Which employees make more than the average salary who were hired in 2010
    query1 = '''SELECT EmployeeID, EmployeeName , PayRate * HoursWorked AS "Salary", HireDate
                FROM employees
                WHERE PayRate * HoursWorked > (SELECT AVG(PayRate * HoursWorked) FROM employees)
                AND year(HireDate) = 2010;''' # no employees were hired in 2010
    
    # There are several cities in the state of Minnesota that have Lake as part of their name.
    query2 = """SELECT employees.EmployeeID, employees.EmployeeName, employeeaddresses.StateAbbr, 
                employeeaddresses.City
                FROM employees
                INNER JOIN employeeaddresses
                ON employees.EmployeeID = employeeaddresses.EmployeeID
                WHERE employeeaddresses.StateAbbr = 'MN'
                AND employeeaddresses.City LIKE '%Lake%'
                AND employeeaddresses.City NOT LIKE '%Lakes%'"""
    
    # Find all the employees who live in Pennsylvania, and group the count by cities
    query4 = """SELECT City, COUNT(*) Count
                FROM employeeaddresses
                WHERE StateAbbr = 'PA'
                GROUP BY City;"""
    query3 = """SELECT employeeaddresses.City, COUNT(*) Count
                FROM employees
                INNER JOIN employeeaddresses
                ON employees.EmployeeID = employeeaddresses.EmployeeID
                WHERE employeeaddresses.StateAbbr = 'PA'
                GROUP BY employeeaddresses.City;"""
    try:
        for row in next(c.RetrieveDataFromDatabase(query1)):
            (empId, full, salary, hd) = row
            print('{0},{1},{2},{3}'.format(empId, full, salary, hd)) 
    except TypeError as err:
        print('Error in displaying data:', err)
    print()    

    try:
        for row in next(c.RetrieveDataFromDatabase(query2)):
            (empId, full, state, city) = row
            print('{0},{1},{2},{3}'.format(empId, full, state, city))
    except TypeError as err:
        print('Error in displaying data:', err)
    print()

    try:
        for row in next(c.RetrieveDataFromDatabase(query3)):
            (city, count) = row
            print('{0},{1}'.format(city, count))
    except TypeError as err:
        print('Error in displaying data:', err)
    c.CloseDatabase()
