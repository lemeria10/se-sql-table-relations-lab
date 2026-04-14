# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

df_boston = pd.read_sql("""
SELECT firstName, lastName
FROM employees
JOIN offices
using (officeCode)
WHERE city = 'Boston'
""", conn)
# print(df_boston)




# STEP 2
# Replace None with your code
df_zero_emp = pd.read_sql("""
SELECT officeCode, city
FROM offices
LEFT JOIN employees
using(officeCode)
GROUP BY officeCode
HAVING COUNT(employeeNumber) = 0
""", conn)
# print(df_zero_emp)

# STEP 3
# Replace None with your code
df_employee = pd.read_sql("""
SELECT firstName, lastName, city, state
FROM employees
LEFT JOIN offices
using(officeCode)
ORDER BY firstName, lastName
""", conn)
# print(df_employee)


# STEP 4
# Replace None with your code
df_contacts = pd.read_sql("""
SELECT contactFirstName, contactLastName, phone, salesRepEmployeeNumber
FROM customers
LEFT JOIN orders
USING(customerNumber)
WHERE orderNumber IS NULL
ORDER BY contactLastName
""", conn)
print(df_contacts)

# STEP 5
# Replace None with your code
df_payment = pd.read_sql("""
SELECT contactFirstName, contactLastName, amount, paymentDate
FROM customers
JOIN payments
USING(customerNumber)
ORDER BY CAST(amount AS REAL) DESC
""", conn)
# print(df_payment)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql("""
SELECT employeeNumber, firstName, lastName, COUNT(customerNumber) AS num_customers
FROM employees
JOIN customers 
ON employeeNumber = salesRepEmployeeNumber
GROUP BY employeeNumber
HAVING AVG(creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)
# print(df_credit)

# STEP 7
# Replace None with your code
df_product_sold = pd.read_sql("""
SELECT productName,
COUNT(orderNumber) AS numorders,
SUM(quantityOrdered) AS totalunits
FROM products
JOIN orderdetails
USING(productCode)
GROUP BY productCode
ORDER BY totalunits DESC
""", conn)
# print(df_product_sold)

# STEP 8
# Replace None with your code
df_total_customers = pd.read_sql("""
SELECT productName, productCode,
COUNT(DISTINCT customerNumber) AS numpurchasers
FROM products
JOIN orderdetails
USING(productCode)
JOIN orders
USING(orderNumber)
GROUP BY productCode
ORDER BY numpurchasers DESC
""", conn)
# print(df_total_customers)

# STEP 9
# Replace None with your code
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e 
ON o.officeCode = e.officeCode
LEFT JOIN customers c 
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
""", conn)
# print(df_customers)

# STEP 10
# Replace None with your code
df_under_20 = pd.read_sql("""
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord ON c.customerNumber = ord.customerNumber
JOIN orderdetails od ON ord.orderNumber = od.orderNumber
WHERE od.productCode IN (
    SELECT od.productCode
    FROM orderdetails od
    JOIN orders o2 ON od.orderNumber = o2.orderNumber
    GROUP BY od.productCode
    HAVING COUNT(DISTINCT o2.customerNumber) < 20
)
order by e.lastName
""", conn)
# print(df_under_20)

conn.close()