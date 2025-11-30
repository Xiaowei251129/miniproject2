### Utility Functions
import pandas as pd
import os
import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time

from utils import get_db_url





def create_connection():

    DATABASE_URL = get_db_url()
    conn = psycopg2.connect(DATABASE_URL)
    # conn.execute("PRAGMA foreign_keys = 1")

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    """
    创建表的辅助函数：
    - 如果传入 drop_table_name，就先 DROP 这张表（用 CASCADE，自动删掉依赖它的外键表）。
    - 然后执行 CREATE TABLE 语句。
    - 出错时打印错误并抛出，让你看到真实原因。
    """
    cur = conn.cursor()
    try:
        if drop_table_name:
            # 注意：这里用 CASCADE，避免外键依赖导致 DROP 失败
            drop_sql = f"DROP TABLE IF EXISTS {drop_table_name} CASCADE;"
            cur.execute(drop_sql)

        cur.execute(create_table_sql)
    except Exception as e:
        # 很重要：不要再悄悄吞错误了
        conn.rollback()
        print(f"[create_table] Error while creating {drop_table_name}: {e}")
        raise
    else:
        # 如果没出错，提交一下（因为你用了 with conn_norm:，严格说也可以不在这里 commit，
        # 但这样更安全、清晰）
        conn.commit()
    finally:
        cur.close()

        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def step1_create_region_table(data_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  region_set = set()
  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).split('\t')
    try:
      region_idx = header.index('Region')
    except ValueError:
      raise ValueError
    for line in f:
      line = line.strip().split('\t')
      if not line:
        continue
      region_value = line[region_idx].strip()
      if region_value:
        region_set.add(region_value)
  region_sorted = sorted(region_set)
  region_rows = [(r,) for r in region_sorted]

  conn_norm = create_connection()
  with conn_norm:
    creat_region_sql = """
        CREATE TABLE IF NOT EXISTS Region(
        RegionID SERIAL not null primary key,
        Region Text not null
        );
    """
    create_table(conn_norm, creat_region_sql, drop_table_name='Region')
    insert_region_sql = """
    INSERT INTO Region(Region) VALUES(%s);
    """
    cur = conn_norm.cursor()
    cur.executemany(insert_region_sql, region_rows)
    cur.close()

# WRITE YOUR CODE HERE

def step2_create_region_to_regionid_dictionary():

  conn_norm = create_connection()
  
  fetch_region_sql = """
    SELECT RegionID, Region
    FROM Region
    """
  with conn_norm:
    rows = execute_sql_statement(fetch_region_sql, conn_norm)

  region_dict = {region: regionid for (regionid, region) in rows}
  
  return region_dict
    
# WRITE YOUR CODE HERE


def step3_create_country_table(data_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  country_region_set = set()
  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).strip().split('\t')
    try:
      country_idx = header.index('Country')
      region_idx = header.index('Region')

    except ValueError:
      raise ValueError

    for line in f:
      line = line.strip().split('\t')
      if not line:
        continue

      country = line[country_idx].strip()
      region = line[region_idx].strip()

      country_region = (country,region)
      if country_region:
        country_region_set.add(country_region)

  sorted_country_region = sorted(country_region_set, key=lambda x: x[0])

  conn_norm = create_connection()

  create_country_sql = """
  CREATE TABLE IF NOT EXISTS Country(
  CountryID SERIAL not null Primary key,
  Country Text not null,
  RegionID SERIAL not null, 
  foreign key(RegionID) references Region(RegionID)
  );
  """
  with conn_norm:
    create_table(conn_norm, create_country_sql, drop_table_name='Country')

  fill_country_sql = """
  INSERT INTO COUNTRY(Country, RegionID) VALUES (%s, %s);
  """
  with conn_norm:
    region_dict = step2_create_region_to_regionid_dictionary()

    country_row = []
    for country, region in sorted_country_region:
      RegionID = region_dict[region]
      country_row.append((country, RegionID))

    cur = conn_norm.cursor()
    cur.executemany(fill_country_sql, country_row)
    cur.close()



# WRITE YOUR CODE HERE


def step4_create_country_to_countryid_dictionary():
  conn_norm = create_connection()
  fetch_country_sql = """
  SELECT CountryID, Country
  FROM Country;
  """
  with conn_norm:
    rows = execute_sql_statement(fetch_country_sql, conn_norm)
  
  country_dict = {Country:CountryID for (CountryID, Country) in rows}

  return country_dict


# WRITE YOUR CODE HERE
        
        
def step5_create_customer_table(data_filename):
  
  rows = []
  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).strip().split('\t')

    try:
      name_idx = header.index('Name')
      address_idx = header.index('Address')
      city_idx = header.index('City')
      country_idx = header.index('Country')
    except ValueError:
      raise ValueError

    for line in f:
      line = line.strip().split('\t')
      if not line:
        continue

      firstName, lastName = line[name_idx].split(' ', 1)
      address = line[address_idx]
      city = line[city_idx]
      country = line[country_idx]

      row = (firstName, lastName, address, city, country)
      rows.append(row)

  sorted_rows = sorted(rows, key=lambda x: (x[0], x[1]))

  customer_values = []
  country_dict = step4_create_country_to_countryid_dictionary()
  for f, l, a, c, co in sorted_rows:
    CountryID = country_dict[co]
    customer_values.append((f, l, a, c, CountryID))

  conn_norm = create_connection()
  create_customer_sql = """
  CREATE TABLE IF NOT EXISTS Customer(
    CustomerID SERIAL not null Primary Key,
    FirstName Text not null,
    LastName Text not null,
    Address Text not null,
    City Text not null,
    CountryID SERIAL not null,
    foreign key(CountryID) REFERENCES Country(CountryID) 
  );
  """

  fill_customer_sql = """
  INSERT INTO Customer(FirstName, LastName, Address, City, CountryID)
  VALUES(%s,%s,%s,%s,%s);
  """

  with conn_norm:
    create_table(conn_norm, create_customer_sql, drop_table_name='Customer')

    cur = conn_norm.cursor()
    cur.executemany(fill_customer_sql, customer_values)
    cur.close() 

# WRITE YOUR CODE HERE


def step6_create_customer_to_customerid_dictionary():
  fetch_customer_sql = """
  SELECT CustomerID, FirstName, LastName
  FROM Customer;
  """
  conn_norm = create_connection()
  with conn_norm:
    rows = execute_sql_statement(fetch_customer_sql, conn_norm)

  customer_dict = {f'{f} {l}': cust_id for cust_id, f, l in rows}
  return customer_dict
# WRITE YOUR CODE HERE
        

def step7_create_productcategory_table(data_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

  cate_set = set()

  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).strip().split('\t')
    try:
      prodcate_idx = header.index('ProductCategory')
      proddisc_idx = header.index('ProductCategoryDescription')
    except ValueError:
      raise ValueError

    for line in f:
      line = line.strip()
      if not line:
        continue

      line = line.split('\t') 
      prodcate = line[prodcate_idx].split(';')
      proddisc = line[proddisc_idx].split(';')

      for cate, disc in zip(prodcate, proddisc):
        cate_set.add((cate, disc))
  
  sorted_cate = sorted(cate_set)

  rows = []
  for c, d in sorted_cate:
    row = (c, d)
    rows.append(row)

  create_prodcate_sql = """
    CREATE TABLE IF NOT EXISTS ProductCategory(
    ProductCategoryID SERIAL not null Primary Key,
    ProductCategory Text not null,
    ProductCategoryDescription Text not null
    );
  """

  fill_prodcate_sql = """
    INSERT INTO ProductCategory(ProductCategory, ProductCategoryDescription)
    VALUES (%s,%s);
  """

  conn_norm = create_connection()
  with conn_norm:
    create_table(conn_norm, create_prodcate_sql, drop_table_name='ProductCategory')

    cur = conn_norm.cursor()
    cur.executemany(fill_prodcate_sql, sorted_cate)
    cur.close()
    
# WRITE YOUR CODE HERE

def step8_create_productcategory_to_productcategoryid_dictionary():
  fetch_category_sql = """
  SELECT ProductCategoryID, ProductCategory
  FROM ProductCategory;
  """
  conn_norm = create_connection()
  with conn_norm:
    rows = execute_sql_statement(fetch_category_sql, conn_norm)
  
  prodcate_dict = {prodcate: prodid for prodid, prodcate in rows}
  return prodcate_dict

# WRITE YOUR CODE HERE
        

def step9_create_product_table(data_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
  product_set = set()
  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).strip().split('\t')

    try:
      product_idx = header.index('ProductName')
      price_idx = header.index('ProductUnitPrice')
      prodcate_idx = header.index('ProductCategory')
    except ValueError:
      raise ValueError
    
    for line in f:
      line = line.strip()
      if not line:
        continue
      line = line.split('\t')

      product = line[product_idx].strip().split(';')
      unitprice = line[price_idx].strip().split(';')
      prodcate = line[prodcate_idx].strip().split(';')

      for prod, price, cate in zip(product, unitprice, prodcate):
        product_set.add((prod, price, cate))
      
  sorted_product = sorted(product_set)

  create_product_sql = """
  CREATE TABLE IF NOT EXISTS Product(
  ProductID SERIAL not null Primary key,
  ProductName Text not null,
  ProductUnitPrice Real not null,
  ProductCategoryID SERIAL not null, 
  foreign key(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
  );
  """
  
  fill_product_sql = """
  INSERT INTO Product(ProductName, ProductUnitPrice, ProductCategoryID)
  VALUES (%s, %s, %s);
  """

  prodcate_dict = step8_create_productcategory_to_productcategoryid_dictionary()
  rows = []
  for prod, price, cate in sorted_product:
    ProductCategoryID = prodcate_dict[cate]
    row = (prod, price, ProductCategoryID)
    rows.append(row)

  conn_norm = create_connection()
  with conn_norm:
    cur = conn_norm.cursor()
    create_table(conn_norm, create_product_sql, drop_table_name='Product')
    cur.executemany(fill_product_sql, rows)
    cur.close()


# WRITE YOUR CODE HERE


def step10_create_product_to_productid_dictionary():
  fetch_product_sql = """
  SELECT ProductID, ProductName
  FROM Product
  """
  conn_norm = create_connection()
  with conn_norm:
    rows = execute_sql_statement(fetch_product_sql, conn_norm)
    product_dict = {prod_name: prod_id for prod_id, prod_name in rows}
  return product_dict

# WRITE YOUR CODE HERE
        

from psycopg2 import extras  # 你文件顶部已经有这一行就不用再加

def step11_create_orderdetail_table(data_filename):
  # Inputs: Name of the data and normalized database filename
  # Output: None
  import datetime

  rows = []
  with open(data_filename, 'r', encoding='utf-8') as f:
    header = next(f).strip().split('\t')

    try:
      cust_idx = header.index('Name')
      prod_idx = header.index('ProductName')
      order_idx = header.index('QuantityOrderded')
      orderdate_idx = header.index('OrderDate')
    except ValueError:
      raise ValueError
    
    for line in f:
      line = line.strip()
      if not line:
        continue

      line = line.split('\t')
      customer = line[cust_idx].strip()
      product = line[prod_idx].strip().split(';')
      order = line[order_idx].strip().split(';')
      date = line[orderdate_idx].strip().split(';')

      for p, o, d in zip(product, order, date):
        rows.append((customer, p, o, d))

  print("step11: number of raw order rows =", len(rows))   # 这里就是 621806

  # 把名字/产品名映射到 ID，并把日期格式化
  customer_dict = step6_create_customer_to_customerid_dictionary()
  product_dict = step10_create_product_to_productid_dictionary()

  row_values = []
  for customer, product, order, date in rows:
    customer_id = customer_dict[customer]
    product_id = product_dict[product]
    qty = int(order)
    date_fmt = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
    row_values.append((customer_id, product_id, date_fmt, qty))

  print("step11: number of row_values =", len(row_values))  # 应该也是 621806

  create_order_sql = """
  CREATE TABLE IF NOT EXISTS OrderDetail(
    OrderID SERIAL not null Primary Key,
    CustomerID SERIAL not null, 
    ProductID SERIAL not null, 
    OrderDate TIMESTAMP not null, 
    QuantityOrdered SERIAL not null,
    foreign key(CustomerID) REFERENCES Customer(CustomerID),
    foreign key(ProductID) REFERENCES Product(ProductID)
  );
  """

  # 注意：这里专门写成 VALUES %s，给 execute_values 用
  fill_order_sql = """
  INSERT INTO OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered)
  VALUES %s;
  """

  conn_norm = create_connection()
  with conn_norm:
    # 先重建表
    create_table(conn_norm, create_order_sql, drop_table_name='OrderDetail')

    cur = conn_norm.cursor()
    # 用 execute_values 分批插入，大幅减少往返次数
    extras.execute_values(
      cur,
      fill_order_sql,
      row_values,
      page_size=10000    # 一批 1 万行，你也可以改成 5000、20000 试
    )
    cur.close()

  




if __name__ == "__main__":
  data_filename = 'data.csv'
  step1_create_region_table(data_filename)
  print('... step1 done')
  step3_create_country_table(data_filename)
  print('... step3 done')
  step5_create_customer_table(data_filename)
  print('... step5 done')
  step7_create_productcategory_table(data_filename)
  print('... step7 done')
  step9_create_product_table(data_filename)
  print('... step9 done')
  step11_create_orderdetail_table(data_filename)
  print('... step11 done')