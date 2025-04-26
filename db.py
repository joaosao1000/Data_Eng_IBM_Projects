import sqlite3
import pandas as pd

conn = sqlite3.connect("STAFF.db")
table_name = "INSTRUCTOR"
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = '/home/project/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attribute_list)

df.to_sql(table_name, conn, if_exists='replace', index = False)
print('done')
conn.close()


