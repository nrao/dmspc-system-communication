import psycopg2 #allows us to connect python to postgresql databases

#connecting to my database that I initialized in pgAdmin4:
conn = psycopg2.connect(
   database="kafka", user='postgres', password='your=pgAdmin 4-password!', host='127.0.0.1', port= '5432'
)

cursor = conn.cursor()

#Creating Table DDMs and columns in sql variable:
sql ='''CREATE TABLE IF NOT EXISTS DDMs (
    DDM_id CHAR(20) NOT NULL,
    image_data BYTEA
)'''
cursor.execute(sql)
conn.commit()

conn.close()