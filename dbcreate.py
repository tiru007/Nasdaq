import sqlalchemy
import pymysql

pymysql.install_as_MySQLdb()

indicies = ['nasdaq_dcs']

# mysql://admin:Radha884$@database-1.cuavh92mat1q.ap-northeast-1.rds.amazonaws.com:3306

def schemacreator(index):
    engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/')
    engine.execute(sqlalchemy.schema.CreateSchema(index))

for index in indicies:
    schemacreator(index)