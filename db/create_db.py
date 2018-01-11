from sqlalchemy import create_engine
import gitpath
import pandas as pd


host = "127.0.0.1"
user = "root"
port = 3306
pw = ''
connection = create_engine("mysql+pymysql://{}:{}@{}:{}".format(user, pw, host, port))

table_creation = """
                 CREATE TABLE IF NOT EXISTS `agrovoc_autocode`.`agris_data` (
                     `id` INT NOT NULL AUTO_INCREMENT,
                     `doc_id` VARCHAR(400) NULL,
                     `text` NVARCHAR(4000) NULL,
                     `codes` VARCHAR(4000) NULL,
                     `page` INT NULL,
                     `search_term` VARCHAR(100) NULL,
                     PRIMARY KEY (`id`));
                 """

connection.execute("""CREATE DATABASE IF NOT EXISTS agrovoc_autocode""")
connection = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(user, '', host, 3306, "agrovoc_autocode"))
connection.execute(table_creation)


def execute_scripts_from_file(filename):
    fd = open(filename, 'r')
    sql_file = fd.read()
    fd.close()
    sql_commands = sql_file.split(';')

    for command in sql_commands:
        if command.strip() != '':
            connection.execute(command)


def populate_table_from_csv(csv):
    terms = 'agrovoc_terms'
    if terms not in connection.table_names():
        df = pd.read_csv(csv).fillna("")
        df.to_sql(name=terms, con=connection)


connection.execute("""CREATE TABLE IF NOT EXISTS `agrovoc`.`agrovoc_terms`(
    L1 VARCHAR(50),
    L2 VARCHAR(50),
    L3 VARCHAR(50),
    L4 VARCHAR(50),
    L5 VARCHAR(50),
    L6 VARCHAR(50),
    L7 VARCHAR(50),
    Code VARCHAR(50),
    `Use?` VARCHAR(2)
    )""")

populate_table_from_csv('{}/db/agris_data.csv'.format(gitpath.root()))
execute_scripts_from_file('{}/db/create_hierarchy_table.sql'.format(gitpath.root()))
