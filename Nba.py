from cgitb import text
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import 



# Carregando e tratando os dados NBA Payroll
nba_payroll = pd.DataFrame(pd.read_csv('c:/Users/Vildson/Documents/Engenharia_Dados/Dados/NBAplayers/NBA Payroll(1990-2023).csv'))
nba_payroll = nba_payroll.drop("Unnamed: 0", axis=1)
# removendo o $ das strings
nba_payroll['payroll'] = nba_payroll['payroll'].str[1:]
nba_payroll['inflationAdjPayroll'] = nba_payroll['inflationAdjPayroll'].str[1:]
# removendo a virgula 
nba_payroll['payroll'] = nba_payroll['payroll'].apply(lambda x: str(x).replace(',',''))
nba_payroll['inflationAdjPayroll']  = nba_payroll['inflationAdjPayroll'] .apply(lambda x: str(x).replace(',',''))
# alterando o tipo dos dados
nba_payroll['payroll'] = nba_payroll['payroll'].astype('int64')
nba_payroll['inflationAdjPayroll'] = nba_payroll['inflationAdjPayroll'].astype('int64')
#nba_payroll.set_index('ID')
nba_payroll.head()
#nba_payroll.info()

nba_payroll = nba_payroll.reset_index(drop=True).reset_index()
nba_payroll['index'] += 1

nba_payroll['date'] = '2023-04-13'
nba_payroll['date'] = pd.to_datetime(nba_payroll['date'])

# Carregando e tratando os dados NBA Box Score
nba_box = pd.DataFrame(pd.read_csv('c:/Users/Vildson/Documents/Engenharia_Dados/Dados/NBAplayers/NBA Player Box Score Stats(1950 - 2022).csv'))
nba_box = nba_box.drop("Unnamed: 0", axis=1)
#nba_box.head
#nba_box.info()

# Carregando e tratando os dados NBA Status
nba_stats = pd.DataFrame(pd.read_csv('c:/Users/Vildson/Documents/Engenharia_Dados/Desafios/NBAplayers/NBA Player Stats(1950 - 2022).csv'))
nba_stats = nba_stats.drop("Unnamed: 0", axis=1)
nba_stats = nba_stats.drop(columns=['Unnamed: 0.1'])
#nba_stats.info()
#nba_stats.head

# Carregando e tratando os dados NBA Salaries
nba_Salaries = pd.DataFrame(pd.read_csv('c:/Users/Vildson/Documents/Engenharia_Dados/Desafios/NBAplayers/NBA Salaries(1990-2023).csv'))
nba_Salaries = nba_Salaries.drop("Unnamed: 0", axis=1)
# removendo o $ das strings
nba_Salaries['salary'] = nba_Salaries['salary'].str[1:]
nba_Salaries['inflationAdjSalary'] = nba_Salaries['inflationAdjSalary'].str[1:]
# removendo a virgula 
nba_Salaries['salary'] = nba_Salaries['salary'].apply(lambda x: str(x).replace(',',''))
nba_Salaries['inflationAdjSalary']  = nba_Salaries['inflationAdjSalary'] .apply(lambda x: str(x).replace(',',''))
# alterando o tipo dos dados
nba_Salaries['salary'] = nba_Salaries['salary'].astype('int64')
nba_Salaries['inflationAdjSalary'] = nba_Salaries['inflationAdjSalary'].astype('int64')
#nba_Salaries.info()
#nba_Salaries.head()


# Definindo conexão com o banco de dados
url = 'postgresql+psycopg2://postgres:1607@localhost:5432/postgres'
engine = create_engine(url,connect_args={'options': '-csearch_path=nba_players'})

sql = '''
      CREATE TABLE nba_players.NBA_Payroll (
        id SERIAL PRIMARY KEY, 
        team VARCHAR(200), 
        seasonStartYear integer,
        payroll int4 null,
        inflationAdjPayroll integer,
        creation_date timestamp DEFAULT current_timestamp 
      );'''

df_artist =  pd.read_sql_query(sql,engine)

with engine.connect() as conn:
    query = conn.execute(text(sql))         
df = pd.DataFrame(query.fetchall())

nba.to_sql('nba_payroll',con=con, if_exists='append')



aux = pd.read_sql_query(sql, conn)

db_uri = "sqlite:///db.sqlite"
engine = create_engine(db_uri)

# create table
engine.execute('CREATE TABLE "EX1" ('
               'id INTEGER NOT NULL,'
               'name VARCHAR, '
               'PRIMARY KEY (id));')



con = conecta_db()
cur = con.cursor()



newdf.to_sql('<table_name>', con=engine, if_exists='append')

# Drop old table and create new empty table
nba.head(0).to_sql('nba_payroll', engine, if_exists='replace',index=False)
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
df.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'nba_payroll', null="") # null values become ''
conn.commit()
cur.close()
conn.close()



# Criar conexão com o banco



dbschema='public'
engine = create_engine(
    'postgresql+psycopg2://root:root@localhost/postgres'),
    connect_args={'options': '-csearch_path={}'.format(dbschema)})


lista_df = [
   "nba_payroll",
   "nba_box",
   "nba_Salaries",
   "nba_Salaries"
]






      
      '''CREATE TABLE NBA_Players.NBA_Player_Box_Score (
        id SERIAL PRIMARY KEY,
        Season INTEGER,
        Game_ID INTEGER,
        PLAYER_NAME VARCHAR(50),
        Team VARCHAR(50),
        GAME_DATE VARCHAR(50),
        MATCHUP VARCHAR(50),
        WL VARCHAR(50),
        MIN INTEGER,
        FGM INTEGER,
        FGA DECIMAL(10,2),
        FG_PCT DECIMAL(10,2),
        FG3M DECIMAL(10,2),
        FG3A DECIMAL(10,2),
        FG3_PCT DECIMAL(10,2),
        FTM INTEGER,
        FTA DECIMAL(10,2),
        FT_PCT DECIMAL(10,2),
        OREB DECIMAL(10,2),
        DREB DECIMAL(10,2),
        REB DECIMAL(10,2),
        AST DECIMAL(10,2),
        STL DECIMAL(10,2),
        BLK DECIMAL(10,2),
        TOV DECIMAL(10,2),
        PF DECIMAL(10,2),
        PTS INTEGER,
        PLUS_MINUS DECIMAL(10,2),
        VIDEO_AVAILABLE INTEGER,
        creation_date timestamp DEFAULT current_timestamp 
      );

      CREATE TABLE NBA_Players.NBA_Player_Stats (
        id SERIAL PRIMARY KEY,
        Season INTEGER,
        Player VARCHAR(80),
        Pos VARCHAR(80),
        Age VARCHAR(50),
        Tm VARCHAR(50),
        G VARCHAR(50),
        GS DECIMAL(10,2),
        MP DECIMAL(10,2),
        FG DECIMAL(10,2),
        FGA DECIMAL(10,2),
        FG DECIMAL(10,2),
        3P DECIMAL(10,2),
        3PA DECIMAL(10,2),
        3P DECIMAL(10,3),
        2P DECIMAL(10,2),
        2PA DECIMAL(10,2),
        2P DECIMAL(10,2),
        eFG DECIMAL(10,2),
        FT DECIMAL(10,2),
        FTA DECIMAL(10,2),
        FT DECIMAL(10,2),
        ORB DECIMAL(10,2),
        DRB DECIMAL(10,2),
        TRB DECIMAL(10,2),
        AST DECIMAL(10,2),
        STL DECIMAL(10,2),
        BLK DECIMAL(10,2),
        TOV DECIMAL(10,2),
        PF DECIMAL(10,2),
        PTS DECIMAL(10,2),
        creation_date timestamp DEFAULT current_timestamp 
      );
      
      CREATE TABLE NBA_Players.NBA_Salaries (
        id SERIAL PRIMARY KEY, 
        playerName         VARCHAR(80),
        seasonStartYear    INTEGER,
        salary             INTEGER,
        inflationAdjSalary INTEGER,
        creation_date timestamp DEFAULT current_timestamp 
      );
      '''
with engine.connect() as conn:
    query = conn.execute(text(Lista_create_sql))         
df = pd.DataFrame(query.fetchall())

import sqlalchemy
import pandas as pd

pip install SQLAlchemy
#pip install pandas 
pip install psycopg2

with engine.connect() as conn:
    query = conn.execute(text(Lista_create_sql))         
df = pd.DataFrame(query.fetchall())



engine = create_engine(
    'postgresql+psycopg2://root:root@localhost/postgres')

sql = '''
CREATE SCHEMA nba_players;
'''
engine.execute(Lista_create_sql)

df = pd.read_sql_query(sql,engine)


engine.execute(sql)


def conecta_db():
  con = psycopg2.connect(host='localhost', 
                         database='postgres',
                         user='postgres', 
                         password='1607')
  return con

# Função para criar schema
def criar_schema(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close() 

# Criando o schema NBA 
sql = '''
    CREATE SCHEMA nba_players
;'''
criar_schema(sql)

# Função para criar tabela no banco
def criar_db(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close()

# Dropando a tabela caso ela já exista
sql = 'DROP TABLE IF EXISTS nba_players.nba_payroll'
criar_db(sql)
# Criando a tabela dos deputados
sql = '''CREATE TABLE nba_players.nba_payroll (
        id SERIAL PRIMARY KEY, 
        team VARCHAR(100) null, 
        seasonStartYear int4 null,
        payroll int4 null,
        inflationAdjPayroll int4 null,
        creation_date timestamp DEFAULT current_timestamp null
      )'''
criar_db(sql)



def createTable(cur):
   try:
      # Criar a tabela de 
      cur.execute('''
          CREATE TABLE nba_players.nba_payroll (
          id SERIAL PRIMARY KEY, 
          team VARCHAR(100) null, 
          seasonStartYear int4 null,
          payroll int4 null,
          inflationAdjPayroll int4 null,
          creation_date timestamp DEFAULT current_timestamp null
        )'''
      )


