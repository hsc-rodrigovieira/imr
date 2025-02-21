import logging
import oracledb
import config
import datetime
import pandas as pd
import streamlit as st

class dbConfig():

    def __init__(self):
        self.db_host = st.secrets.database.HOST
        self.db_user = st.secrets.database.USERNAME
        self.db_pass = st.secrets.database.PASSWORD
        self.path_script_dispensacoes = config.PATH_SCRIPTS + config.SCRIPT_DISPENSACOES
        self.path_script_epimed = config.PATH_SCRIPTS + config.SCRIPT_EPIMED
        self.path_script_alteracoes = config.PATH_SCRIPTS + config.SCRIPT_ALTERACAO        
        self.path_script_hemo = config.PATH_SCRIPTS + config.SCRIPT_HEMODIALISE        
        self.path_processed = config.PATH_SILVER_DATA
        self.dt_format = config.DATE_FORMAT_DB
        oracledb.init_oracle_client()
        logging.basicConfig(level=logging.INFO, filename=config.PATH_LOG, format="%(asctime)s - %(levelname)s - %(message)s")
        pass

    def busca_dispensacoes(self, dt_ini:datetime.date, dt_fim:datetime.date) -> tuple[list,list]:

        rows = []
        descr = []
        columns = []

        dates = dict(dt_ini=dt_ini.strftime(self.dt_format),
                     dt_fim=(dt_fim + datetime.timedelta(15)).strftime(self.dt_format))

        try:
            with open(self.path_script_dispensacoes, "r", encoding='latin') as script_sql:            
                sqlFile = script_sql.read()
                query = sqlFile.split(';')  
                with oracledb.connect(user=self.db_user, password=self.db_pass, dsn=self.db_host) as connection:
                    try:
                        with connection.cursor() as cursor:
                            for s in range(len(query)):
                                cursor.execute(query[s],dates)
                                rows.append(cursor.fetchall())
                                descr.append(cursor.description)
                                columns.append([x[0] for x in descr[s]])                            
                    except ValueError as e:
                        logging.error(f"Erro ao processar os dados. {e}")
                    except ConnectionError as e:
                        logging.error(f"Falha na conexao com o banco de dados. {e}")
        except FileNotFoundError as e:
            logging.error(f"Erro ao ler script. {e}")
        logging.info("Sucesso na coleta da dispensações")
        return rows, columns
    
    def busca_atendimentos_epimed(self, atendimentos:str) -> tuple[list,list]:
        rows = []
        descr = []
        columns = []
        try:
            with open(self.path_script_epimed, "r", encoding='latin') as script_sql:            
                sqlFile = script_sql.read().replace("\n", "").replace("\t", "")
                query = sqlFile.format(atendimentos)
                with oracledb.connect(user=self.db_user, password=self.db_pass, dsn=self.db_host) as connection:
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(query)
                            rows.append(cursor.fetchall())
                            descr.append(cursor.description)
                            columns.append([x[0] for x in descr[0]])                            
                    except ValueError as e:
                        logging.error(f"Erro ao processar os dados. {e}")
                    except ConnectionError as e:
                        logging.error(f"Falha na conexao com o banco de dados. {e}")
        except FileNotFoundError as e:
            logging.error(f"Erro ao ler script. {e}")     
            return [],[]
        logging.info("Sucesso na coleta dos atendimentos")
        return rows, columns   

    def busca_atendimentos_corrigidos(self, df_atendimentos:pd.DataFrame, dt_ini:datetime.date) -> pd.DataFrame:        
        try:
            with open(self.path_script_alteracoes, "r", encoding='latin') as script_sql:
                sqlFile = script_sql.read().replace("\n", "").replace("\t", "")
                query = sqlFile
                for i in range(len(df_atendimentos)):                    
                    rows = []
                    params = dict(cd_paciente=int(df_atendimentos.at[i,'cd_paciente']),
                                  cd_atendimento=int(df_atendimentos.at[i,'cd_atendimento']),
                                  dt_ini=str(df_atendimentos.at[i,'dt']))                    
                    with oracledb.connect(user=self.db_user, password=self.db_pass, dsn=self.db_host) as connection:
                        try:
                            with connection.cursor() as cursor:
                                cursor.execute(query,params)
                                rows.append(cursor.fetchall())                       
                        except ValueError as e:
                            logging.error(f"Erro ao processar os dados. {e}")
                        except ConnectionError as e:
                            logging.error(f"Falha na conexao com o banco de dados. {e}")
                    if len(rows[0]) == 0:
                        df_atendimentos.at[i,'cd_atendimento_real'] = 0
                    else:                        
                        df_atendimentos.at[i,'cd_atendimento_real'] = rows[0][0][0]
                df_atendimentos['cd_atendimento_real'] = df_atendimentos['cd_atendimento_real'].astype('int64')            
        except FileNotFoundError as e:
            logging.error(f"Erro ao ler script. {e}")
            return pd.DataFrame()        
        path_file = self.path_processed+f"{dt_ini.year}/{dt_ini.month}/"
        df_atendimentos.to_csv(path_file+f"atendimentos_corrigidos_{dt_ini.strftime("%B")}.csv", index=False)
        logging.info("Sucesso na coleta dos atendimentos")
        return df_atendimentos  

    def busca_sessoes_hemodialise(self, dt_ini:datetime.date, dt_fim:datetime.date) -> tuple[list,list]:
        rows = []
        descr = []
        columns = []
        
        dates = dict(dt_ini=dt_ini.strftime(self.dt_format),
                     dt_fim=dt_fim.strftime(self.dt_format))        

        try:
            with open(self.path_script_hemo, "r", encoding='latin') as script_sql:            
                sqlFile = script_sql.read().replace("\n", "").replace("\t", "")
                query = sqlFile
                with oracledb.connect(user=self.db_user, password=self.db_pass, dsn=self.db_host) as connection:
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(query,dates)
                            rows.append(cursor.fetchall())
                            descr.append(cursor.description)
                            columns.append([x[0] for x in descr[0]])                            
                    except ValueError as e:
                        logging.error(f"Erro ao processar os dados. {e}")
                    except ConnectionError as e:
                        logging.error(f"Falha na conexao com o banco de dados. {e}")
        except FileNotFoundError as e:
            logging.error(f"Erro ao ler script. {e}")     
            return [],[]
        logging.info("Sucesso na coleta dos atendimentos")
        return rows, columns                    