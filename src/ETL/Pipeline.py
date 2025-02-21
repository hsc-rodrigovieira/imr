import logging
import config
import datetime
import locale
import os
import re
import zipfile
import calendar
import pandas as pd

class Pipeline():

    def __init__(self) -> None:
        self.path_epimed = config.PATH_EXTERNAL_DATA
        self.path_raw = config.PATH_BRONZE_DATA
        self.path_processed = config.PATH_SILVER_DATA
        self.path_export = config.PATH_GOLD_DATA
        self.path_zip = config.PATH_ZIP_DATA
        logging.basicConfig(level=logging.INFO, filename=config.PATH_LOG, format="%(asctime)s - %(levelname)s - %(message)s", encoding='utf-8')
        locale.setlocale(locale.LC_TIME,"pt_BR")
        pass

    def cria_repositorio(self, path:str) -> bool:
        try:
            # Cria o diretório para salvar os arquivos CSV
            os.mkdir(path)
            logging.info(f"cria_repositorio: Criado diretorio em {path}")
            return True
        except OSError as e:
            if len(os.listdir(path)) > 0:
                logging.warning(f"cria_repositorio: Diretorio ja existe. {e}")               
                return True
            else:
                logging.warning(f"cria_repositorio: Nao foi possivel criar o diretorio. {e}")          
                return False

    def formata_dados_brutos(self, rows:list, columns:list, dt_ini:datetime.date) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            df_dados = pd.DataFrame(data=rows[0], columns=[c.lower() for c in columns[0]])
            df_estoque = pd.DataFrame(data=rows[1], columns=[c.lower() for c in columns[1]])
            df_saidas = df_estoque.loc[df_estoque['tp_mov']=='SAIDA'].reset_index(drop=True).copy()
            df_devolucoes = df_estoque.loc[df_estoque['tp_mov']=='DEVOLUCAO'].reset_index(drop=True).copy() 
            path_file = self.path_raw+f"{dt_ini.year}/{dt_ini.month}/"
            if self.cria_repositorio(path_file):
                df_dados.to_csv(path_file+f"dados_{dt_ini.strftime("%B")}.csv", index=False)
                df_saidas.to_csv(path_file+f"saidas_{dt_ini.strftime("%B")}.csv", index=False)
                df_devolucoes.to_csv(path_file+f"devolucoes_{dt_ini.strftime("%B")}.csv", index=False)
                return df_dados, df_saidas, df_devolucoes
            else:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()    
        except ValueError as e:
            logging.error(f"formata_dados_brutos: Erro ao processar os dados. {e}")     
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()    

    def valida_devolucoes(self, df_devolucoes:pd.DataFrame, df_saidas:pd.DataFrame, colunas:list, dt_ini:datetime.date) -> pd.DataFrame:
        colunas_esperadas = ['cd_atendimento','ds_unid_int','tp_classificacao','tp_mov','cd_produto','ds_produto','dt_gravacao','qt_movimentacao','unid_ref','vl_custo_medio']
        
        if all(df_devolucoes.columns.isin(colunas_esperadas)):
            for i in range(len(df_devolucoes)):
                cd_atd   = df_devolucoes.at[i,'cd_atendimento']
                cd_prod  = df_devolucoes.at[i,'cd_produto']
                dt_mov   = df_devolucoes.at[i,'dt_gravacao']
                devolver = df_devolucoes.at[i,'qt_movimentacao']
                
                while devolver > 0:
                    aux = df_saidas.loc[(df_saidas['cd_atendimento']==cd_atd) & (df_saidas['cd_produto']==cd_prod) & (df_saidas['dt_gravacao']<=dt_mov)].tail(1)
                    if aux.empty:
                        devolver = 0
                    else:            
                        saldo = aux['qt_movimentacao'].values[0]            
                        if saldo <= devolver:
                            df_saidas = df_saidas.drop(aux.index)
                            devolver-=saldo
                        else:
                            df_saidas.iloc[aux.index[0],7] = saldo-devolver
                            devolver=0
                    df_saidas = df_saidas.reset_index(drop=True)
            
            df_saidas = df_saidas.drop('tp_mov',axis=1)
            df_saidas = df_saidas.sort_values(['cd_atendimento','tp_classificacao','dt_gravacao','ds_produto'])
            df_saidas['dt_gravacao'] = pd.to_datetime(df_saidas['dt_gravacao'])#.dt.strftime("%d/%m/%Y")
            df_saidas = df_saidas.drop(df_saidas.loc[df_saidas['dt_gravacao']>=pd.to_datetime(dt_ini)].index[0])
            df_saidas['vl_total'] = df_saidas['qt_movimentacao'] * df_saidas['vl_custo_medio']
            df_saidas.columns = colunas
            return df_saidas.reset_index(drop=True)
        else:
            logging.error("valida_devolucoes: Erro ao processar os dados. Dataframe não contém as colunas esperadas")     
            return pd.DataFrame()
        
    def monta_dispensacao(self, df_dados:pd.DataFrame, df_saidas:pd.DataFrame, dt_ini:datetime.date) -> pd.DataFrame:
        colunas_esperadas = ['cd_atendimento', 'ds_unid_int', 'tp_classificacao', 'cd_produto','ds_produto',
                             'dt_consumo', 'qt_movimentacao', 'unid_ref','vl_unitario', 'vl_total']
        if all(df_dados.columns.isin(colunas_esperadas)) and all(df_saidas.columns.isin(colunas_esperadas)): 
            df_resultado_sem_epimed = pd.concat([df_dados,df_saidas],axis=0).sort_values(['cd_atendimento','tp_classificacao','dt_consumo','ds_produto']).reset_index(drop=True)
            df_resultado_sem_epimed = df_resultado_sem_epimed.drop_duplicates()
            
            path_file = self.path_processed+f"{dt_ini.year}/{dt_ini.month}/"
            if self.cria_repositorio(path_file):
                df_resultado_sem_epimed.to_csv(path_file+f"dispensacao_total_{dt_ini.strftime("%B")}.csv", index=False)
                return df_resultado_sem_epimed
            else:
                return pd.DataFrame()
        else:
            logging.error("monta_dispensacao: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return pd.DataFrame()
        
    def importa_lista_epimed(self, dt_ini:datetime.date) -> pd.DataFrame:
        colunas_esperadas = ['atendimento', 'internacao', 'saida']
        lista_epimed = pd.DataFrame()

        if dt_ini.month == 1:
            path_epimed_mes_1_antes = self.path_epimed+f"{dt_ini.year-1}/12.csv"
            path_epimed_mes_2_antes = self.path_epimed+f"{dt_ini.year-1}/11.csv"
        else:
            path_epimed_mes_1_antes = self.path_epimed+f"{dt_ini.year}/{dt_ini.month-1}.csv"
            path_epimed_mes_2_antes = self.path_epimed+f"{dt_ini.year}/{dt_ini.month-2}.csv"
        
        path_epimed_mes_atual = self.path_epimed+f"{dt_ini.year}/{dt_ini.month}.csv"

        try:
            if dt_ini.year-1 == 2023:
                epimed_mes_2_antes = pd.DataFrame()
            else:
                epimed_mes_2_antes = pd.read_csv(path_epimed_mes_2_antes,encoding='utf-8')
                epimed_mes_2_antes.columns = [str.replace(col.lower(),' ','_') for col in epimed_mes_2_antes.columns]            

            epimed_mes_1_antes = pd.read_csv(path_epimed_mes_1_antes,encoding='utf-8')
            epimed_mes_1_antes.columns = [str.replace(col.lower(),' ','_') for col in epimed_mes_1_antes.columns]

            epimed_mes_atual = pd.read_csv(path_epimed_mes_atual,encoding='utf-8')
            epimed_mes_atual.columns = [str.replace(col.lower(),' ','_') for col in epimed_mes_atual.columns]

        except FileNotFoundError as e:
            logging.error(f"importa_lista_epimed: Erro ao importar os dados. {e}")
            return pd.DataFrame()

        if epimed_mes_2_antes.empty:
            if all((epimed_mes_1_antes.columns.isin(colunas_esperadas)) & (epimed_mes_atual.columns.isin(colunas_esperadas))):
                lista_epimed = pd.concat([epimed_mes_1_antes,epimed_mes_atual]).reset_index(drop=True)
            else:
                logging.error("importa_lista_epimed: Erro ao processar os dados. Tabela do arquivo não contém as colunas esperadas")
                return pd.DataFrame()
        else:
            if all((epimed_mes_2_antes.columns.isin(colunas_esperadas)) & (epimed_mes_1_antes.columns.isin(colunas_esperadas)) & (epimed_mes_atual.columns.isin(colunas_esperadas))):
                lista_epimed = pd.concat([epimed_mes_2_antes,epimed_mes_1_antes,epimed_mes_atual]).reset_index(drop=True)
            else:
                logging.error("importa_lista_epimed: Erro ao processar os dados. Tabela do arquivo não contém as colunas esperadas")
                return pd.DataFrame()

        if not lista_epimed.empty:
            lista_epimed['internacao'] = pd.to_datetime(lista_epimed['internacao'],dayfirst=True).dt.normalize()
            lista_epimed['saida'] = pd.to_datetime(lista_epimed['saida'],dayfirst=True).dt.normalize()            
            lista_epimed['valida'] = lista_epimed.apply(lambda x: 1 if (dt_ini.month >= pd.to_datetime(x['internacao'],dayfirst=True).month) or (dt_ini.month <= pd.to_datetime(x['saida'],dayfirst=True).month) else None, axis = 1 ) # type: ignore
            lista_epimed = lista_epimed.dropna(subset='valida').reset_index(drop=True).drop('valida',axis=1)
            return lista_epimed
        else:
            logging.error("importa_lista_epimed: Erro ao processar os dados. Tabela vazia.")
            return pd.DataFrame()
        
    def valida_epimed(self, df_epimed:pd.DataFrame, df_atendimentos_inconsistentes:pd.DataFrame) -> pd.DataFrame:
        colunas_esperadas_atendimentos = ['cd_atendimento', 'pac', 'cd_paciente', 'dt', 'tp_atend', 'cd_atendimento_real']
        colunas_esperadas_epimed = ['atendimento', 'internacao', 'saida']
        
        if all(df_epimed.columns.isin(colunas_esperadas_epimed)) and all(df_atendimentos_inconsistentes.columns.isin(colunas_esperadas_atendimentos)):
            try:
                df_epimed['cd_atendimento'] = df_epimed['atendimento'].apply(lambda x: x if df_atendimentos_inconsistentes.loc[df_atendimentos_inconsistentes['cd_atendimento']==x].empty
                                                                                       else df_atendimentos_inconsistentes.loc[df_atendimentos_inconsistentes['cd_atendimento']==x,'cd_atendimento_real'].squeeze()) # type: ignore
                df_epimed = df_epimed.drop(df_epimed.loc[df_epimed['cd_atendimento']==0].index).reset_index(drop=True)                
                return df_epimed.drop('atendimento',axis=1)
            except ValueError as e:
                logging.error(f"valida_epimed: Erro ao validar os dados. {e}")
                return pd.DataFrame()
        else:
            logging.error("valida_epimed: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return pd.DataFrame()            
        
    def filtra_dispensacao_epimed(self, df_dispensacao:pd.DataFrame, df_epimed:pd.DataFrame) -> pd.DataFrame:
        colunas_esperadas_dispensacao = ['cd_atendimento', 'ds_unid_int', 'tp_classificacao',
                                         'cd_produto', 'ds_produto', 'dt_consumo', 'qt_movimentacao',
                                         'unid_ref','vl_unitario', 'vl_total']
        colunas_esperadas_epimed = ['cd_atendimento', 'internacao', 'saida']
        
        if all(df_epimed.columns.isin(colunas_esperadas_epimed)) and all(df_dispensacao.columns.isin(colunas_esperadas_dispensacao)):
            df_dispensacao['valida'] = df_dispensacao[['cd_atendimento','dt_consumo']].apply(
                lambda x: True if ((pd.to_datetime(x['dt_consumo'],dayfirst=True) >= df_epimed.loc[df_epimed['cd_atendimento']==x['cd_atendimento'],'internacao']) &
                                   (pd.to_datetime(x['dt_consumo'],dayfirst=True) <= df_epimed.loc[df_epimed['cd_atendimento']==x['cd_atendimento'],'saida'])).any() else False, axis=1
                )
            return df_dispensacao
        else:
            logging.error("filtra_dispensacao_epimed: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return pd.DataFrame()    

    def monta_dataframe_final(self, df_dispensacao:pd.DataFrame, dt_ini:datetime.date, dt_fim:datetime.date) -> pd.DataFrame:
        colunas_esperadas = ['cd_atendimento', 'ds_unid_int', 'tp_classificacao',
                             'cd_produto', 'ds_produto', 'dt_consumo', 'qt_movimentacao',
                             'unid_ref','vl_unitario', 'vl_total', 'valida']
        
        if all(df_dispensacao.columns.isin(colunas_esperadas)):
            df = df_dispensacao.loc[df_dispensacao['valida'] == True].drop('valida',axis=1)
            df['dt_consumo'] = pd.to_datetime(df['dt_consumo'],dayfirst=True)
            df_final = df.loc[(df['dt_consumo'].between(pd.to_datetime(dt_ini),pd.to_datetime(dt_fim)))&(df['qt_movimentacao'] > 0)].reset_index(drop=True)
            df_final['dt_consumo'] = df['dt_consumo'].dt.strftime("%d/%m/%Y")
            
            path_file = self.path_processed+f"{dt_ini.year}/{dt_ini.month}/"
            if self.cria_repositorio(path_file):
                df_final.to_csv(path_file+f"mvto_estoque_final_{dt_ini.strftime("%B")}.csv", index=False)
                return df_final
            else:
                return pd.DataFrame()                        
        else:
            logging.error("monta_dataframe_final: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return pd.DataFrame()
    
    def exporta_dados_revisao(self, df:pd.DataFrame, dt_ini:datetime.date) -> bool:
        colunas_esperadas = ['cd_atendimento', 'ds_unid_int', 'tp_classificacao',
                             'cd_produto', 'ds_produto', 'dt_consumo', 'qt_movimentacao',
                             'unid_ref','vl_unitario', 'vl_total']     
        path_file = self.path_export+f"{dt_ini.year}/{dt_ini.month}/"
        
        if all(df.columns.isin(colunas_esperadas)):
            df.columns = config.COLUNAS_EXPORTAR            
            if self.cria_repositorio(path_file):
                df.to_excel(path_file+f"dispensacoes_{dt_ini.strftime("%B")}_validacao.xlsx", sheet_name=f'{dt_ini.strftime("%b")}-{dt_ini.strftime("%y")}', index=False, float_format='%.4f')
                return True
            else:
                return False
        else:
            logging.error("exporta_dados_revisao: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return False
    
    def exporta_dados_envio(self, df:pd.DataFrame, dt_ini:datetime.date) -> bool:
        colunas_esperadas = ['cd_atendimento', 'ds_unid_int', 'tp_classificacao',
                             'cd_produto', 'ds_produto', 'dt_consumo', 'qt_movimentacao',
                             'unid_ref','vl_unitario', 'vl_total']        
        path_file = self.path_export+f"{dt_ini.year}/{dt_ini.month}/"
        
        if all(df.columns.isin(colunas_esperadas)):
            df.columns = config.COLUNAS_EXPORTAR
            df = df.drop('Setor',axis=1)
            df['Quantidade'] = df[['Classificação','Quantidade']].apply(lambda x: 1 if x['Classificação']=='COZINHA' else x['Quantidade'], axis=1)
            df['Unidade'] = df[['Classificação','Unidade']].apply(lambda x: "" if x['Classificação']=='COZINHA' else x['Unidade'], axis=1)
            df['Custo Unitário'] = df[['Classificação','Custo Unitário','Custo Total']].apply(lambda x: x['Custo Total'] if x['Classificação']=='COZINHA' else x['Custo Unitário'], axis=1)
            df['Classificação'] = df['Classificação'].apply(lambda x: 'MEDICAMENTOS' if x == 'COZINHA' else 'MATERIAL HOSPITALAR' if x =='MATERIAL OPME' else x)
            df['Data do Consumo'] = pd.to_datetime(df['Data do Consumo'],dayfirst=True).dt.strftime('%d/%m/%Y')

            if self.cria_repositorio(path_file):
                df.to_excel(path_file+f"dispensacoes_{dt_ini.strftime("%B")}_envio.xlsx", sheet_name=f'{dt_ini.strftime("%b")}-{dt_ini.strftime("%y")}', index=False, float_format='%.4f')
                return True
            else:
                return False
        else:
            logging.error("exporta_dados_envio: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return False   

    def monta_resumo(self, df:pd.DataFrame, dt_ini:datetime.date) -> bool:
        colunas_esperadas = ['tp_classificacao','vl_total']
        path_file = self.path_export+f"{dt_ini.year}/{dt_ini.month}/"

        if all(df.columns.isin(colunas_esperadas)):     
            resumo = df.groupby('tp_classificacao').sum().reset_index()
            resumo['vl_total'] = resumo['vl_total'].apply(lambda x: "R$ {0:,.2f}".format(float(x)))
            resumo.columns = ['Classificação','Custo Total']
            if self.cria_repositorio(path_file):
                resumo.to_excel(path_file+f"resumo_{dt_ini.strftime("%B")}.xlsx", index=False, sheet_name=f"resumo {dt_ini.strftime("%B")}")                
                return True
            else:
                return False
        else:
            logging.error("monta_resumo: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return False

    def monta_correcoes(self, df:pd.DataFrame, dt_ini:datetime.date) -> bool:
        colunas_esperadas = ['cd_atendimento','pac','tp_atend','cd_atendimento_real']   
        path_file = self.path_export+f"{dt_ini.year}/{dt_ini.month}/"

        if all(df.columns.isin(colunas_esperadas)):
            df.columns = ['Atendimento EPIMED','Paciente','Tipo','Orientação']
            df['Orientação'] = df['Orientação'].apply(lambda x: "Não foi localizada internação correlacionada. Favor remover do EPIMED." if x==0 else f"Favor substituir pela internação {x}")
            if self.cria_repositorio(path_file):
                df.to_excel(path_file+f"correcoes_{dt_ini.strftime("%B")}.xlsx", index=False, sheet_name=f"correcoes {dt_ini.strftime("%B")}")                
                return True
            else:
                return False
        else:
            logging.error("monta_correcoes: Erro ao processar os dados. Dataframe não contém as colunas esperadas")
            return False
        
    def exporta_zip(self, path_dict:dict, dt_ini:datetime.date) -> str:
        chaves_necessarias = ["path_file_validacao","path_file_envio","path_file_hemodialise","path_file_resumo","path_file_correcoes"]        

        if all(pd.Series(path_dict.keys()).isin(chaves_necessarias)):
            try:
                path_downloads = self.path_zip + f"dados_{dt_ini.strftime("%Y")}_{dt_ini.strftime("%B")}.zip"
                with zipfile.ZipFile(path_downloads,'w',zipfile.ZIP_DEFLATED) as zp:
                    for p in path_dict.keys():
                        zp.write(path_dict[p], os.path.basename(path_dict[p]))
                return path_downloads
            except FileNotFoundError as e:
                logging.error(f"exporta_zip: Erro ao criar arquivo zipado para download. {e}")
                return ""
        else:
            logging.error("exporta_zip: Erro ao processar dados. Dicionário não contém todos os caminhos de arquivos necessários.")
            return ""
        
    def check_complete(self, dt_ini:datetime.date):
        return f'dados_{dt_ini.year}_{dt_ini.strftime("%B")}.zip' in os.listdir(self.path_zip)
    
    def elapsed_time(self, start:datetime.datetime) -> str:
        elp_time = datetime.datetime.now() - start
        if elp_time.total_seconds() >= 60:
            return f"{(elp_time.total_seconds())/60:.2f}m"
        else:
            return f"{elp_time.total_seconds():.2f}s"
        
    def formata_sessoes_hemodialise(self, rows:list, columns:list, dt_ini:datetime.date) -> pd.DataFrame:
        try:
            df = pd.DataFrame(data=rows[0], columns=[c.lower() for c in columns[0]])              
            df.columns = ['Mês','Total de sessões de hemodiálise']
            df['Mês'] = df['Mês'].dt.strftime("%B")
            df['Mês'] = df['Mês'].apply(lambda x: str.capitalize(x))
            path_file = self.path_export+f"{dt_ini.year}/{dt_ini.month}/"
            if self.cria_repositorio(path_file):
                df.to_excel(path_file+f"sessoes_hemodialise_{dt_ini.strftime("%B")}.xlsx", index=False, sheet_name=f"hemo {dt_ini.strftime("%B")}")                
                return df
            else:
                return pd.DataFrame()
        except ValueError as e:
            logging.error(f"formata_sessoes_hemodialise: Erro ao processar os dados. {e}")     
            return pd.DataFrame()
        
    def valida_colunas_epimed(self, dataframe:pd.DataFrame) -> bool:
        colunas_obrigatorias = config.COLUNAS_EPIMED              
        if all(dataframe.columns.isin(colunas_obrigatorias)):
            return True
        else:
            return False
        
    def valida_registros_epimed(self, dataframe:pd.DataFrame, competencia:str) -> tuple[bool, pd.DataFrame]:
        valores_competencia = str.split(str(competencia).replace(".csv",""),'-')
        ano = int(valores_competencia[0])
        mes = int(valores_competencia[1])

        dataframe.fillna({'SAIDA':datetime.date(ano, mes, calendar.monthrange(ano, mes)[1]).strftime("%d/%m/%Y %H:%M")},inplace=True)     
        dataframe['valida'] = dataframe.apply(lambda x: 1 if (re.match(pattern=config.REGEX_DATE_PATTERN,string=x['INTERNACAO'])) 
                                                         and (re.match(pattern=config.REGEX_DATE_PATTERN,string=x['SAIDA'])) 
                                                         and (re.match(pattern=config.REGEX_ATENDIMENTO_PATTERN,string=str(x['ATENDIMENTO']))) else 0,axis=1)
        if len(dataframe) == dataframe['valida'].sum():
            return True, dataframe.drop('valida',axis=1).reset_index(drop=True)
        else:
            return False, dataframe.loc[dataframe['valida']==0].drop('valida',axis=1).reset_index(drop=True)
        
    def salva_registros_epimed(self, dataframe:pd.DataFrame, competencia:str) -> tuple[bool, int]:
        valores_competencia = str.split(str(competencia).replace(".csv",""),'-')
        ano = int(valores_competencia[0])
        mes = int(valores_competencia[1])

        path_file = self.path_epimed + f"{ano}/"
        if self.cria_repositorio(path_file):
            dataframe.to_csv(f"{path_file}{mes}.csv",index=False,mode="w")
            logging.info(f"salva_registros_epimed: Arquivo salvo em {path_file}{mes}.csv")
            return True, len(dataframe)
        else:
            return False, 0