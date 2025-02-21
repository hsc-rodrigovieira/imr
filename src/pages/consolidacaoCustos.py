import os
import config
import datetime
import calendar
import streamlit as st
import pandas    as pd
from ETL.dbConfig import dbConfig
from ETL.Pipeline import Pipeline

db = dbConfig()
pipe = Pipeline()

today = datetime.datetime.now()
next_year = today.year + 1
first_value = datetime.date(today.year, today.month, 1)
last_value = datetime.date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
jan_1 = datetime.date(2024, 1, 1)
dec_31 = datetime.date(next_year, 12, 31)
status_etl = ''
path_export = config.PATH_GOLD_DATA
path_download = config.PATH_ZIP_DATA

if 'btn_start' in st.session_state and st.session_state.btn_start == True:
    st.session_state.running = True
else:
    st.session_state.running = False
    
st.header("Consolidação de custos - Impacto MR", anchor=False)

date_filter = st.date_input( label="Período",
                             value=(first_value,last_value),
                             min_value=jan_1,
                             max_value=dec_31,
                             format="DD.MM.YYYY" )

if pipe.check_complete(dt_ini=date_filter[0]):
    st.warning("Esta competência já foi consolidada. Deseja gerar novamente?")
    col_btn_baixar, col_btn_gerar_novamente = st.columns(2)
    with col_btn_baixar:
        path_download = path_download+f'dados_{date_filter[0].year}_{date_filter[0].strftime("%B")}.zip'
        with open(path_download, "rb") as file_zip:
            btn_validacao = st.download_button( label="Baixar tabelas",
                                                data=file_zip,
                                                file_name=os.path.basename(path_download),
                                                mime="application/zip",
                                                key='btn_download',
                                                use_container_width=True )    
    with col_btn_gerar_novamente:
        btn_start = st.button("Gerar novamente",
                              disabled=st.session_state.running,
                              key='btn_start',
                              use_container_width=True)    
else:
    btn_start = st.button("Gerar",
                          disabled=st.session_state.running,
                          key='btn_start',
                          use_container_width=True)
    
if btn_start:        
    with st.status("Iniciando...", expanded=True) as status:
        col_status, col_elp_time = st.columns([0.9,0.1])            
        inicio = datetime.datetime.now()
        col_status.write("Iniciado")    
        col_elp_time.caption(f"{inicio.strftime("%X")}") 
        
        status.update(label="Extraindo base de dados....", state="running")
        rows_dispensacoes, columns_dispensacoes = db.busca_dispensacoes(dt_ini=date_filter[0],
                                                                        dt_fim=date_filter[1])
        rows_hemodialise, columns_hemodialise = db.busca_sessoes_hemodialise(dt_ini=date_filter[0],
                                                                             dt_fim=date_filter[1])
        df_dados, df_saidas, df_devolucoes = pipe.formata_dados_brutos(rows=rows_dispensacoes,
                                                                       columns=columns_dispensacoes,
                                                                       dt_ini=date_filter[0])
        df_hemodialise = pipe.formata_sessoes_hemodialise(rows=rows_hemodialise,
                                                          columns=columns_hemodialise,
                                                          dt_ini=date_filter[0])        
        col_status.write("Concluída extração da base de dados")    
        col_elp_time.caption(f"{pipe.elapsed_time(inicio)}") 
        anterior = datetime.datetime.now()
        
        status.update(label="Validando movimentações de estoque...", state="running")
        devolucoes_validadas = pipe.valida_devolucoes(df_devolucoes=df_devolucoes,
                                                        df_saidas=df_saidas,
                                                        colunas=list(df_dados.columns),
                                                        dt_ini=date_filter[0])   
        df_dispensacao = pipe.monta_dispensacao(df_dados=df_dados,
                                                df_saidas=devolucoes_validadas,
                                                dt_ini=date_filter[0])
        col_status.write("Validadas movimentações de estoque")    
        col_elp_time.caption(f"{pipe.elapsed_time(anterior)}")
        anterior = datetime.datetime.now()

        status.update(label="Validando atendimentos no EPIMED...", state="running")     
        lista_epimed = pipe.importa_lista_epimed(dt_ini=date_filter[0])
        atendimentos = ','.join(map(str,lista_epimed['atendimento']))
        rows_, columns_ = db.busca_atendimentos_epimed(atendimentos=atendimentos)
        atendimentos_errados = pd.DataFrame(data=rows_[0],
                                            columns=[c.lower() for c in columns_[0]])
        atendimentos_corrigidos = db.busca_atendimentos_corrigidos(df_atendimentos=atendimentos_errados,
                                                                   dt_ini=date_filter[0])
        df_epimed = pipe.valida_epimed(df_epimed=lista_epimed,
                                       df_atendimentos_inconsistentes=atendimentos_corrigidos)
        col_status.write("Validados atendimentos no EPIMED")    
        col_elp_time.caption(f"{pipe.elapsed_time(anterior)}")
        anterior = datetime.datetime.now()

        status.update(label="Validando dados no EPIMED com as movimentações de estoque...", state="running") 
        df_filtrado = pipe.filtra_dispensacao_epimed(df_dispensacao=df_dispensacao,
                                                     df_epimed=df_epimed)
        df_final = pipe.monta_dataframe_final(df_dispensacao=df_filtrado,
                                              dt_ini=date_filter[0],
                                              dt_fim=date_filter[1])
        col_status.write("Dados no EPIMED validados com as movimentações de estoque")    
        col_elp_time.caption(f"{pipe.elapsed_time(anterior)}")
        anterior = datetime.datetime.now()

        status.update(label="Exportando Excel...", state="running") 
        pipe.exporta_dados_revisao(df=df_final.copy(),
                                   dt_ini=date_filter[0])
        pipe.exporta_dados_envio(df=df_final.copy(),
                                 dt_ini=date_filter[0])
        resumo = pipe.monta_resumo(df=df_final[['tp_classificacao','vl_total']],
                                   dt_ini=date_filter[0])
        correcoes = pipe.monta_correcoes(df=atendimentos_corrigidos[['cd_atendimento','pac','tp_atend','cd_atendimento_real']].copy(),
                                         dt_ini=date_filter[0])
        col_status.write("Dados exportados em Excel")    
        col_elp_time.caption(f"{pipe.elapsed_time(anterior)}")
        final = datetime.datetime.now()

        col_status.write("Finalizado!")    
        col_elp_time.caption(f"{final.strftime("%X")} | {pipe.elapsed_time(inicio)}")
        status.update(label="Finalizado!", state="complete")
        status_etl = status._current_state

if status_etl == 'complete':
    path_file_validacao   = path_export+f"{date_filter[0].year}/{date_filter[0].month}/dispensacoes_{date_filter[0].strftime("%B")}_validacao.xlsx"
    path_file_envio       = path_export+f"{date_filter[0].year}/{date_filter[0].month}/dispensacoes_{date_filter[0].strftime("%B")}_envio.xlsx"
    path_file_hemodialise = path_export+f"{date_filter[0].year}/{date_filter[0].month}/sessoes_hemodialise_{date_filter[0].strftime("%B")}.xlsx"
    path_file_resumo      = path_export+f"{date_filter[0].year}/{date_filter[0].month}/resumo_{date_filter[0].strftime("%B")}.xlsx"
    path_file_correcoes   = path_export+f"{date_filter[0].year}/{date_filter[0].month}/correcoes_{date_filter[0].strftime("%B")}.xlsx"

    dict_path = dict(path_file_validacao = path_file_validacao,
                     path_file_envio = path_file_envio,
                     path_file_hemodialise = path_file_hemodialise,
                     path_file_resumo = path_file_resumo,
                     path_file_correcoes = path_file_correcoes)

    path_download = pipe.exporta_zip(path_dict=dict_path,
                                     dt_ini=date_filter[0])

    if path_download:
        with open(path_download, "rb") as file_zip:
            btn_validacao = st.download_button( label="Baixar tabelas",
                                                data=file_zip,
                                                file_name=os.path.basename(path_download),
                                                mime="application/zip",
                                                use_container_width=True )    
    else:
        st.warning("Erro ao gerar arquivos para download") 

    