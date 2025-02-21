import streamlit as st
import pandas    as pd
from ETL.Pipeline import Pipeline

pipe = Pipeline()
    
st.header("Incluir registros do EPIMED - Impacto MR", anchor=False)
arquivo_carregado = st.file_uploader( label="Selecione o arquivo",
                                      type='csv',
                                      help="Insira um arquivo no formato CSV para upload" )

if arquivo_carregado:
    csv = pd.read_csv(arquivo_carregado)    
    if pipe.valida_colunas_epimed(dataframe=csv):
        status_validacao, dados = pipe.valida_registros_epimed(dataframe=csv,competencia=arquivo_carregado.name)
        if status_validacao:
            st.info("O formato do arquivo é valido.")            
            if st.button("Carregar",use_container_width=True):
                status_salvar, qtd_registros = pipe.salva_registros_epimed(dataframe=dados,competencia=arquivo_carregado.name)
                if status_salvar:
                    st.success(f"Salvo com sucesso! {qtd_registros} registros inseridos")
                else:
                    st.error("Erro ao salvar. Informe o setor de informática.")
        else:
            st.warning("O formato de um ou mais registos está incorreto. Verifique as datas (dd/mm/yyyy hr:mi) ou o número do atendimento")
            st.table(dados)
    else:
        st.warning("A estrutura do arquivo é inválida. É esperado um arquivo CSV com 3 colunas (ATENDIMENTO, INTERNACAO, SAIDA)")
