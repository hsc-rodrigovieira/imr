import streamlit as st
import config

# Dicionário que mapeia as páginas da aplicação
pages = {
    "App": [
        st.Page(
            page="pages/baixarArquivos.py",
            title="Baixar arquivos EPIMED",
            icon=config.ICON_PAGE_DOWNLOAD
        ),
        
        st.Page(
            page="pages/consolidacaoCustos.py",
            title="Consolidação de custos",
            icon=config.ICON_PAGE_CONSOLIDACAO
        ),
        
        st.Page(
            page="pages/uploadListaEpimed.py",
            title="Incluir registros do EPIMED",
            icon=config.ICON_PAGE_UPLOAD
        )
    ]
}

# Configura o sistema de navegação da aplicação com base no dicionário de páginas
pg = st.navigation(pages)

# Executa a página atualmente selecionada na navegação
pg.run()