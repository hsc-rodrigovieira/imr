import streamlit as st
import pandas    as pd
import os
import re
import config

files = pd.DataFrame(os.listdir(config.PATH_ZIP_DATA))
files.columns = ["arquivo"]
files["ordem"] = files["arquivo"].apply(lambda x: f"{re.search(config.REGEX_YEAR_PATTERN,x).group()}-{config.MONTH_MASK[re.search(config.REGEX_MONTH_PATTERN,x).group().replace(".","")]}") # type: ignore
files = files.sort_values("ordem",ascending=False).reset_index(drop=True)

st.header("Baixar arquivos EPIMED - Impacto MR", anchor=False)
st.divider()

with st.container():
    for i in range(len(files)):    
        col1, col2 = st.columns([0.7,0.3])
        if i == 0:
            col1.write(files.at[i,"arquivo"])        
            with open(config.PATH_ZIP_DATA+files.at[i,"arquivo"], "rb") as file_zip:
                col2.download_button( label="Baixar tabelas",
                                    data=file_zip,
                                    file_name=os.path.basename(config.PATH_ZIP_DATA+files.at[i,"arquivo"]),
                                    mime="application/zip",
                                    use_container_width=True )            
        else:
            col1.write(files.at[i,"arquivo"])        
            with open(config.PATH_ZIP_DATA+files.at[i,"arquivo"], "rb") as file_zip:
                col2.download_button( label="Baixar tabelas",
                                    data=file_zip,
                                    file_name=os.path.basename(config.PATH_ZIP_DATA+files.at[i,"arquivo"]),
                                    mime="application/zip",
                                    use_container_width=True ) 
        st.divider()
        