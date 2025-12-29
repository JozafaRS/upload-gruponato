import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from decouple import config
from slugify import slugify

DB_URL = config("DB_URL")
TABLE_NAME = config("TABLE_NAME")

engine = create_engine(DB_URL)

def validate_data_frame(data_frame: pd.DataFrame) -> None:
    EXPECTED_COLUMNS = [
        "Cód. Cliente", "Cliente", "Endereço", "Nº Nota", "Emissão", 
        "Vencto", "Recebto", "Entregador", "Forma Pagto", "Valor Bruto", 
        "Desc/Acrés", "Valor Líquido", "Atendente", "Carteira", "Canal Cliente", 
        "Canal Venda", "Cidade", "Observação", "Hora Pedido", "Bairro", 
        "Latitude", "Longitude", "Endereço Conclusão do Pedido", "Empresa"
    ]

    columns_not_found = [column for column in EXPECTED_COLUMNS if column not in data_frame.columns]
    if columns_not_found:
        raise TypeError(f'Colunas Essenciais Ausentes: {columns_not_found}') 
    
def filter_new_data(data_frame: pd.DataFrame) -> pd.DataFrame:
    with engine.connect() as connection:
        if inspect(engine).has_table(TABLE_NAME):
            query = text(f"SELECT DISTINCT n_nota FROM {TABLE_NAME}")
            current_ids = pd.read_sql_query(query, connection)['n_nota'].values
        else:
            current_ids = []

    new_data = data_frame[~data_frame['Nº Nota'].isin(current_ids)].copy()
    
    new_data.columns = [slugify(col, separator='_') for col in new_data.columns]
    return new_data

def main():
    st.set_page_config(layout="wide")
    st.title("Importação base Grupo Nato")

    col1, col2 = st.columns([6, 2], gap="large")
    
    with col1:
        file = st.file_uploader("Planilha", 'xlsx', False)

        if file:
            df = pd.read_excel(file)

            try:
                validate_data_frame(df)
                new_data = filter_new_data(df)

                if new_data.empty:
                        st.info("Todos os registros desta planilha já existem no banco de dados.")
                else:
                    send_button = st.button("Enviar Novos dados")

                    if send_button:
                        with col2:
                            with st.spinner("Enviando novos dados"):
                                n = new_data.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
                            st.success(f"{n or 0} novos registros adicionados com sucesso!")
            except Exception as e:
                st.error(f"Erro ao processar: {e}")

if __name__ == '__main__':
    main()