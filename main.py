import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from decouple import config

DB_URL = config("DB_URL")

engine = create_engine(DB_URL)

def validate_data_frame(data_frame: pd.DataFrame, expected_columns: list[str]) -> None:
    columns_not_found = [column for column in expected_columns if column not in data_frame.columns]
    if columns_not_found:
        raise TypeError(f'Colunas Essenciais Ausentes: {columns_not_found}') 
    
def filter_new_data(data_frame: pd.DataFrame, table_name: str, unique_column: str) -> pd.DataFrame:
    with engine.connect() as connection:
        if inspect(engine).has_table(table_name):
            query = text(f'SELECT DISTINCT "{unique_column}" FROM {table_name}')
            current_ids = pd.read_sql_query(query, connection)[unique_column].values
        else:
            current_ids = []

    new_data = data_frame[~data_frame[unique_column].isin(current_ids)].copy()
    
    return new_data

def processar_pedidos(file, table_name, unique_column):
    def convert_to_float(x: str) -> float:
        if pd.isna(x) or x == "--" or str(x).strip() == "":
            return 0.0
        
        if isinstance(x, (int, float)):
            return float(x)
        
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".").strip()
            try:
                return float(x)
            except ValueError:
                return 0.0
                
        return 0.0
    st.write('### → Pedidos:')
    progress_bar = st.progress(0, 'Lendo arquivo...')

    try:
        df = pd.read_excel(
            file, 
            parse_dates=["Emissão", "Vencto", "Recebto"], 
            date_format="%d-%m-%y",
            na_values=["--"],
            converters={column: convert_to_float for column in ["Valor Bruto","Desc/Acrés", "Valor Líquido"]}
        )
    except Exception as e:
        progress_bar.empty()
        st.error(f'Erro ao abrir arquivo. Erro: {e}')
        return
    
    progress_bar.progress(1/4, 'Validando planilha...')

    try:
        validate_data_frame(df, [
            "Cód. Cliente", "Cliente", "Endereço", "Nº Nota", "Emissão", 
            "Vencto", "Recebto", "Entregador", "Forma Pagto", "Valor Bruto", 
            "Desc/Acrés", "Valor Líquido", "Atendente", "Carteira", "Canal Cliente", 
            "Canal Venda", "Cidade", "Observação", "Hora Pedido", "Bairro", 
            "Latitude", "Longitude", "Endereço Conclusão do Pedido", "Empresa"
        ])
    except Exception as e:
        progress_bar.empty()
        st.error(f'Planilha inválida. Erro: {e}')
        return
    
    progress_bar.progress(2/4, 'Filtrando registros...')

    try:
        new_data = filter_new_data(df, table_name, unique_column)
    except Exception as e:
        progress_bar.empty()
        st.error(f'Erro ao se conectar ao banco de dados. Erro: {e}')
        return

    if new_data.empty:
        st.info("Todos os registros desta planilha já existem no banco de dados.")
        progress_bar.empty()
        return

    progress_bar.progress(3/4, "Enviando dados")

    try:
        n = new_data.to_sql(table_name, engine, if_exists='append', index=False)
        progress_bar.progress(4/4, 'Sucesso!')
        st.success(f"{n or 0} novos registros adicionados com sucesso!")
        
    except Exception as e:
        progress_bar.empty()
        st.warning(f'Houve um erro ao enviar dados. Erro: {e}')
        return
    
    
    progress_bar.empty()

def processar_itens(file, table_name, unique_column):
    st.write('### → Itens dos Pedidos:')
    progress_bar = st.progress(0, 'Lendo arquivo...')

    try:
        df = pd.read_excel(file)
    except Exception as e:
        progress_bar.empty()
        st.error(f'Erro ao abrir arquivo. Erro: {e}')
        return
    
    progress_bar.progress(1/4, 'Validando planilha...')

    try:
        validate_data_frame(df, ['Item', 'Código Cliente', 'Cliente', 'Produto', 'Data', 'Nota',
       'Forma Pagto', 'Qtde', 'Preço', 'Total'])
    except Exception as e:
        progress_bar.empty()
        st.error(f'Planilha inválida. Erro: {e}')
        return
    
    progress_bar.progress(2/4, 'Filtrando registros...')

    try:
        new_data = filter_new_data(df, table_name, unique_column)
    except Exception as e:
        progress_bar.empty()
        st.error(f'Erro ao se conectar ao banco de dados. Erro: {e}')
        return

    if new_data.empty:
        st.info("Todos os registros desta planilha já existem no banco de dados.")
        progress_bar.empty()
        return

    progress_bar.progress(3/4, "Enviando dados")

    try:
        n = new_data.to_sql(table_name, engine, if_exists='append', index=False)
        progress_bar.progress(4/4, 'Sucesso!')
        st.success(f"{n or 0} novos registros adicionados com sucesso!")
        
    except Exception as e:
        progress_bar.empty()
        st.warning(f'Houve um erro ao enviar dados. Erro: {e}')
        return
    
    
    progress_bar.empty()

def processar_clientes(file, table_name):
    st.write('### → Clientes:')
    progress_bar = st.progress(0, 'Lendo arquivo...')

    try:
        df = pd.read_excel(file, header=11, skipfooter=5)
        df.dropna(axis=1, how='all', inplace=True)
        df.rename(columns={'Unnamed: 12': ' Cidade/UF'}, inplace=True)
    except Exception as e:
        progress_bar.empty()
        st.error(f'Erro ao abrir arquivo. Erro: {e}')
        return
    
    progress_bar.progress(1/3, 'Validando planilha...')

    try:
        validate_data_frame(df, [' Item', ' Cliente', ' Carteira', ' Canal', ' Tipo', ' Status',
       ' Endereço', ' Bairro', ' Cidade/UF'])
    except Exception as e:
        progress_bar.empty()
        st.error(f'Planilha inválida. Erro: {e}')
        return

    progress_bar.progress(2/3, "Enviando dados")

    try:
        n = df.to_sql(table_name, engine, if_exists='replace', index=False)
        progress_bar.progress(3/3, 'Sucesso!')
        st.success(f"{n or 0} novos registros adicionados com sucesso!")
        
    except Exception as e:
        progress_bar.empty()
        st.warning(f'Houve um erro ao enviar dados. Erro: {e}')
        return
    
    progress_bar.empty()

def main():
    st.set_page_config(layout="wide")
    st.title("Importação base Grupo Nato")

    col1, col2 = st.columns(2, gap="large")
    
    with col1:
        st.write("## Planilhas")
        pedidos = st.file_uploader("Planilha Pedidos", ['xlsx', 'xls'], False)
        itens = st.file_uploader("Planilha Itens dos Pedidos", ['xlsx', 'xls'], False)
        clientes = st.file_uploader("Planilha Clientes", ['xlsx', 'xls'], False)

        send_button = st.button("Enviar")

    with col2:
        st.write("## Logs")
        if send_button and (pedidos or itens or clientes):
            if pedidos:
                processar_pedidos(pedidos, 'pedidos_gruponato', 'Nº Nota')
            if itens:
                processar_itens(itens, 'itenspedido_gruponato', 'Nota')
            if clientes:
                processar_clientes(clientes, 'clientes_gruponato')

if __name__ == '__main__':
    main()