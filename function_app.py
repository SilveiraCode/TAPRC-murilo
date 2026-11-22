import io
import logging
import os

import azure.functions as func
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Ordem obrigatória para respeitar as chaves estrangeiras (FKs) no schema dbo
TABELAS_ORDEM = [
    "categoria",
    "cliente_organizacao",
    "csat",
    "fila",
    "sla",
    "analista",
    "solicitante",
    "chamado",
    "chamado_sla",
    "chamado_status_historico",
    "csat_avaliacao",
]


def conectar_sql() -> pyodbc.Connection:
    servidor = os.environ["SQL_HOST"]
    banco_dados = os.environ["SQL_DATABASE"]
    usuario = os.environ["SQL_USER"]
    senha = os.environ["SQL_PASSWORD"]

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={servidor},1433;"
        f"DATABASE={banco_dados};"
        f"UID={usuario};"
        f"PWD={senha};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def _limpar_valor(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and val.is_integer():
        return int(val)
    if hasattr(val, "to_pydatetime"):
        return val.to_pydatetime()
    if hasattr(val, "item"):
        return _limpar_valor(val.item())
    return val


def ler_csv_datalake(
    blob_service: BlobServiceClient, container: str, blob_name: str
) -> pd.DataFrame:
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    if not blob_client.exists():
        logging.warning(f"Arquivo '{container}/{blob_name}' não encontrado no Data Lake.")
        return pd.DataFrame()

    csv_bytes = blob_client.download_blob().readall()
    texto = csv_bytes.decode("utf-8-sig")
    linhas = texto.splitlines()
    if not linhas:
        return pd.DataFrame()

    sep = ";" if linhas[0].count(";") > linhas[0].count(",") else ","
    df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=object)
    df = df.where(pd.notnull(df), None)
    return df


def popular_tabela(cursor: pyodbc.Cursor, tabela: str, df: pd.DataFrame) -> int:
    if df.empty:
        logging.info(f"[{tabela}] Sem registros para inserir.")
        return 0

    cursor.execute(
        """
        SELECT c.name, c.is_identity
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo'
          AND t.name = ?
        ORDER BY c.column_id
        """,
        tabela,
    )
    colunas_info = cursor.fetchall()
    colunas_destino = [row[0] for row in colunas_info]
    colunas_identity = {row[0] for row in colunas_info if row[1] == 1}

    colunas_validas = [col for col in colunas_destino if col in df.columns]
    if not colunas_validas:
        logging.warning(f"[{tabela}] Nenhuma coluna em comum entre o CSV e dbo.{tabela}.")
        return 0

    tem_identity_no_csv = any(col in colunas_identity for col in colunas_validas)
    if tem_identity_no_csv:
        cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] ON")

    try:
        colunas_sql = ", ".join(f"[{col}]" for col in colunas_validas)
        placeholders = ", ".join("?" for _ in colunas_validas)
        query = f"INSERT INTO dbo.[{tabela}] ({colunas_sql}) VALUES ({placeholders})"

        registros = [
            tuple(_limpar_valor(v) for v in row)
            for row in df[colunas_validas].itertuples(index=False, name=None)
        ]
        cursor.fast_executemany = False
        cursor.executemany(query, registros)
        return len(registros)
    finally:
        if tem_identity_no_csv:
            try:
                cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] OFF")
            except Exception:
                pass


def popular_tabela(cursor: pyodbc.Cursor, tabela: str, df: pd.DataFrame) -> int:
    if df.empty:
        logging.info(f"[{tabela}] Sem registros para inserir.")
        return 0

    cursor.execute(
        """
        SELECT c.name, c.is_identity
        FROM sys.columns c
        INNER JOIN sys.tables t ON c.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo'
          AND t.name = ?
        ORDER BY c.column_id
        """,
        tabela,
    )
    colunas_info = cursor.fetchall()
    colunas_destino = [row[0] for row in colunas_info]
    colunas_identity = {row[0] for row in colunas_info if row[1] == 1}

    colunas_validas = [col for col in colunas_destino if col in df.columns]
    if not colunas_validas:
        logging.warning(f"[{tabela}] Nenhuma coluna em comum entre o CSV e dbo.{tabela}.")
        return 0

    tem_identity_no_csv = any(col in colunas_identity for col in colunas_validas)
    if tem_identity_no_csv:
        cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] ON")

    try:
        colunas_sql = ", ".join(f"[{col}]" for col in colunas_validas)
        placeholders = ", ".join("?" for _ in colunas_validas)
        query = f"INSERT INTO dbo.[{tabela}] ({colunas_sql}) VALUES ({placeholders})"

        registros = [tuple(x) for x in df[colunas_validas].itertuples(index=False, name=None)]
        cursor.fast_executemany = True
        cursor.executemany(query, registros)
        return len(registros)
    finally:
        if tem_identity_no_csv:
            cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] OFF")


@app.timer_trigger(
    schedule="0 0 6 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True,
)
def carregar_tabelas_dbo(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("Execução do timer atrasada (past_due).")

    logging.info("Iniciando carga diária dos CSVs para o Azure SQL (dbo)...")

    conn_str_dl = os.environ["DATALAKE_CONNECTION_STRING"]
    container = os.environ["DATALAKE_CONTAINER"]
    prefix = os.getenv("CSV_PREFIX", "").strip("/")

    blob_service = BlobServiceClient.from_connection_string(conn_str_dl)
    conn_sql = conectar_sql()

    try:
        cursor = conn_sql.cursor()
        for tabela in TABELAS_ORDEM:
            blob_name = f"{prefix}/{tabela}.csv" if prefix else f"{tabela}.csv"
            logging.info(f"Lendo '{container}/{blob_name}'...")

            df = ler_csv_datalake(blob_service, container, blob_name)
            inseridos = popular_tabela(cursor, tabela, df)
            conn_sql.commit()
            logging.info(f"[{tabela}] {inseridos} registros inseridos.")

        logging.info("Carga diária finalizada com sucesso!")
    except Exception as erro:
        conn_sql.rollback()
        logging.error(f"Falha na carga diária: {erro}")
        raise
    finally:
        conn_sql.close()