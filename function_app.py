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

# Chaves únicas de negócio para evitar duplicidade (erro 2627) em reexecuções diárias
CHAVES_UNICAS_TABELA = {
    "categoria": ["cd_categoria"],
    "cliente_organizacao": ["cd_cliente_organizacao"],
    "csat": ["Id"],
    "fila": ["cd_fila"],
    "sla": ["cd_sla"],
    "analista": ["cd_analista"],
    "solicitante": ["cd_solicitante"],
    "chamado": ["nr_chamado"],
}


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
    if isinstance(val, str):
        val_limpo = val.strip()
        if val_limpo == "" or val_limpo.lower() in {"nan", "none", "null", "nat"}:
            return None
        return val_limpo
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

    mapa_df_cols = {str(col).strip().lower(): col for col in df.columns}
    colunas_validas = [
        col for col in colunas_destino if col.strip().lower() in mapa_df_cols
    ]
    if not colunas_validas:
        logging.warning(f"[{tabela}] Nenhuma coluna em comum entre o CSV e dbo.{tabela}.")
        return 0

    df_ordenado = pd.DataFrame(
        {col: df[mapa_df_cols[col.strip().lower()]] for col in colunas_validas}
    )

    registros = [
        tuple(_limpar_valor(v) for v in row)
        for row in df_ordenado.itertuples(index=False, name=None)
    ]

    mapa_cols_validas = {col.strip().lower(): col for col in colunas_validas}
    chaves = [
        mapa_cols_validas[c.strip().lower()]
        for c in CHAVES_UNICAS_TABELA.get(tabela, [])
        if c.strip().lower() in mapa_cols_validas
    ]
    cursor.fast_executemany = False

    if not chaves:
        tem_identity_no_csv = any(col in colunas_identity for col in colunas_validas)
        if tem_identity_no_csv:
            cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] ON")
        try:
            colunas_sql = ", ".join(f"[{col}]" for col in colunas_validas)
            placeholders = ", ".join("?" for _ in colunas_validas)
            query = f"INSERT INTO dbo.[{tabela}] ({colunas_sql}) VALUES ({placeholders})"
            cursor.executemany(query, registros)
            return len(registros)
        finally:
            if tem_identity_no_csv:
                try:
                    cursor.execute(f"SET IDENTITY_INSERT dbo.[{tabela}] OFF")
                except Exception:
                    pass

    colunas_update = [
        c for c in colunas_validas if c not in chaves and c not in colunas_identity
    ]
    origem_cols_sql = ", ".join(f"? AS [{c}]" for c in colunas_validas)
    condicao_on = " AND ".join(f"alvo.[{c}] = origem.[{c}]" for c in chaves)

    set_update_sql = ", ".join(f"alvo.[{c}] = origem.[{c}]" for c in colunas_update)
    insert_cols = [c for c in colunas_validas if c not in colunas_identity]
    insert_cols_sql = ", ".join(f"[{c}]" for c in insert_cols)
    insert_vals_sql = ", ".join(f"origem.[{c}]" for c in insert_cols)

    merge_sql = f"""
    MERGE dbo.[{tabela}] AS alvo
    USING (SELECT {origem_cols_sql}) AS origem
        ON ({condicao_on})
    """
    if set_update_sql:
        merge_sql += f"\nWHEN MATCHED THEN UPDATE SET {set_update_sql}"
    merge_sql += (
        f"\nWHEN NOT MATCHED THEN INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql});"
    )

    cursor.executemany(merge_sql, registros)
    return len(registros)


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
            logging.info(f"[{tabela}] {inseridos} registros processados.")

        logging.info("Carga diária finalizada com sucesso!")
    except Exception as erro:
        try:
            conn_sql.rollback()
        except Exception:
            pass
        logging.error(f"Falha na carga diária: {erro}")
        raise
    finally:
        conn_sql.close()