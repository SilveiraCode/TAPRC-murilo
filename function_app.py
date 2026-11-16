import logging
import os
import mysql.connector
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
             use_monitor=False) 
def timer_trigger_oi(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('O temporizador está atrasado!')

    logging.info('Executando a Azure Function para capturar dados do banco itsm...')

    # Lendo as credenciais de forma segura via variáveis de ambiente
    db_host = os.environ.get("DB_HOST")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_name = os.environ.get("DB_NAME")

    try:
        # Abrindo conexão com o banco de dados de origem itsm
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            
            # Capturando os dados da tabela escolhida (ex: analista)[cite: 5]
            cursor.execute("SELECT * FROM analista;")
            resultados = cursor.fetchall()
            
            logging.info(f"Sucesso! Foram encontrados {len(resultados)} registros na tabela 'analista'.")
            for linha in resultados:
                logging.info(f"Registro: {linha}")

    except Exception as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("Conexão com o banco de dados fechada.")

@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    
    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )