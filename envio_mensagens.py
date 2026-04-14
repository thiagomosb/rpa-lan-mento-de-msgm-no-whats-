import os
import mysql.connector
import logging
from datetime import datetime
from pathlib import Path
from time import sleep
import pyperclip
import unicodedata
from urllib.parse import quote_plus
from dotenv import load_dotenv
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv(Path(__file__).parent / '.env')

log_path = Path(__file__).parent / 'envio_alertas.log'
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s %(message)s')

empresas_por_grupo = {
    'GO': ['DOLP ENGENHARIA - GO'],
    'TO': ['DOLP ENGENHARIA - TO'],
    'MT': ['DOLP ENGENHARIA - MT']
}

METAS_POR_EMPRESA = {
    'DOLP ENGENHARIA - MT': {'SESMT': 24, 'SUPERVISORES': 15}
}
META_SESMT_DEFAULT = 15
META_SUPERVISORES_DEFAULT = 8

def normalizar(texto):
    if not texto:
        return ""
    texto = texto.strip().upper()
    texto = unicodedata.normalize("NFD", texto)
    texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    return ' '.join(texto.split()).replace("-", " ")

def conectar_bd():
    db_type = os.getenv('DB_TYPE', 'mysql').strip().lower()
    db_host = os.getenv('DB_HOST', '').strip()
    db_port = os.getenv('DB_PORT', '').strip()
    db_name = os.getenv('DB_NAME', '').strip()
    db_user = os.getenv('DB_USER', '').strip()
    db_password = os.getenv('DB_PASSWORD', '').strip()
    db_service_name = os.getenv('DB_SERVICE_NAME', '').strip()

    if db_type in ('mysql', 'mariadb'):
        conn_args = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'database': db_name
        }
        if db_port:
            conn_args['port'] = int(db_port)
        return mysql.connector.connect(**conn_args)

    if db_type in ('oracle', 'oracledb'):
        import oracledb
        port = int(db_port) if db_port else 1521
        if db_service_name:
            dsn = oracledb.makedsn(db_host, port, service_name=db_service_name)
        else:
            dsn = oracledb.makedsn(db_host, port, sid=db_name)
        return oracledb.connect(dsn=dsn, user=db_user, password=db_password)

    raise ValueError(f"DB_TYPE desconhecido: {db_type}. Use mysql, mariadb, oracle ou oracledb.")

def consultar_comunica_recursos_pendentes():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ID,
               REGIONAL,
               TIPO_DE_EQUIPE,
               OCORRENCIA,
               AFETACAO,
               CONTROLADOR,
               DATA_SOLICITACAO,
               TIPO_DE_NOTA,
               INFORMACAO_CONTROLADOR,
               ROUND(
                   (CAST(SYSDATE AS DATE) - CAST(DATA_SOLICITACAO AS DATE)) * 1440,
                   2
               ) AS TEMPO_MINUTOS
        FROM COMUNICA_RECURSOS
        WHERE 1=1
          AND ((CAST(SYSDATE AS DATE) - CAST(DATA_SOLICITACAO AS DATE)) * 1440) > 10
          AND STATUS NOT IN ('Aceitar')
    """)
    raw_resultados = cursor.fetchall()
    resultados = []
    for row in raw_resultados:
        resultados.append(tuple(
            col.read() if hasattr(col, 'read') else col
            for col in row
        ))
    cursor.close()
    conn.close()
    return resultados

def agrupar_pendentes_por_regional(pendentes):
    pendentes_por_regional = {'GO': [], 'TO': [], 'MT': []}
    for row in pendentes:
        regional = str(row[1] or '').upper()
        if 'GO' in regional:
            pendentes_por_regional['GO'].append(row)
        elif 'TO' in regional:
            pendentes_por_regional['TO'].append(row)
        elif 'MT' in regional:
            pendentes_por_regional['MT'].append(row)
        else:
            for chave in pendentes_por_regional:
                pendentes_por_regional[chave].append(row)
    return pendentes_por_regional

def formatar_mensagem_comunica_recursos(grupo, pendentes):
    if not pendentes:
        return [f"{grupo}\n\n✅ Nenhuma comunicação pendente no COMUNICA_RECURSOS (> 10 min)."]

    mensagens = []
    for id_, regional, tipo_equipe, ocorrencia, afetacao, controlador, data_solicitacao, tipo_nota, informacao_controlador, tempo_minutos in pendentes:
        mensagem = f"{grupo}\n\n"
        mensagem += "🚨 Comunicação pendente no COMUNICA_RECURSOS (> 10 min)\n\n"
        mensagem += f"ID: {id_}\n"
        mensagem += f"Regional: {regional}\n"
        mensagem += f"Tipo de equipe: {tipo_equipe}\n"
        mensagem += f"Ocorrência: {ocorrencia}\n"
        mensagem += f"Afetação: {afetacao}\n"
        mensagem += f"Controlador: {controlador}\n"
        mensagem += f"Data solicitação: {data_solicitacao}\n"
        mensagem += f"Tipo de nota: {tipo_nota}\n"
        mensagem += f"Informação controlador: {informacao_controlador}\n"
        mensagem += f"Tempo (minutos): {tempo_minutos}\n\n"
        mensagem += "💬 Mensagem preparada pelo robô 🤖"
        mensagens.append(mensagem.strip())
    return mensagens

def consultar_inspecoes_detalhadas_por_empresa():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.nom_fant, p.nome, p.funcao_geral, COUNT(DISTINCT b.idtb_turnos) AS total_inspecoes
        FROM view_power_bi_blitz_contatos b
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        JOIN view_power_bi_pessoas p ON b.nome_inspetor = p.nome
        WHERE YEAR(t.dt_inicio) = YEAR(CURRENT_DATE)
          AND MONTH(t.dt_inicio) = MONTH(CURRENT_DATE)
          AND p.funcao_geral IN (
              'TÉCNICO DE SEGURANÇA DO TRABALHO',
              'TECNICO DE SEGURANÇA DO TRABALHO II',
              'COORDENADOR DE SEGURANÇA',
              'SUPERVISOR',
              'LIDER DE CAMPO',
              'SUPERVISOR '
          )
        GROUP BY t.nom_fant, p.nome, p.funcao_geral
        ORDER BY t.nom_fant, p.funcao_geral, total_inspecoes DESC
    """)
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()

    empresas = {}
    for empresa, nome, funcao, total in resultados:
        if empresa not in empresas:
            empresas[empresa] = {'sesmt': [], 'supervisores': []}
        if funcao.strip().upper() in ["TÉCNICO DE SEGURANÇA DO TRABALHO", "TECNICO DE SEGURANÇA DO TRABALHO II", "COORDENADOR DE SEGURANÇA"]:
            empresas[empresa]['sesmt'].append((nome, total))
        elif funcao.strip().upper() in ["SUPERVISOR", "LIDER DE CAMPO"]:
            empresas[empresa]['supervisores'].append((nome, total))

    return empresas
def calcular_taxa_contato_detalhada_por_unidade(empresa):
    conn = conectar_bd()
    cursor = conn.cursor()

    # Todas as equipes por unidade
    cursor.execute("""
        SELECT unidade, num_operacional
        FROM view_power_bi_turnos
        WHERE nom_fant = %s
          AND YEAR(dt_inicio) = YEAR(CURRENT_DATE)
          AND MONTH(dt_inicio) = MONTH(CURRENT_DATE)
    """, (empresa,))
    todos = cursor.fetchall()

    # Equipes inspecionadas por unidade
    cursor.execute("""
        SELECT t.unidade, b.num_operacional
        FROM view_power_bi_blitz_contatos b
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        WHERE t.nom_fant = %s
          AND YEAR(b.data_turno) = YEAR(CURRENT_DATE)
          AND MONTH(b.data_turno) = MONTH(CURRENT_DATE)
    """, (empresa,))
    inspecionados = cursor.fetchall()

    cursor.close()
    conn.close()

    # Organiza os dados por unidade
    unidades = {}
    for unidade, equipe in todos:
        if unidade not in unidades:
            unidades[unidade] = {'todos': set(), 'inspecionados': set()}
        unidades[unidade]['todos'].add(equipe)
    for unidade, equipe in inspecionados:
        if unidade in unidades:
            unidades[unidade]['inspecionados'].add(equipe)

    resultado = {}
    for unidade, dados in unidades.items():
        total = len(dados['todos'])
        inspecionadas = dados['inspecionados']
        nao_inspecionadas = dados['todos'] - inspecionadas
        taxa = round((len(inspecionadas) / total * 100), 2) if total else 0
        resultado[unidade] = {
            'taxa': taxa,
            'inspecionadas': sorted(inspecionadas),
            'nao_inspecionadas': sorted(nao_inspecionadas)
        }
    return resultado


def calcular_taxa_contato(empresa):
    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("""
        FROM view_power_bi_turnos
        WHERE nom_fant = %s
          AND YEAR(dt_inicio) = YEAR(CURRENT_DATE)
          AND MONTH(dt_inicio) = MONTH(CURRENT_DATE)
    """, (empresa,))
    todas_equipes = set(row[0] for row in cursor.fetchall())

    cursor.execute("""
        SELECT DISTINCT b.num_operacional
        FROM view_power_bi_blitz_contatos b
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        WHERE t.nom_fant = %s
          AND YEAR(b.data_turno) = YEAR(CURRENT_DATE)
          AND MONTH(b.data_turno) = MONTH(CURRENT_DATE)
    """, (empresa,))
    inspecionadas = set(row[0] for row in cursor.fetchall())

    cursor.close()
    conn.close()

    nao_inspecionadas = todas_equipes - inspecionadas
    taxa_contato = (len(inspecionadas) / len(todas_equipes) * 100) if todas_equipes else 0

    return round(taxa_contato, 2), len(inspecionadas), len(nao_inspecionadas), list(inspecionadas), list(nao_inspecionadas)

def consultar_indicadores_gerais_por_empresa(empresa):
    conn = conectar_bd()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(DISTINCT b.idtb_turnos)
        FROM view_power_bi_blitz_contatos b
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        WHERE YEAR(b.data_turno) = YEAR(CURRENT_DATE)
          AND MONTH(b.data_turno) = MONTH(CURRENT_DATE)
          AND t.nom_fant = %s
    """, (empresa,))
    total_inspecoes = cursor.fetchone()[0]

    taxa_contato, total_inspecionadas, total_nao_inspecionadas, lista_inspecionadas, lista_nao_inspecionadas = calcular_taxa_contato(empresa)

    cursor.execute("""
        SELECT COUNT(*)
        FROM view_power_bi_blitz_respostas r
        JOIN view_power_bi_blitz_contatos b ON r.Key = b.Key
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        WHERE r.resposta_int = 2
          AND YEAR(dt_inicio) = YEAR(CURRENT_DATE)
          AND MONTH(dt_inicio) = MONTH(CURRENT_DATE)
          AND t.nom_fant = %s
    """, (empresa,))
    total_ncs = cursor.fetchone()[0]

    cursor.execute("""
        SELECT r.subgrupo, COUNT(*) AS total
        FROM view_power_bi_blitz_respostas r
        JOIN view_power_bi_blitz_contatos b ON r.Key = b.Key
        JOIN view_power_bi_turnos t ON b.idtb_turnos = t.idtb_turnos
        WHERE r.resposta_int = 2
          AND YEAR(b.data_turno) = YEAR(CURRENT_DATE)
          AND MONTH(b.data_turno) = MONTH(CURRENT_DATE)
          AND t.nom_fant = %s
        GROUP BY r.subgrupo
        ORDER BY total DESC
        LIMIT 3
    """, (empresa,))
    top_subgrupos = cursor.fetchall()

    cursor.close()
    conn.close()

    return total_inspecoes, taxa_contato, total_ncs, top_subgrupos, total_inspecionadas, total_nao_inspecionadas, lista_inspecionadas, lista_nao_inspecionadas

def gerar_mensagens_por_grupo():
    pendentes_comunica = consultar_comunica_recursos_pendentes()
    pendentes_por_regional = agrupar_pendentes_por_regional(pendentes_comunica)

    mensagens_por_grupo = {}
    for grupo in empresas_por_grupo:
        mensagens_por_grupo[grupo] = formatar_mensagem_comunica_recursos(grupo, pendentes_por_regional.get(grupo, []))
    return mensagens_por_grupo

def enviar_mensagem_whatsapp(numero, mensagem, driver):
    try:
        mensagem_url = quote_plus(mensagem)
        driver.get(f"https://web.whatsapp.com/send?phone={numero}&text={mensagem_url}&app_absent=0")

        # Espera carregar a lateral de contatos/nome (apenas para garantir que o app está pronto)
        WebDriverWait(driver, 90).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='side']/div[1]/div/div/div/div/div/div"))
        )

        # Espera o campo de mensagem aparecer
        campo = WebDriverWait(driver, 90).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='main']/footer/div[1]/div/span/div/div/div/div[3]/div[1]/p"))
        )
        campo.click()
        sleep(1)

        pyperclip.copy(mensagem)
        campo.send_keys(Keys.CONTROL, 'v')
        sleep(1)

        botao_enviar = WebDriverWait(driver, 90).until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='main']/footer/div[1]/div/span/div/div/div/div[4]/div/span/button/div/div/div[1]/span"))
        )
        botao_enviar.click()
        sleep(2)
        logging.info(f"✅ Mensagem enviada para {numero}")

    except Exception as e:
        logging.error(f"❌ Erro ao enviar mensagem para {numero}: {e}")



def inicializar_driver():
    options = Options()
    whatsapp_profile_dir = os.getenv('WHATSAPP_PROFILE_DIR', 'C:/Temp/.selenium_whatsapp_profile')
    chrome_binary_path = os.getenv('CHROME_BINARY_PATH', r'C:/Program Files/Google/Chrome/Application/chrome.exe')
    options.add_argument(f"--user-data-dir={whatsapp_profile_dir}")
    options.add_argument("--start-maximized")
    options.binary_location = chrome_binary_path
    driver = webdriver.Chrome(options=options)
    driver.get("https://web.whatsapp.com")
    WebDriverWait(driver, 120).until(lambda d: d.find_element(By.XPATH, "//div[@contenteditable='true']"))
    return driver

