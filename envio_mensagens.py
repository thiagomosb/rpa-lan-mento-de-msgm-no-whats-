import mysql.connector
import logging
from datetime import datetime
from pathlib import Path
from time import sleep
import pyperclip
import unicodedata
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

def conectar_mariadb():
    return mysql.connector.connect(
        host='',
        database='',
        user='',
        password='')

def consultar_inspecoes_detalhadas_por_empresa():
    conn = conectar_mariadb()
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
    conn = conectar_mariadb()
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
    conn = conectar_mariadb()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT num_operacional
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
    conn = conectar_mariadb()
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
    agora = datetime.now()
    empresas = consultar_inspecoes_detalhadas_por_empresa()
    mensagens_por_empresa = {}

    meses_pt = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }

    for empresa, detalhes in empresas.items():
        meta_sesmt = METAS_POR_EMPRESA.get(empresa, {}).get('SESMT', META_SESMT_DEFAULT)
        meta_supervisores = METAS_POR_EMPRESA.get(empresa, {}).get('SUPERVISORES', META_SUPERVISORES_DEFAULT)

        total_inspecoes, taxa_contato, total_ncs, top_subgrupos, total_inspecionadas, total_nao_inspecionadas, lista_inspecionadas, lista_nao_inspecionadas = consultar_indicadores_gerais_por_empresa(empresa)

        mensagem = f"{empresa}\n\n"
        mensagem += f"📅 Indicadores Mensais ({meses_pt[agora.month]}/{agora.year})\n\n"
        mensagem += f"✅ Inspeções realizadas: {total_inspecoes}\n"
        mensagem += f"📈 Taxa de Contato: {taxa_contato}%\n"
        mensagem += f"🛻 Equipes inspecionadas: {total_inspecionadas}\n"
        mensagem += f"❌ Equipes não inspecionadas: {total_nao_inspecionadas}\n\n"
        mensagem += f"🚨 Não conformidades (NCs): {total_ncs}\n"
        mensagem += "🔝 Subgrupos com mais reprovações:\n"
        for idx, (subgrupo, qtd) in enumerate(top_subgrupos, start=1):
            mensagem += f"{idx}. {subgrupo} ({qtd})\n"

        mensagem += "\n👷‍♂️ SESMT:\n"
        for nome, total in detalhes['sesmt']:
            status = "✅" if total >= meta_sesmt else "❌"
            mensagem += f"- {nome}: {total}/{meta_sesmt} {status}\n"

        mensagem += "\n🧑‍💼 SUPERVISORES:\n"
        for nome, total in detalhes['supervisores']:
            status = "✅" if total >= meta_supervisores else "❌"
            mensagem += f"- {nome}: {total}/{meta_supervisores} {status}\n"

        mensagem += "\n\n✅ Equipes inspecionadas (por unidade):\n"
        detalhes_unidades = calcular_taxa_contato_detalhada_por_unidade(empresa)
        for unidade, dados in detalhes_unidades.items():
            if dados['inspecionadas']:
                equipes = ", ".join(dados['inspecionadas'])
                mensagem += f"- {unidade} ({dados['taxa']}%): {equipes}\n"

        mensagem += "\n\n❌ Equipes não inspecionadas (por unidade):\n"
        for unidade, dados in detalhes_unidades.items():
            if dados['nao_inspecionadas']:
                equipes = ", ".join(dados['nao_inspecionadas'])
                mensagem += f"- {unidade} : {equipes}\n"


        mensagem += "\n\n💬 Mensagem enviada via robô 🤖"
        mensagens_por_empresa[empresa] = mensagem.strip()

    mensagens_por_grupo = {}
    for grupo, empresas_grupo in empresas_por_grupo.items():
        mensagens = []
        for emp_grupo in empresas_grupo:
            for emp_nome in mensagens_por_empresa:
                if normalizar(emp_nome) == normalizar(emp_grupo):
                    mensagens.append(mensagens_por_empresa[emp_nome])
                    break
        mensagens_por_grupo[grupo] = "\n\n".join(mensagens) if mensagens else "⚠️ Nenhuma informação disponível no Banco de dados."
    return mensagens_por_grupo

def enviar_mensagem_whatsapp(numero, mensagem, driver):
    try:
        pyperclip.copy(mensagem)
        driver.get(f"https://web.whatsapp.com/send?phone={numero}&text&app_absent=0")

        # Espera o campo aparecer
        campo = WebDriverWait(driver, 90).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="main"]/footer/div[1]/div/span/div/div[2]/div[1]/div[2]/div[1]/p'))
        )

        WebDriverWait(driver, 90).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="main"]/footer/div[1]/div/span/div/div[2]/div[1]/div[2]/div[1]/p')))
        campo.click()
        sleep(1)

        # Cola a mensagem
        campo.send_keys(Keys.CONTROL, 'v')
        sleep(1)

        # Pressiona Enter para enviar
        campo.send_keys(Keys.ENTER)
        sleep(2)  # aguarda o envio
        logging.info(f"✅ Mensagem enviada para {numero}")

    except Exception as e:
        logging.error(f"❌ Erro ao enviar mensagem para {numero}: {e}")



def inicializar_driver():
    options = Options()
    options.add_argument("--user-data-dir=C:/Temp/.selenium_whatsapp_profile")
    options.add_argument("--start-maximized")
    options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    driver = webdriver.Chrome(options=options)
    driver.get("https://web.whatsapp.com")
    WebDriverWait(driver, 120).until(lambda d: d.find_element(By.XPATH, "//div[@contenteditable='true']"))
    return driver

