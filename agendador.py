import time
import json
import os
from datetime import datetime, timedelta
from envio_mensagens import gerar_mensagens_por_grupo, enviar_mensagem_whatsapp, inicializar_driver

CONFIG_PATH = "config_agendador.json"
CONTATOS_PATH = "contatos.csv"

def carregar_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "habilitado": False,
        "intervalo_dias": 1,
        "hora_envio": "16:55"
    }

def enviar_mensagens():
    print(f"[{datetime.now()}] Iniciando envio de mensagens...")
    mensagens = gerar_mensagens_por_grupo()

    if not os.path.exists(CONTATOS_PATH):
        print("❌ Arquivo de contatos não encontrado.")
        return

    import pandas as pd
    df_contatos = pd.read_csv(CONTATOS_PATH)
    driver = inicializar_driver()

    for _, contato in df_contatos.iterrows():
        grupo = contato["grupo"]
        numero = contato["numero"]
        nome = contato["nome"]
        chave_grupo = grupo.split("-")[-1].strip()
        mensagem = mensagens.get(chave_grupo, "⚠️ Nenhuma mensagem disponível.")
        print(f"Enviando para {nome} ({numero})...")
        enviar_mensagem_whatsapp(numero, mensagem, driver)

    driver.quit()
    print("✅ Envio concluído.")

def executar_agendador():
    config = carregar_config()
    if not config.get("habilitado"):
        print("⏹️ Agendamento desativado.")
        return

    hora_envio = datetime.strptime(config["hora_envio"], "%H:%M").time()
    intervalo_dias = config["intervalo_dias"]
    ultima_execucao_path = "ultima_execucao.txt"

    # Carrega última execução
    if os.path.exists(ultima_execucao_path):
        with open(ultima_execucao_path, "r") as f:
            ultima_execucao = datetime.strptime(f.read(), "%Y-%m-%d")
    else:
        ultima_execucao = datetime.min

    hoje = datetime.now().date()
    agora = datetime.now().time()

    if (hoje - ultima_execucao.date()).days >= intervalo_dias and agora >= hora_envio:
        enviar_mensagens()
        with open(ultima_execucao_path, "w") as f:
            f.write(str(hoje))
    else:
        print("⏳ Ainda não é hora de enviar.")

# 🔁 LOOP CONTÍNUO PARA TESTE
if __name__ == "__main__":
    while True:
        executar_agendador()
        time.sleep(60)  # Verifica a cada 60 segundos
