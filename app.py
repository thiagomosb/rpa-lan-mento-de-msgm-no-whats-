import streamlit as st
import pandas as pd
from datetime import datetime
from envio_mensagens import gerar_mensagens_por_grupo, enviar_mensagem_whatsapp, inicializar_driver
import os



csv_path = "contatos.csv"
DEFAULT_CONTATO = {
    "nome": "Thiago",
    "funcao": "OUTROS",
    "unidade": "MORRINHOS",
    "numero": "+5574988214340",
    "grupo": "DOLP ENGENHARIA - GO"
}

# Carrega contatos do CSV
if "contatos" not in st.session_state:
    contatos = []
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            contatos = pd.read_csv(csv_path).to_dict(orient="records")
        except Exception:
            contatos = []
    if not contatos:
        contatos = [DEFAULT_CONTATO]
        pd.DataFrame(contatos).to_csv(csv_path, index=False)
    st.session_state.contatos = contatos

if "mostrar_lista" not in st.session_state:
    st.session_state.mostrar_lista = False


st.markdown("""
    <div style="text-align: center; font-size: 18px; margin-bottom: 25px;">
        Organize os contatos para envio automatizado via WhatsApp.<br>
        <strong>Bot de Envio Dos Resumos das Inspeções do Mês 💬</strong>
    </div>
""", unsafe_allow_html=True)


# === LAYOUT LADO A LADO === #
col_esq, col_dir = st.columns(2)

# === COLUNA ESQUERDA: ADICIONAR === #
with col_esq:
    st.markdown("#### ➕ Adicionar Contato")

    with st.form("formulario_contato"):
        nome = st.text_input("Nome completo")
        funcao = st.selectbox("Função", [
            "SUPERVISOR", "LIDER DE CAMPO", "TÉCNICO DE SEGURANÇA DO TRABALHO",
            "COORDENADOR OPERACIONAL", "COORDENADOR DE SEGURANÇA", "ENGENHEIRO DE SEGURANÇA","ANALISTA OPERACIONAL", "GESTOR DE CONTRATO","OUTROS"
        ])
        unidade = st.selectbox("Unidade", [
    "MORRINHOS", "RIO VERDE", "ITUMBIARA", "CALDAS NOVAS",
    "CATALÃO", "PIRES DO RIO", "APARECIDA DE GOIANIA",
    "PALMAS", "CUIÁBA", "VARZIÁ GRANDE"
])


        c1, c2 = st.columns([1, 4])
        with c1:
            st.text_input("Código do País", value="+55", disabled=True, label_visibility="collapsed")
        with c2:
            numero_input = st.text_input("Número com DDD (ex: 62984181348)", max_chars=11)

        grupo = st.radio("Grupo", [
            "DOLP ENGENHARIA - GO",
            "DOLP ENGENHARIA - MT",
            "DOLP ENGENHARIA - TO"
        ], horizontal=True)

        submitted = st.form_submit_button("Adicionar contato")

    if submitted and nome and numero_input and funcao:
        numero = f"+55{numero_input.strip()}"
        novo = {
            "nome": nome,
            "funcao": funcao,
            "unidade": unidade,
            "numero": numero,
            "grupo": grupo
        }
        st.session_state.contatos.append(novo)
        pd.DataFrame(st.session_state.contatos).to_csv(csv_path, index=False)
        st.success(f"Contato {nome} adicionado com sucesso!")

# === COLUNA DIREITA: REMOVER === #

    st.subheader("🧹 Remover Contatos ")
    if st.button("👁️ Ver lista de contatos"):
        st.session_state.mostrar_lista = True

    if st.session_state.mostrar_lista and st.session_state.contatos:
        df_contatos = pd.DataFrame(st.session_state.contatos)
        opcoes = df_contatos["nome"] + " - " + df_contatos["numero"].astype(str)
        selecionados = st.multiselect("Selecione para remover:", options=opcoes.tolist())

        if st.button("❌ Confirmar Remoção"):
            st.session_state.contatos = [
                c for c in st.session_state.contatos
                if f"{c['nome']} - {c['numero']}" not in selecionados
            ]
            pd.DataFrame(st.session_state.contatos).to_csv(csv_path, index=False)
            st.success("Contatos removidos com sucesso.")
            st.session_state.mostrar_lista = False



# === RODAPÉ: ENVIO DE MENSAGENS === #
st.markdown("---")
if st.session_state.contatos:
    if st.button("📌 Enviar Mensagens via WhatsApp"):
        mensagens = gerar_mensagens_por_grupo()
        driver = inicializar_driver()

        for contato in st.session_state.contatos:
            grupo = contato["grupo"]
            numero = contato["numero"]
            chave_grupo = grupo.split("-")[-1].strip()
            lista_mensagens = mensagens.get(chave_grupo, ["⚠️ Nenhuma mensagem disponível."])
            st.write(f"Enviando {len(lista_mensagens)} mensagens para {contato['nome']} ({numero})...")
            for mensagem in lista_mensagens:
                enviar_mensagem_whatsapp(numero, mensagem, driver)

        driver.quit()
        st.success("✅ Mensagens enviadas com sucesso.")

#----------------------------------------- Agendador automatico -------------------------------------#
with col_dir:
# === Agendador Automático ===
    import json

    st.markdown("#### 🔁 Agendamento Automático")

    CONFIG_PATH = "config_agendador.json"
    CONFIG_PADRAO = {
        "habilitado": False,
        "intervalo_dias": 1,
        "hora_envio": "08:00"
    }

    # Função para carregar
    def carregar_config():
        try:
            if os.path.exists(CONFIG_PATH) and os.path.getsize(CONFIG_PATH) > 0:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
        except json.JSONDecodeError:
            st.error("❌ Arquivo de configuração inválido. Usando padrão.")
        return CONFIG_PADRAO

    # Função para salvar
    def salvar_config(config_dict):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(config_dict, f, indent=4)
            return True
        except Exception as e:
            st.error(f"❌ Erro ao salvar configuração: {e}")
            return False

    # Interface
    config = carregar_config()

    with st.form("form_agendador"):
        habilitado = st.checkbox("✅ Ativar envio automático", value=config.get("habilitado", False))
        intervalo_dias = st.number_input("Enviar a cada quantos dias?", min_value=1, max_value=30, value=config.get("intervalo_dias", 1))
        hora_envio = st.time_input("Horário do envio", value=datetime.strptime(config.get("hora_envio", "08:00"), "%H:%M").time())
        
        salvar = st.form_submit_button("💾 Salvar Configuração")

        if salvar:
            nova_config = {
                "habilitado": habilitado,
                "intervalo_dias": intervalo_dias,
                "hora_envio": hora_envio.strftime("%H:%M")
            }
            sucesso = salvar_config(nova_config)
            if sucesso:
                st.success("✅ Configuração salva com sucesso!")
                st.write("🔁", nova_config)


    

