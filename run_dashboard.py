"""
Dashboard interativo (Streamlit) combinando:
- `dashboard_calendar.json` (calendário de safra)
- `dashboard_data.json` (análises e links)

Recursos:
- Filtros por sentimento e país
- Métricas principais
-
- Análises detalhadas com links
- Distribuições por sentimento e país
"""

import html
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import boto3
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from supabase import create_client, Client


# -----------------------------------------------------------------------------
# Configuração da página
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Dashboard Safra (Piloto)",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Estilos globais - tema claro forçado
CSS = """
<style>
  :root {
    color-scheme: light !important;
  }
  
  html, body, .stApp {
    color-scheme: light !important;
    background: linear-gradient(180deg, #f4f7fb 0%, #eef2f7 100%) !important;
  }
  
  .stApp > header,
  .stApp > div,
  main,
  .block-container,
  [data-testid="stAppViewContainer"],
  [data-testid="stHeader"] {
    background-color: #f4f7fb !important;
    color: #0f172a !important;
  }
  
  .stApp { background: linear-gradient(180deg, #f4f7fb 0%, #eef2f7 100%) !important; }
  main .block-container { padding-top: 1.5rem; padding-bottom: 1.5rem; }
  .section-title { font-size: 24px; font-weight: 800; color: #0f172a; padding: 8px 0 4px 0; margin: 0 0 8px 0; letter-spacing: 0.2px; border-bottom: 2px solid #d7deeb; }
  .section-subtitle { font-size: 18px; font-weight: 700; color: #111827; padding: 4px 0 2px 0; margin: 0 0 6px 0; }
  .streamlit-expanderHeader { background: linear-gradient(135deg, #e8f5e9, #e3f2fd); color: #0f172a; border-radius: 8px; padding: 10px 12px; }
  .streamlit-expanderContent { background: #ffffff; border-radius: 0 0 8px 8px; padding: 12px; }
  
  div[data-testid="column"] .stButton>button[kind="secondary"],
  .stButton>button[kind="secondary"],
  button[kind="secondary"] {
    min-height: 50px !important;
    height: 50px !important;
    max-height: 50px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    line-height: 1.2 !important;
    padding: 10px 8px !important;
    box-sizing: border-box !important;
  }
  
  
  .stMultiSelect div[data-baseweb="select"],
  .stSelectbox div[data-baseweb="select"],
  .stTextInput div[data-baseweb="input"],
  .stNumberInput div[data-baseweb="input"],
  .stDateInput div[data-baseweb="input"],
  .stTextArea div[data-baseweb="input"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
    border: 1px solid #cbd5e1 !important;
  }
  
  .stMultiSelect input,
  .stSelectbox input,
  .stTextInput input,
  .stNumberInput input,
  .stDateInput input,
  .stTextArea textarea {
    background-color: #ffffff !important;
    color: #0f172a !important;
  }
  
  .stMultiSelect div[data-baseweb="select"] svg,
  .stSelectbox div[data-baseweb="select"] svg {
    fill: #0f172a !important;
  }
  
  .js-plotly-plot,
  .plotly,
  .plot-container,
  [data-testid="stPlotlyChart"] {
    background-color: #ffffff !important;
  }
  
  .plotly .modebar {
    background-color: #ffffff !important;
  }
  
  [data-baseweb] {
    color-scheme: light !important;
  }
  
  .element-container,
  .stContainer,
  [data-testid="stVerticalBlock"] {
    background-color: transparent !important;
  }
  
  .streamlit-expanderHeader {
    background: linear-gradient(135deg, #e8f5e9, #e3f2fd) !important;
    color: #0f172a !important;
  }
  
  .streamlit-expanderContent {
    background: #ffffff !important;
    color: #0f172a !important;
  }
</style>
"""

# -----------------------------------------------------------------------------
# Configuração Supabase para inserção de produtos
# -----------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_ANON_KEY = st.secrets["SUPABASE_KEY"]  # Renomeado para ser mais claro

# Estado global do Supabase (será inicializado conforme necessário)
supabase: Client = None
# Tokens de sessão armazenados na sessão do Streamlit
if "sb_access_token" not in st.session_state:
    st.session_state.sb_access_token = ""
if "sb_refresh_token" not in st.session_state:
    st.session_state.sb_refresh_token = ""

def initialize_supabase():
    """Inicializa cliente Supabase se ainda não foi feito."""
    global supabase
    if supabase is None and SUPABASE_URL and SUPABASE_ANON_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def ensure_session() -> bool:
    """
    Garante que o cliente supabase tenha uma sessão válida.
    Retorna True se usuário estiver autenticado, False caso contrário.
    """
    initialize_supabase()
    if not supabase:
        return False

    # Se temos tokens guardados, restaura sessão e tenta refresh
    if st.session_state.sb_access_token and st.session_state.sb_refresh_token:
        try:
            supabase.auth.set_session(
                access_token=st.session_state.sb_access_token,
                refresh_token=st.session_state.sb_refresh_token,
            )
        except Exception as e:
            st.write(f"[debug] set_session falhou: {e}")

    # Tenta obter usuário atual; se falhar, tenta refresh explícito
    try:
        user_resp = supabase.auth.get_user()
        if user_resp and getattr(user_resp, "user", None):
            session = supabase.auth.get_session()
            if session and getattr(session, "access_token", None) and getattr(session, "refresh_token", None):
                st.session_state.sb_access_token = session.access_token
                st.session_state.sb_refresh_token = session.refresh_token
            return True
    except Exception:
        try:
            refresh = supabase.auth.refresh_session()
            if refresh and getattr(refresh, "session", None):
                st.session_state.sb_access_token = refresh.session.access_token
                st.session_state.sb_refresh_token = refresh.session.refresh_token
                # testa novamente
                user_resp = supabase.auth.get_user()
                return bool(user_resp and getattr(user_resp, "user", None))
        except Exception:
            return False
    return False

def is_user_authenticated() -> bool:
    """Verifica se o usuário está autenticado."""
    return ensure_session()

def authenticate_user(email: str, password: str) -> bool:
    """Faz login do usuário no Supabase."""
    if not supabase:
        initialize_supabase()
        if not supabase:
            return False

    try:
        response = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )

        # Armazena tokens para reuso entre reruns
        session = response.session
        if session and session.access_token and session.refresh_token:
            st.session_state.sb_access_token = session.access_token
            st.session_state.sb_refresh_token = session.refresh_token
        else:
            # fallback: tenta pegar sessão atual
            current = supabase.auth.get_session()
            if current and current.access_token and current.refresh_token:
                st.session_state.sb_access_token = current.access_token
                st.session_state.sb_refresh_token = current.refresh_token

        return True
    except Exception as e:
        st.error(f"Erro de autenticação: {e}")
        return False

def logout_user():
    """Faz logout do usuário."""
    if supabase:
        try:
            supabase.auth.sign_out()
        except:
            pass

    # Limpa tokens armazenados
    st.session_state.sb_access_token = ""
    st.session_state.sb_refresh_token = ""

def auth_status_badge():
    """Retorna um texto curto com o status atual do ensure_session()."""
    ok = ensure_session()
    token_ok = bool(st.session_state.sb_access_token and st.session_state.sb_refresh_token)
    return f"{'🟢' if ok else '🔴'} sessão {'ok' if ok else 'inválida'} • tokens {'ok' if token_ok else 'ausentes'}"

def get_aws_credentials():
    """
    Obtém credenciais AWS do st.secrets.
    
    IMPORTANTE: st.secrets só existe no servidor Python, NUNCA no navegador.
    As credenciais são lidas apenas no servidor e nunca expostas ao frontend.
    
    Retorna: (aws_key, aws_secret, lambda_function_name, region)
    """
    try:
        # st.secrets é acessível apenas no servidor Streamlit, nunca no cliente
        aws_key = st.secrets["AWS_KEY"]
        aws_secret = st.secrets["AWS_SECRET"]
        lambda_function_name = st.secrets.get("LAMBDA_NAME", "plano-safra")
        region = st.secrets.get("AWS_REGION", "sa-east-1")
        return aws_key, aws_secret, lambda_function_name, region
    except (KeyError, AttributeError):
        return None, None, None, None

def trigger_lambda():
    """
    Dispara a função Lambda usando boto3.
    
    SEGURANÇA: Esta função executa 100% no servidor Python.
    As credenciais AWS nunca são enviadas ao navegador.
    
    Retorna: (success: bool, message: str)
    """
    # Verificar autenticação
    if not ensure_session():
        return False, "Usuário não autenticado. Faça login para executar esta ação."
    
    # Obter credenciais do secrets.toml (apenas no servidor)
    aws_key, aws_secret, lambda_function_name, region = get_aws_credentials()
    
    if not aws_key or not aws_secret:
        return False, "Credenciais AWS não configuradas em secrets.toml"
    
    try:
        # Criar cliente Lambda (executado apenas no servidor)
        # As credenciais nunca saem do servidor Python
        lambda_client = boto3.client(
            "lambda",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=region
        )
        
        # Invocar Lambda (chamada HTTP feita pelo servidor, não pelo navegador)
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType="Event",  # Assíncrono
            Payload=json.dumps({})
        )
        
        status_code = response.get("StatusCode")
        
        if status_code == 202:
            return True, f"Lambda '{lambda_function_name}' disparada com sucesso! Processamento iniciado em segundo plano."
        else:
            return False, f"Resposta inesperada da Lambda (Status: {status_code})"
            
    except Exception as e:
        error_msg = str(e)
        # Mensagens de erro genéricas (nunca expõem credenciais)
        if "UnrecognizedClientException" in error_msg or "InvalidClientTokenId" in error_msg:
            return False, "Credenciais AWS inválidas ou expiradas. Verifique secrets.toml"
        elif "ResourceNotFoundException" in error_msg:
            return False, f"Função Lambda '{lambda_function_name}' não encontrada na região {region}"
        elif "AccessDeniedException" in error_msg:
            return False, "Credenciais não têm permissão para invocar a função Lambda"
        else:
            # Não expor detalhes do erro que possam conter informações sensíveis
            return False, "Erro ao disparar Lambda. Verifique as configurações."

# -----------------------------------------------------------------------------
# Constantes globais
# -----------------------------------------------------------------------------
TARGET_YEAR = datetime.now().year

def check_product_exists(produto: str, local: str) -> bool:
    """Verifica se o produto já existe na tabela monitored_products."""
    try:
        ensure_session()
        response = (
            supabase.table("monitored_products")
            .select("ID")
            .eq("PRODUTO", produto.strip().upper())
            .eq("LOCAL", local.strip())
            .execute()
        )
        return bool(response.data)
    except Exception:
        return False

def insert_new_product(produto: str, local: str) -> bool:
    """Insere um novo produto na tabela monitored_products."""
    try:
        ensure_session()
        user_resp = supabase.auth.get_user()
        user_id = user_resp.user.id if user_resp and getattr(user_resp, "user", None) else None

        payload = {
            "PRODUTO": produto.strip().upper(),
            "LOCAL": local.strip(),
            "STATUS": "ADICIONADO"
        }
        if user_id:
            payload["CRIADO_POR"] = user_id  # respeita políticas RLS típicas

        response = supabase.table("monitored_products").insert(payload).execute()

        if hasattr(response, "error") and response.error:
            st.error(f"Erro Supabase (insert): {response.error}")
            return False

        return bool(response.data)
    except Exception:
        return False

def render_product_insertion_form():
    """Renderiza apenas o formulário de inserção de produtos."""
    col1, col2 = st.columns(2)

    st.caption(f"Status da sessão: {auth_status_badge()}")

    with col1:
        produto_input = st.text_input(
            "Produto",
            placeholder="Ex: DAMASCO, CAFÉ, MILHO",
            help="Nome do produto em português",
            key="produto_input"
        )

    with col2:
        local_input = st.text_input(
            "Local/País",
            placeholder="Ex: Turquia, Brasil, EUA",
            help="País ou região de origem",
            key="local_input"
        )

    # Validação e inserção
    if st.button("Verificar e Inserir", type="primary", use_container_width=True, key="insert_button"):
        if not produto_input.strip() or not local_input.strip():
            st.error("Preencha ambos os campos (Produto e Local).")
            return

        if not ensure_session():
            st.error("Faça login para inserir/verificar produto.")
            return

        with st.spinner("Verificando produto..."):
            exists = check_product_exists(produto_input, local_input)

        if exists:
            st.error(f"O produto **{produto_input.upper()}** já está cadastrado para **{local_input}**.")
        else:
            with st.spinner("Inserindo produto..."):
                success = insert_new_product(produto_input, local_input)

            if success:
                st.success(f"Produto **{produto_input.upper()}** inserido com sucesso para **{local_input}**!")
                st.info("O produto será processado automaticamente no próximo pipeline de análise.")
                # Limpar campos usando rerun para resetar o form
                st.rerun()
            else:
                st.error("Erro ao inserir o produto. Tente novamente.")
                st.caption("Se o erro persistir, verifique permissões RLS e o campo CRIADO_POR.")

    # Lista de produtos recentes
    st.markdown("---")
    section_subtitle("Produtos Recentes Adicionados")

    try:
        recent_response = supabase.table("monitored_products").select("PRODUTO, LOCAL, STATUS, DATA_CRIACAO").eq("STATUS", "ADICIONADO").order("DATA_CRIACAO", desc=True).limit(10).execute()

        if recent_response.data:
            recent_df = pd.DataFrame(recent_response.data)
            recent_df['DATA_CRIACAO'] = pd.to_datetime(recent_df['DATA_CRIACAO']).dt.strftime('%d/%m/%Y %H:%M')

            st.dataframe(
                recent_df[['PRODUTO', 'LOCAL', 'STATUS', 'DATA_CRIACAO']],
                use_container_width=True,
                column_config={
                    "PRODUTO": st.column_config.TextColumn("Produto"),
                    "LOCAL": st.column_config.TextColumn("Local"),
                    "STATUS": st.column_config.TextColumn("Status")
                }
            )
        else:
            st.info("Nenhum produto adicionado recentemente.")
    except Exception as e:
        st.error(f"Erro ao carregar produtos recentes: {e}")

def render_insert_product_view():
    """Renderiza a tela de inserção de produtos."""
    section_title("Inserir Novo Produto")

    initialize_supabase()
    if not supabase:
        st.error("Configuração do Supabase não encontrada. Verifique as variáveis de ambiente.")
        return

    # Inicializar estado de login na sessão se não existir
    if "login_success" not in st.session_state:
        st.session_state.login_success = False
    if "user_email" not in st.session_state:
        st.session_state.user_email = ""

    # Verificar se usuário está autenticado
    if not ensure_session():
        st.markdown("""
        🔐 **Autenticação necessária**

        Para inserir novos produtos, você precisa estar logado no sistema.
        """)

        with st.form("login_form", clear_on_submit=True):
            st.subheader("Login")
            email = st.text_input("Email", placeholder="seu@email.com", key="login_email_input")
            password = st.text_input("Senha", type="password", placeholder="••••••••", key="login_password_input")
            submitted = st.form_submit_button("Entrar", type="primary")

            if submitted:
                if email and password:
                    with st.spinner("Autenticando..."):
                        if authenticate_user(email, password):
                            st.session_state.login_success = True
                            st.session_state.user_email = email
                            st.success("Login realizado com sucesso!")
                            st.balloons()  # Animação de sucesso
                        else:
                            st.session_state.login_success = False
                            st.error("Email ou senha incorretos.")
                else:
                    st.error("Preencha email e senha.")

    # Se login foi bem-sucedido, mostrar o formulário diretamente
    if st.session_state.login_success:
        st.info("**Login realizado!** O formulário de inserção será mostrado abaixo.")

        # Mostrar diretamente o formulário de inserção após login
        user_email = st.session_state.user_email or "Usuário"

        st.success(f"Logado como: {user_email}")

        st.markdown("""
        Insira os dados do novo produto a ser monitorado. O sistema irá:
        - Verificar se o produto já existe
        - Inserir como "ADICIONADO" se for novo
        - Processar automaticamente no próximo pipeline
        """)

        # Botão de logout
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("Desconectar", help="Fazer logout", key="logout_after_login"):
                logout_user()
                st.session_state.login_success = False
                st.session_state.user_email = ""
                st.rerun()

        render_product_insertion_form()
        
        # Botão para disparar Lambda
        st.markdown("---")
        section_subtitle("Atualizar Safra")
        st.caption("Dispara o processamento da pipeline de análise de safra.")
        
        if st.button("Atualizar Safra", type="primary", use_container_width=True, key="trigger_lambda_btn_after_login"):
            with st.spinner("Disparando processamento da pipeline..."):
                success, message = trigger_lambda()
                if success:
                    st.success(f"{message}")
                    st.info("O processamento pode levar alguns minutos. Os resultados aparecerão no dashboard quando concluído.")
                else:
                    st.error(f"{message}")
        
        return

    # Usuário já estava autenticado (não passou pelo login)
    try:
        user_resp = supabase.auth.get_user()
        user_email = user_resp.user.email if user_resp and getattr(user_resp, "user", None) else st.session_state.user_email
    except:
        user_email = st.session_state.user_email or "Usuário"

    st.success(f"Logado como: {user_email}")

    st.markdown("""
    Insira os dados do novo produto a ser monitorado. O sistema irá:
    - Verificar se o produto já existe
    - Inserir como "ADICIONADO" se for novo
    - Processar automaticamente no próximo pipeline
    """)

    # Botão de logout
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Sair", help="Fazer logout"):
            logout_user()
            st.session_state.login_success = False
            st.session_state.user_email = ""
            st.rerun()

    render_product_insertion_form()

    # Botão para disparar Lambda
    st.markdown("---")
    section_subtitle("Atualizar Relatório")
    st.caption("Inicia o processamento do algoritmo de análise de safra e atualiza o dashboard.")
    
    if st.button("Executar Algoritmo", type="primary", use_container_width=True, key="trigger_lambda_btn"):
        with st.spinner("Disparando processamento da pipeline..."):
            success, message = trigger_lambda()
            if success:
                st.success(f"{message}")
                st.info("O processamento pode levar alguns minutos. Os resultados aparecerão no dashboard quando concluído.")
            else:
                st.error(f"{message}")

def section_title(text: str):
    st.markdown(f'<div class="section-title">{text}</div>', unsafe_allow_html=True)

def section_subtitle(text: str):
    st.markdown(f'<div class="section-subtitle">{text}</div>', unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Carregamento de dados
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    """Carrega dados de calendário e análises do Supabase."""
    initialize_supabase()
    if not supabase:
        st.error("Conexão com Supabase não configurada.")
        return None, None

    try:
        # Buscar dados da view dashboard
        dashboard_response = supabase.table("vw_dashboard_products").select("*").execute()
        dashboard_rows = dashboard_response.data or []

        # Buscar dados de calendário (mantém compatibilidade com estrutura existente)
        calendar_response = supabase.table("vw_monitored_products").select("*").execute()
        calendar_rows = calendar_response.data or []

        # Processar dados de análise (da coluna RESULTADO)
        analysis_data = {
            "metadata": {
                "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cenario_climatico": "Cenário climático atualizado via Supabase",
                "ano_alvo": TARGET_YEAR
            },
            "analises": []
        }

        # Processar cada linha da view dashboard
        for row in dashboard_rows:
            resultado_json = row.get("RESULTADO")
            if resultado_json:
                try:
                    # Parse do JSON da coluna RESULTADO
                    analise = json.loads(resultado_json) if isinstance(resultado_json, str) else resultado_json
                    analysis_data["analises"].append(analise)
                except json.JSONDecodeError as e:
                    st.warning(f"Erro ao processar JSON para produto {row.get('PRODUTO', 'desconhecido')}: {e}")
                    continue

        # Processar dados de calendário (compatibilidade com estrutura existente)
        calendar_data = {
            "metadata": {
                "gerado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ano": TARGET_YEAR
            },
            "produtos": []
        }

        # Mapear produtos para formato de calendário
        mapa_meses = {
            "JAN": 1, "FEV": 2, "MAR": 3, "ABR": 4, "MAI": 5, "JUN": 6,
            "JUL": 7, "AGO": 8, "SET": 9, "OUT": 10, "NOV": 11, "DEZ": 12
        }

        # Criar set de produtos que estão no relatório (análises)
        produtos_no_relatorio = {analise.get("produto", "").strip().upper() for analise in analysis_data["analises"]}

        for row in calendar_rows:
            produto = row.get("PRODUTO", "").strip()
            safra = row.get("COLHEITA", "")
            local = row.get("LOCAL", "")

            # Processar meses ativos baseado na safra
            meses_ativos = {mes: False for mes in MESES}
            if safra and isinstance(safra, str):
                partes = [p.strip().upper() for p in safra.split('-')]
                if len(partes) == 2 and partes[0] in mapa_meses and partes[1] in mapa_meses:
                    ini = mapa_meses[partes[0]]
                    fim = mapa_meses[partes[1]]

                    if ini <= fim:
                        for i in range(ini, fim + 1):
                            meses_ativos[MESES[i-1]] = True
                    else:
                        # Safra que cruza o ano (ex: DEZ-MAR)
                        for i in range(ini, 13):
                            meses_ativos[MESES[i-1]] = True
                        for i in range(1, fim + 1):
                            meses_ativos[MESES[i-1]] = True

            calendar_data["produtos"].append({
                "produto": produto,
                "local": local,
                "no_relatorio": produto.upper() in produtos_no_relatorio,
                "meses_ativos": meses_ativos
            })

        return calendar_data, analysis_data

    except Exception as e:
        st.error(f"Erro ao carregar dados do Supabase: {e}")
        return None, None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
MESES = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
MESES_LABELS = {
    "JAN": "Janeiro",
    "FEV": "Fevereiro",
    "MAR": "Março",
    "ABR": "Abril",
    "MAI": "Maio",
    "JUN": "Junho",
    "JUL": "Julho",
    "AGO": "Agosto",
    "SET": "Setembro",
    "OUT": "Outubro",
    "NOV": "Novembro",
    "DEZ": "Dezembro",
}
RELATORIO_MES = "Dezembro"


def sentiment_icon(sent):
    return ""


def build_calendar_html(produtos: List[Dict]) -> str:
    """Gera HTML + JS leve para um calendário com hover estável."""
    por_mes = {m: [] for m in MESES}
    for item in produtos:
        nome = item.get("produto", "").strip()
        origem = item.get("local", "").strip()
        meses = item.get("meses_ativos", {})
        tracked = item.get("no_relatorio", False)
        for mes in MESES:
            if meses.get(mes, False):
                por_mes[mes].append({"nome": nome, "tracked": tracked, "origem": origem})

    calendar_js = json.dumps(por_mes, ensure_ascii=False)
    months_js = json.dumps(
        [{"key": m, "label": MESES_LABELS.get(m, m)} for m in MESES],
        ensure_ascii=False,
    )

    html = f"""
    <style>
      /* Grid 12 colunas, ajustado para caber sem barra horizontal */
      .cal-wrapper {{
        display: grid;
        grid-template-columns: repeat(12, minmax(0, 1fr));
        gap: 6px;
        padding: 10px;
        background: #f8f9fa;
        border-radius: 10px;
        overflow: hidden;
        width: 100%;
      }}

      .cal-card {{
        position: relative;
        border-radius: 10px;
        padding: 12px 8px;
        text-align: center;
        font-weight: 700;
        font-size: 13px;
        cursor: default;
        min-height: 80px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.12);
        transition: transform .2s, box-shadow .2s;
      }}
      .cal-card:hover {{ transform: translateY(-3px); box-shadow: 0 6px 14px rgba(0,0,0,0.18); z-index: 2; }}
      .has-tracked {{ background: linear-gradient(135deg, #1b5e20, #2e7d32); color: #fff; }}
      .has-products {{ background: linear-gradient(135deg, #e5e7eb, #d1d5db); color: #374151; }}
      .is-empty {{ background: linear-gradient(135deg, #f3f4f6, #e5e7eb); color: #9ca3af; }}
      .cal-line {{ width: 40%; height: 3px; border-radius: 2px; margin: 8px auto 0; background: rgba(255,255,255,0.5); }}
      .has-products .cal-line {{ background: rgba(0,0,0,0.2); }}
      .is-empty .cal-line {{ background: rgba(0,0,0,0.2); }}
      .tt {{
        position: fixed;
        background: #2c3e50;
        color: #fff;
        padding: 12px;
        border-radius: 8px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.28);
        max-width: 320px;
        max-height: 360px;
        overflow-y: auto;
        font-size: 12px;
        display: none;
        z-index: 9999;
      }}
      .tt-header {{
        font-size: 15px;
        font-weight: 700;
        margin-bottom: 8px;
        padding-bottom: 6px;
        border-bottom: 1px solid rgba(255,255,255,0.25);
      }}
      .tt-item {{ padding: 4px 0; }}
      .tt-tracked {{ color: #ffd700; font-weight: 700; }}
      .tt-empty {{ color: #bfc9d3; font-style: italic; }}
      .tt::-webkit-scrollbar {{ width: 6px; }}
      .tt::-webkit-scrollbar-thumb {{ background: rgba(255,255,255,0.3); border-radius: 3px; }}
    </style>
    <div class="cal-wrapper" id="cal-wrapper"></div>
    <div class="tt" id="tt"></div>
    <script>
      const data = {calendar_js};
      const months = {months_js};
      const wrap = document.getElementById('cal-wrapper');
      const tt = document.getElementById('tt');

      function buildTooltip(m, items) {{
        let html = `<div class='tt-header'>${{m}}</div>`;
        if (!items.length) {{
          html += `<div class='tt-item tt-empty'>Sem produtos</div>`;
        }} else {{
          const sorted = [...items].sort((a,b)=> (b.tracked?1:0) - (a.tracked?1:0));
          sorted.forEach(it => {{
            const label = it.tracked ? '[R]' : '[-]';
            const cls = it.tracked ? 'tt-tracked' : '';
            const origem = it.origem ? it.origem : 'Origem não informada';
            html += `<div class='tt-item ${{cls}}'>${{label}} ${{it.nome}}<br><span style="opacity:0.85;font-weight:500;">Origem: ${{origem}}</span></div>`;
          }});
        }}
        return html;
      }}

      function showTT(evt, html) {{
        tt.innerHTML = html;
        tt.style.display = 'block';
        const rect = evt.currentTarget.getBoundingClientRect();
        const ttRect = tt.getBoundingClientRect();
        let top = rect.top - ttRect.height - 10;
        if (top < 10) top = rect.bottom + 10;
        let left = rect.left + rect.width/2 - ttRect.width/2;
        if (left < 10) left = 10;
        if (left + ttRect.width > window.innerWidth - 10) left = window.innerWidth - ttRect.width - 10;
        tt.style.top = `${{top}}px`;
        tt.style.left = `${{left}}px`;
      }}
      function hideTT() {{ tt.style.display = 'none'; }}

      months.forEach(m => {{
        const items = data[m.key] || [];
        const hasTracked = items.some(i => i.tracked);
        const hasProducts = items.length > 0;
        const cls = hasTracked ? 'has-tracked' : (hasProducts ? 'has-products' : 'is-empty');
        const div = document.createElement('div');
        div.className = `cal-card ${{cls}}`;
        div.innerHTML = `<div>${{m.label}}</div><div class='cal-line'></div>`;
        const ttHtml = buildTooltip(m.label, items);
        div.addEventListener('mouseenter', (e)=> showTT(e, ttHtml));
        div.addEventListener('mouseleave', hideTT);
        wrap.appendChild(div);
      }});
    </script>
    """
    return html


def render_calendar_list(produtos: List[Dict], analises: List[Dict]) -> None:
    """Renderiza lista mês a mês em cards compactos (sem hover)."""
    emoji_sent = {"POSITIVO": "🟢", "NEUTRO": "⚪", "NEGATIVO": "🔴"}
    # Mapa de produto -> sentimento (normalizado em upper)
    mapa_sent = {}
    for a in analises:
        nome = (a.get("produto") or "").strip().upper()
        mapa_sent[nome] = a.get("sentimento", "").upper()

    por_mes = {m: [] for m in MESES}
    for item in produtos:
        nome = item.get("produto", "").strip()
        local = (item.get("local", "") or "").strip() or "Origem não informada"
        nome_key = nome.upper()
        meses = item.get("meses_ativos", {})
        tracked = item.get("no_relatorio", False)
        sentimento = mapa_sent.get(nome_key)
        emoji = emoji_sent.get(sentimento, "•") if tracked else "○"
        for mes in MESES:
            if meses.get(mes, False):
                por_mes[mes].append({"nome": nome, "tracked": tracked, "emoji": emoji, "local": local})

    # Monta HTML em grid responsivo
    cards_html = ""
    for mes in MESES:
        mes_label = MESES_LABELS.get(mes, mes)
        items = sorted(por_mes[mes], key=lambda x: not x["tracked"])
        if items:
            itens_html = "".join(
                f"<div class='cal-item {'cal-tracked' if it['tracked'] else ''}' title='Origem: {html.escape(it['local'], quote=True)}'>"
                f"{it['emoji']} {it['nome']}"
                f"</div>"
                for it in items
            )
        else:
            itens_html = "<div class='cal-item cal-empty'>Sem produtos</div>"

        cards_html += f"""
        <div class="cal-card-list">
          <div class="cal-card-header">{mes_label}</div>
          <div class="cal-card-body">
            {itens_html}
          </div>
        </div>
        """

    st.markdown(
        f"""
        <style>
          .cal-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 10px;
          }}
          .cal-card-list {{
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.05);
            display: flex;
            flex-direction: column;
            min-height: 140px;
          }}
          .cal-card-header {{
            padding: 10px 12px;
            font-weight: 800;
            color: #0f172a;
            border-bottom: 1px solid #e5e7eb;
            text-align: center;
            background: linear-gradient(135deg, #e0f2f1, #c8e6c9);
            border-top-left-radius: 10px;
            border-top-right-radius: 10px;
          }}
          .cal-card-body {{
            padding: 8px 12px 12px 12px;
            font-size: 13px;
            color: #374151;
            display: flex;
            flex-direction: column;
            gap: 4px;
          }}
          .cal-item {{
            padding: 4px 6px;
            border-radius: 6px;
            background: #f9fafb;
            border: 1px solid #e5e7eb;
          }}
          .cal-item.cal-tracked {{
            background: #fff7e6;
            border-color: #facc15;
            color: #92400e;
            font-weight: 700;
          }}
          .cal-item.cal-empty {{
            font-style: italic;
            color: #9ca3af;
            background: #f3f4f6;
            border: 1px dashed #e5e7eb;
          }}
        </style>
        <div class="cal-grid">
          {cards_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metrics(calendar_data: Dict, analyses: List[Dict]) -> None:
    total_produtos = len(calendar_data["produtos"])
    produtos_tracked = sum(1 for p in calendar_data["produtos"] if p.get("no_relatorio", False))
    total_analises = len(analyses)
    pos = sum(1 for a in analyses if a.get("sentimento") == "POSITIVO")
    neg = sum(1 for a in analyses if a.get("sentimento") == "NEGATIVO")
    neu = sum(1 for a in analyses if a.get("sentimento") == "NEUTRO")

    st.markdown(
        """
        <style>
          .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-top: 8px;
          }
          .metric-block {
            display: flex;
            flex-direction: column;
            gap: 6px;
          }
          .metric-title {
            font-size: 13px;
            color: #374151;
            margin: 0;
            font-weight: 600;
            text-align: center;
          }
          .metric-card {
            background: #ffffff;
            border-radius: 10px;
            padding: 6px 6px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            border: 1px solid #e5e7eb;
            text-align: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            min-height: 110px;
          }
          .metric-value {
            font-size: 36px;
            color: #111827;
            margin: 0;
            font-weight: 700;
            line-height: 1.05;
          }
          .metric-sub {
            font-size: 11px;
            color: #6b7280;
            margin: 4px 0 0 0;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="metric-grid">
          <div class="metric-block">
            <p class="metric-title">Total de Produtos</p>
            <div class="metric-card">
              <p class="metric-value">{total_produtos}</p>
            </div>
          </div>
          <div class="metric-block">
            <p class="metric-title">Produtos Rastreados</p>
            <div class="metric-card">
              <p class="metric-value">{produtos_tracked}</p>
            </div>
          </div>
          <div class="metric-block">
            <p class="metric-title">Análises Disponíveis</p>
            <div class="metric-card">
              <p class="metric-value">{total_analises}</p>
            </div>
          </div>
          <div class="metric-block">
            <p class="metric-title">Perspectivas</p>
            <div class="metric-card">
              <p class="metric-value">{pos} / {neu} / {neg}</p>
              <p class="metric-sub">Positivo / Neutro / Negativo</p>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filters_in_column(col, analysis_data: Dict) -> tuple[list[str], list[str]]:
    col.markdown("### Filtros")
    sentimentos = ["POSITIVO", "NEUTRO", "NEGATIVO"]
    # Usa .get() para evitar KeyError e filtra valores vazios
    paises = sorted({a.get("pais", "") for a in analysis_data["analises"] if a.get("pais")})
    # default vazio (mostra tudo), fallback para todos se nada selecionado
    sent_sel = col.multiselect("Perspectiva", sentimentos, default=[])
    pais_sel = col.multiselect("País", paises, default=[])
    if not sent_sel:
        sent_sel = sentimentos
    if not pais_sel:
        pais_sel = paises
    return sent_sel, pais_sel


def render_analyses(analises: List[Dict]) -> None:
    if not analises:
        st.warning("Nenhuma análise corresponde aos filtros selecionados.")
        return

    by_sent = defaultdict(list)
    for a in analises:
        sentimento = a.get("sentimento", "NEUTRO")
        by_sent[sentimento].append(a)

    icon_map = {"POSITIVO": "🟢", "NEUTRO": "⚪", "NEGATIVO": "🔴"}
    for sent in ["POSITIVO", "NEUTRO", "NEGATIVO"]:
        if sent not in by_sent:
            continue
        st.subheader(f"{icon_map.get(sent, '')} {sent}")
        for a in by_sent[sent]:
            label = f"{icon_map.get(a['sentimento'], '')} **{a['produto']}** ({a['pais']})"
            with st.expander(label, expanded=False):
                st.markdown("**📝 Resumo**")
                st.markdown(a.get("resumo", ""))
                if a.get("links"):
                    st.markdown("**🔗 Fontes (até 5):**")
                    for link in a["links"][:5]:
                        st.markdown(f"- [{link['titulo']}]({link['url']})")
                        st.caption(f"  Data: {link.get('data', 'N/A')}")


def render_stats(analises: List[Dict]) -> None:
    c1, c2 = st.columns(2)
    if analises:
        with c1:
            section_subtitle("Distribuição por Sentimento")
            # Usa .get() para evitar KeyError
            df = pd.DataFrame([a.get("sentimento", "NEUTRO") for a in analises], columns=["sentimento"])
            sent_counts = df["sentimento"].value_counts().reset_index()
            sent_counts.columns = ["sentimento", "contagem"]
            fig = px.pie(sent_counts, values="contagem", names="sentimento", color="sentimento",
                         color_discrete_map={"POSITIVO": "#16a34a", "NEUTRO": "#9ca3af", "NEGATIVO": "#dc2626"})
            fig.update_traces(
                textposition="inside",
                textinfo="label+percent",
                hovertemplate="%{label}: %{value}",
                textfont=dict(size=16, color="#ffffff"),
                texttemplate="<b>%{label}</b><br><b>%{percent}</b>"
            )
            fig.update_layout(showlegend=True, margin=dict(l=0, r=0, t=0, b=0), height=320)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            section_subtitle("Distribuição por País")
            # Usa .get() para evitar KeyError e filtra valores vazios
            df = pd.DataFrame([a.get("pais", "") for a in analises if a.get("pais")], columns=["pais"])
            df_pais_counts = df["pais"].value_counts().reset_index(name="count").rename(columns={"index": "pais"})
            bar_fig = px.bar(df_pais_counts, x="pais", y="count", text="count")
            bar_fig.update_traces(textposition="outside")
            bar_fig.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(bar_fig, use_container_width=True)


def render_alerts_view(cal: Dict, ana: Dict) -> None:
    """Tela 1: lembrete de produtos em alerta (sentimento NEGATIVO)."""
    section_title("Produtos em Alerta")
    st.caption("Lista de produtos com perspectiva NEGATIVA. Use esta visão para checar rapidamente o que exige atenção imediata.")
    alertas = [a for a in ana["analises"] if a.get("sentimento") == "NEGATIVO"]
    if not alertas:
        st.info("Nenhum produto em alerta no momento.")
        return
    alertas = sorted(alertas, key=lambda x: x.get("produto", ""))
    for a in alertas:
        with st.expander(f"🔴 {a.get('produto', '')} ({a.get('pais', '')})"):
            st.markdown("**Resumo**")
            st.markdown(a.get("resumo", ""))
            if a.get("links"):
                st.markdown("**Fontes (até 3):**")
                for link in a["links"][:3]:
                    st.markdown(f"- [{link['titulo']}]({link['url']})")
                    st.caption(f"  Data: {link.get('data', 'N/A')}")


# -----------------------------------------------------------------------------
# Views
# -----------------------------------------------------------------------------
def render_home(cal: Dict, ana: Dict) -> None:
    """Tela 2: principais (métricas, calendário, gráficos)."""
    section_title("Calendário")
    section_subtitle("Métricas Principais")
    render_metrics(cal, ana["analises"])

    st.markdown("---")
    section_subtitle("Calendário de Safra")
    st.caption("Aqui você vê, mês a mês, todos os produtos; os rastreados aparecem com a bolinha da cor da avaliação.")
    render_calendar_list(cal["produtos"], ana["analises"])


def render_analysis_view(cal: Dict, ana: Dict) -> None:
    """Tela 3: análises detalhadas."""
    section_title("Análises")
    col_list, col_filters = st.columns([3, 1])
    sent_filter, pais_filter = render_filters_in_column(col_filters, ana)
    analises_filtradas = [
        a for a in ana["analises"]
        if a.get("sentimento", "") in sent_filter and a.get("pais", "") in pais_filter
    ]
    with col_list:
        render_analyses(analises_filtradas)
    st.markdown("---")
    section_subtitle("Estatísticas Adicionais")
    render_stats(analises_filtradas)


# -----------------------------------------------------------------------------
# Clima (desativado temporariamente)
# -----------------------------------------------------------------------------
# def render_clima_view(ana):
#     section_title("Cenário Climático Global")
#     st.caption(f"Atualizado em: {ana['metadata']['data_geracao']}")
#     st.info(f"**{ana['metadata']['ano_alvo']}** — {ana['metadata']['cenario_climatico']}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    """Ponto de entrada do dashboard Streamlit."""
    cal, ana = load_data()
    if not cal or not ana:
        st.stop()

    # Aplica tema claro
    st.markdown(CSS, unsafe_allow_html=True)
    
    # JavaScript para forçar tema claro mesmo com sistema em modo escuro
    st.markdown("""
    <script>
    (function() {
        // Força color-scheme: light no documento
        document.documentElement.style.colorScheme = 'light';
        document.documentElement.setAttribute('data-theme', 'light');
        
        // Remove qualquer atributo de tema escuro
        document.documentElement.removeAttribute('data-dark-mode');
        document.documentElement.classList.remove('dark');
        
        // Força tema claro no body
        if (document.body) {
            document.body.style.colorScheme = 'light';
            document.body.classList.remove('dark');
        }
        
        // Força cores claras em elementos do Streamlit
        function forceLightTheme() {
            const elements = document.querySelectorAll('[class*="dark"], [data-theme="dark"]');
            elements.forEach(el => {
                el.classList.remove('dark');
                el.setAttribute('data-theme', 'light');
                el.style.colorScheme = 'light';
            });
            
            // Força cores claras em inputs e selects
            const inputs = document.querySelectorAll('input, select, textarea, [data-baseweb="select"], [data-baseweb="input"]');
            inputs.forEach(el => {
                el.style.backgroundColor = '#ffffff';
                el.style.color = '#0f172a';
                el.style.borderColor = '#cbd5e1';
            });
            
            // Força cores claras em gráficos Plotly
            const plotlyCharts = document.querySelectorAll('.js-plotly-plot, .plotly, .plot-container');
            plotlyCharts.forEach(el => {
                el.style.backgroundColor = '#ffffff';
            });
        }
        
        // Executa imediatamente e periodicamente
        forceLightTheme();
        setTimeout(forceLightTheme, 100);
        setTimeout(forceLightTheme, 500);
        
        // Observa mudanças e força tema claro continuamente
        const observer = new MutationObserver(function() {
            document.documentElement.style.colorScheme = 'light';
            document.documentElement.setAttribute('data-theme', 'light');
            if (document.body) {
                document.body.style.colorScheme = 'light';
            }
            forceLightTheme();
        });
        
        observer.observe(document.documentElement, { 
            attributes: true, 
            attributeFilter: ['class', 'data-theme', 'data-dark-mode'],
            subtree: true
        });
        observer.observe(document.body, { attributes: true, subtree: true });
    })();
    </script>
    """, unsafe_allow_html=True)

    st.title("Dashboard Inteligência de Safra")
    st.markdown(f"**Relatório referente a:** {RELATORIO_MES}")

    # Telas:
    # 1) Inicial: lembrete de produtos em alerta (sentimento NEGATIVO)
    # 2) Calendário: métricas, calendário, gráficos
    # 3) Análises: lista e estatísticas detalhadas
    # 4) Inserir Produto: adicionar novos produtos para monitoramento
    # 5) Clima: cenário climático global (desativado)
    if "screen" not in st.session_state:
        st.session_state.screen = "inicio"

    # Botões de navegação centralizados
    nav_container = st.container()
    with nav_container:
        # CSS específico para garantir altura uniforme e texto em uma linha
        st.markdown("""
        <style>
        /* Força altura uniforme e texto em uma linha para todos os botões de navegação */
        div[data-testid="column"] button[kind="secondary"],
        button[kind="secondary"] {
            height: 50px !important;
            min-height: 50px !important;
            max-height: 50px !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Distribuição melhorada das colunas (espaço equilibrado para todos os botões)
        col_nav = st.columns([1, 1, 1, 1.3, 1.3, 1, 1])
        if col_nav[1].button("Inicial", key="nav_inicio", use_container_width=True, type="secondary", help="Produtos em alerta"):
            st.session_state.screen = "inicio"
        if col_nav[2].button("Calendário", key="nav_principal", use_container_width=True, type="secondary", help="Métricas e calendário"):
            st.session_state.screen = "principal"
        if col_nav[3].button("Análises", key="nav_analises", use_container_width=True, type="secondary", help="Análises detalhadas"):
            st.session_state.screen = "analises"
        if col_nav[4].button("Adicionar Produto", key="nav_insert", use_container_width=True, type="secondary", help="Inserir novo produto"):
            st.session_state.screen = "insert"
        if col_nav[5].button("Recarregar Dados", key="nav_reload", use_container_width=True, type="secondary", help="Recarrega os dados do dashboard"):
            st.cache_data.clear()
            st.rerun()
            
    st.markdown("---")
    if st.session_state.screen == "inicio":
        render_alerts_view(cal, ana)
    elif st.session_state.screen == "principal":
        render_home(cal, ana)
    elif st.session_state.screen == "analises":
        render_analysis_view(cal, ana)
    elif st.session_state.screen == "insert":
        render_insert_product_view()
    else:
        # Clima desativado; fallback para tela principal
        render_home(cal, ana)

    st.markdown("---")
    st.caption(f"Dashboard gerado em {ana['metadata']['data_geracao']} • Ano alvo: {ana['metadata']['ano_alvo']}")


if __name__ == "__main__":
    main()