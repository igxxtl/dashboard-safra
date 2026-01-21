import html
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import boto3
import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from supabase import create_client, Client
st.set_page_config(
    page_title="Dashboard Safra (Piloto)",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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

RELATORIO_MES = MESES_LABELS.get(MESES[datetime.now().month - 1], "")

CSS = """
<style>
  :root,
  html,
  body {
    color-scheme: light !important;
    background: linear-gradient(180deg, #f4f7fb 0%, #eef2f7 100%) !important;
    background-color: #f4f7fb !important;
    color: #0f172a !important;
  }
  
  * {
    color-scheme: light !important;
  }
  
  html, body, .stApp {
    background: linear-gradient(180deg, #f4f7fb 0%, #eef2f7 100%) !important;
    background-color: #f4f7fb !important;
  }
  
  .stApp > header,
  .stApp > div,
  main,
  .block-container,
  [data-testid="stAppViewContainer"],
  [data-testid="stHeader"],
  [data-testid="stAppViewContainer"] > div,
  [data-testid="stHeader"] > div {
    background-color: #f4f7fb !important;
    background: #f4f7fb !important;
    color: #0f172a !important;
  }
  
  .stApp { 
    background: linear-gradient(180deg, #f4f7fb 0%, #eef2f7 100%) !important; 
  }
  
  main .block-container { 
    padding-top: 2rem; 
    padding-bottom: 2rem; 
  }
  
  .section-title { 
    font-size: 28px; 
    font-weight: 800; 
    color: #0f172a; 
    padding: 12px 0 8px 0; 
    margin: 0 0 16px 0; 
    letter-spacing: 0.3px; 
    border-bottom: 3px solid transparent;
    border-image: linear-gradient(135deg, #3b82f6, #8b5cf6) 1;
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.05), rgba(139, 92, 246, 0.05));
    padding-left: 16px;
    padding-right: 16px;
    border-radius: 8px 8px 0 0;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  }
  
  .section-subtitle { 
    font-size: 20px; 
    font-weight: 700; 
    color: #1e293b; 
    padding: 8px 0 4px 0; 
    margin: 0 0 12px 0;
    letter-spacing: 0.2px;
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.03), rgba(139, 92, 246, 0.03));
    padding-left: 12px;
    padding-right: 12px;
    border-radius: 6px;
    border-left: 4px solid #3b82f6;
  }
  
  .streamlit-expanderHeader { 
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important; 
    background-color: #ffffff !important;
    color: #0f172a !important; 
    border-radius: 10px !important; 
    padding: 14px 16px !important;
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08) !important;
    border: 1.5px solid #e5e7eb !important;
    transition: all 0.3s ease !important;
  }
  
  .streamlit-expanderHeader:hover {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    background-color: #f8fafc !important;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12) !important;
    transform: translateY(-1px) !important;
    border-color: #cbd5e1 !important;
  }
  
  .streamlit-expanderHeader:focus,
  .streamlit-expanderHeader:active {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    background-color: #f8fafc !important;
  }
  
  .streamlit-expanderContent { 
    background: #ffffff !important; 
    border-radius: 0 0 10px 10px !important; 
    padding: 16px !important;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06) !important;
    border: 1px solid rgba(203, 213, 225, 0.5) !important;
    border-top: none !important;
  }
  
  /* Botões secundários da navbar */
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
    background: linear-gradient(135deg, #ffffff, #f1f5f9) !important;
    color: #0f172a !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 10px !important;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.04) !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
  }
  
  div[data-testid="column"] .stButton>button[kind="secondary"]:hover,
  .stButton>button[kind="secondary"]:hover,
  button[kind="secondary"]:hover {
    background: linear-gradient(135deg, #f1f5f9, #e2e8f0) !important;
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1), 0 2px 4px rgba(0, 0, 0, 0.06) !important;
    transform: translateY(-1px) !important;
    border-color: #94a3b8 !important;
  }
  
  /* TODOS os botões - força cores claras */
  .stButton>button,
  button[data-baseweb="button"],
  button[kind="tertiary"],
  button {
    background-color: #ffffff !important;
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    color: #0f172a !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 8px !important;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06) !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
  }
  
  .stButton>button:hover,
  button[data-baseweb="button"]:hover,
  button[kind="tertiary"]:hover,
  button:hover {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    color: #0f172a !important;
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1) !important;
    transform: translateY(-1px) !important;
  }
  
  /* Botões primary mantêm cor primária mas com fundo claro */
  .stButton>button[kind="primary"],
  button[kind="primary"] {
    background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
    color: #ffffff !important;
    border: 1.5px solid #1e40af !important;
    border-radius: 10px !important;
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3), 0 2px 4px rgba(0, 0, 0, 0.1) !important;
    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
  }
  
  .stButton>button[kind="primary"]:hover,
  button[kind="primary"]:hover {
    background: linear-gradient(135deg, #2563eb, #1d4ed8) !important;
    box-shadow: 0 6px 16px rgba(59, 130, 246, 0.4), 0 4px 8px rgba(0, 0, 0, 0.12) !important;
    transform: translateY(-2px) !important;
  }
  
  
  .stMultiSelect div[data-baseweb="select"],
  .stSelectbox div[data-baseweb="select"],
  .stTextInput div[data-baseweb="input"],
  .stNumberInput div[data-baseweb="input"],
  .stDateInput div[data-baseweb="input"],
  .stTextArea div[data-baseweb="input"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05) !important;
    transition: all 0.2s ease !important;
  }
  
  .stMultiSelect div[data-baseweb="select"]:focus-within,
  .stSelectbox div[data-baseweb="select"]:focus-within,
  .stTextInput div[data-baseweb="input"]:focus-within,
  .stNumberInput div[data-baseweb="input"]:focus-within,
  .stDateInput div[data-baseweb="input"]:focus-within,
  .stTextArea div[data-baseweb="input"]:focus-within {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1), 0 2px 4px rgba(0, 0, 0, 0.08) !important;
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

  .stSelectbox div[data-baseweb="select"] button,
  .stMultiSelect div[data-baseweb="select"] button,
  [data-baseweb="select"] button,
  .stSelectbox div[data-baseweb="select"] button:focus,
  .stMultiSelect div[data-baseweb="select"] button:focus {
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    color: #0f172a !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 8px !important;
    transition: all 0.2s ease !important;
  }
  
  .stSelectbox div[data-baseweb="select"] button:hover,
  .stMultiSelect div[data-baseweb="select"] button:hover,
  [data-baseweb="select"] button:hover {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    border-color: #94a3b8 !important;
  }

  /* FORÇA CORES CLARAS EM TODOS OS ESTADOS DOS EXPANDERS */
  .streamlit-expanderHeader,
  .streamlit-expanderHeader *,
  [data-testid="stExpander"],
  [data-testid="stExpander"] > div,
  [data-testid="stExpander"] > div > div,
  [class*="streamlit-expander"],
  [class*="expander"] {
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
    color-scheme: light !important;
  }
  
  .streamlit-expanderHeader button,
  .streamlit-expanderHeader button *,
  .streamlit-expanderHeader button:focus,
  .streamlit-expanderHeader button:hover,
  .streamlit-expanderHeader button:active,
  .streamlit-expanderHeader button:visited,
  [data-testid="stExpander"] button,
  [data-testid="stExpander"] button *,
  [data-testid="stExpander"] button:focus,
  [data-testid="stExpander"] button:hover,
  [data-testid="stExpander"] button:active {
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
    border: none !important;
    box-shadow: none !important;
  }
  
  .streamlit-expanderHeader button:hover {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    background-color: #f8fafc !important;
  }
  
  /* Força cores em todos os elementos dentro do header do expander */
  .streamlit-expanderHeader span,
  .streamlit-expanderHeader div,
  .streamlit-expanderHeader p,
  .streamlit-expanderHeader label,
  .streamlit-expanderHeader strong,
  .streamlit-expanderHeader b,
  [data-testid="stExpander"] span,
  [data-testid="stExpander"] div,
  [data-testid="stExpander"] p,
  [data-testid="stExpander"] label,
  [data-testid="stExpander"] strong,
  [data-testid="stExpander"] b {
    background: transparent !important;
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  /* Força cores claras em dropdowns e menus de filtros */
  [data-baseweb="popover"],
  [data-baseweb="menu"],
  [role="listbox"],
  [role="option"],
  .stSelectbox [data-baseweb="popover"],
  .stMultiSelect [data-baseweb="popover"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 10px !important;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12), 0 2px 4px rgba(0, 0, 0, 0.08) !important;
    padding: 4px !important;
  }
  
  [data-baseweb="popover"] [role="option"],
  [data-baseweb="menu"] [role="option"],
  [role="listbox"] [role="option"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
    transition: all 0.2s ease !important;
  }
  
  [data-baseweb="popover"] [role="option"]:hover,
  [data-baseweb="menu"] [role="option"]:hover,
  [role="listbox"] [role="option"]:hover {
    background: linear-gradient(135deg, #f1f5f9, #e2e8f0) !important;
    color: #0f172a !important;
    transform: translateX(2px) !important;
  }
  
  /* FORÇA CORES CLARAS EM EXPANDERS - REGRAS ADICIONAIS E MAIS ESPECÍFICAS */
  .streamlit-expanderHeader {
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
    color-scheme: light !important;
  }
  
  .streamlit-expanderHeader:hover,
  .streamlit-expanderHeader:focus,
  .streamlit-expanderHeader:active,
  .streamlit-expanderHeader[aria-expanded="true"],
  .streamlit-expanderHeader[aria-expanded="false"] {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    background-color: #f8fafc !important;
    color: #0f172a !important;
  }
  
  .streamlit-expanderContent {
    background: #ffffff !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
    color-scheme: light !important;
  }
  
  /* Força cores claras em todos os elementos dentro de expanders */
  .streamlit-expanderContent *,
  .streamlit-expanderContent *::before,
  .streamlit-expanderContent *::after {
    color: #0f172a !important;
    background-color: transparent !important;
  }
  
  .streamlit-expanderContent p,
  .streamlit-expanderContent div,
  .streamlit-expanderContent span,
  .streamlit-expanderContent markdown,
  .streamlit-expanderContent a,
  .streamlit-expanderContent li,
  .streamlit-expanderContent ul,
  .streamlit-expanderContent ol {
    color: #0f172a !important;
    background-color: transparent !important;
  }
  
  /* Força cores em expanders mesmo quando expandidos ou em qualquer estado */
  [data-testid="stExpander"][aria-expanded="true"],
  [data-testid="stExpander"][aria-expanded="false"],
  [data-testid="stExpander"]:hover,
  [data-testid="stExpander"]:focus,
  [data-testid="stExpander"]:active {
    background: transparent !important;
    background-color: transparent !important;
    color-scheme: light !important;
  }
  
  [data-testid="stExpander"] .streamlit-expanderHeader {
    background: linear-gradient(135deg, #ffffff, #f8fafc) !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
  }
  
  [data-testid="stExpander"] .streamlit-expanderHeader:hover {
    background: linear-gradient(135deg, #f8fafc, #f1f5f9) !important;
    background-color: #f8fafc !important;
  }
  
  [data-testid="stExpander"] .streamlit-expanderContent {
    background: #ffffff !important;
    background-color: #ffffff !important;
    color: #0f172a !important;
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
    position: relative !important;
    z-index: 1 !important;
  }
  
  /* Garante que containers não sobreponham conteúdo */
  .stContainer > *,
  [data-testid="stVerticalBlock"] > * {
    position: relative !important;
    z-index: 2 !important;
  }
  
  /* Força tema claro em todos os elementos genéricos */
  div,
  span,
  p,
  h1, h2, h3, h4, h5, h6,
  section,
  article,
  aside,
  header,
  footer,
  nav,
  ul, ol, li,
  table, tr, td, th,
  label,
  caption {
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  /* Força fundo claro em containers e cards */
  [class*="container"],
  [class*="card"],
  [class*="box"],
  [class*="panel"],
  [class*="section"] {
    background-color: #ffffff !important;
    background: #ffffff !important;
    color: #0f172a !important;
  }
  
  /* Força cores em elementos de texto */
  p, span, div, label, caption, td, th {
    color: #0f172a !important;
  }
  
  /* Força fundo branco em elementos de formulário */
  form,
  fieldset,
  legend {
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  /* Remove qualquer herança de tema escuro */
  [data-theme="dark"],
  [class*="dark"],
  [class*="Dark"] {
    background-color: #ffffff !important;
    background: #ffffff !important;
    color: #0f172a !important;
  }
  
  /* Força cores em elementos do Streamlit que podem herdar do sistema */
  [data-testid],
  [data-baseweb],
  [role] {
    color-scheme: light !important;
  }
  
  /* Garante que todos os elementos dentro do app tenham fundo claro */
  .stApp * {
    color-scheme: light !important;
  }
  
  /* Força cores em elementos de lista e tabela */
  ul, ol {
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  li {
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  table {
    background-color: #ffffff !important;
    color: #0f172a !important;
  }
  
  td, th {
    background-color: #ffffff !important;
    color: #0f172a !important;
  }
  
  /* Força cores em elementos de markdown */
  .stMarkdown,
  .stMarkdown *,
  [data-testid="stMarkdownContainer"],
  [data-testid="stMarkdownContainer"] * {
    background-color: transparent !important;
    color: #0f172a !important;
  }
  
  /* Força cores em elementos de dataframe */
  [data-testid="stDataFrame"],
  [data-testid="stDataFrame"] *,
  .stDataFrame,
  .stDataFrame *,
  div[data-testid="stDataFrame"],
  div[data-testid="stDataFrame"] * {
    background-color: #ffffff !important;
    color: #0f172a !important;
    z-index: 1 !important;
    position: relative !important;
  }
  
  /* Garante que dataframes não sejam sobrepostos */
  [data-testid="stDataFrame"],
  [data-testid="stDataFrame"] > div,
  [data-testid="stDataFrame"] table {
    z-index: 10 !important;
    position: relative !important;
    opacity: 1 !important;
    visibility: visible !important;
    display: block !important;
  }
  
  /* Garante que elementos dentro de containers sejam visíveis */
  .stContainer [data-testid="stDataFrame"],
  [data-testid="stVerticalBlock"] [data-testid="stDataFrame"],
  [data-testid="stVerticalBlock"] > div > div [data-testid="stDataFrame"] {
    z-index: 20 !important;
    position: relative !important;
    opacity: 1 !important;
    visibility: visible !important;
    display: block !important;
  }
  
  /* Garante que tabelas dentro de dataframes sejam visíveis */
  [data-testid="stDataFrame"] table,
  [data-testid="stDataFrame"] tbody,
  [data-testid="stDataFrame"] tr,
  [data-testid="stDataFrame"] td,
  [data-testid="stDataFrame"] th {
    opacity: 1 !important;
    visibility: visible !important;
    display: table !important;
  }
  
  /* Remove qualquer overlay que possa estar sobrepondo */
  [data-testid="stDataFrame"]::before,
  [data-testid="stDataFrame"]::after {
    display: none !important;
  }
  
  /* Força cores em elementos de info/warning/error */
  .stInfo,
  .stWarning,
  .stError,
  .stSuccess {
    background-color: #ffffff !important;
    color: #0f172a !important;
  }
</style>
<script>
(function() {
  // Força tema claro no documento
  document.documentElement.setAttribute('data-theme', 'light');
  document.documentElement.style.colorScheme = 'light';
  document.body.style.colorScheme = 'light';
  
  // Remove classes de tema escuro
  document.documentElement.classList.remove('dark', 'Dark');
  document.body.classList.remove('dark', 'Dark');
  
  // Observa mudanças no DOM para aplicar tema claro em elementos novos
  const observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
      mutation.addedNodes.forEach(function(node) {
        if (node.nodeType === 1) { // Element node
          // Remove classes de tema escuro
          if (node.classList) {
            node.classList.remove('dark', 'Dark');
          }
          // Força tema claro
          node.setAttribute('data-theme', 'light');
          node.style.colorScheme = 'light';
          
          // Aplica em todos os filhos
          const allChildren = node.querySelectorAll('*');
          allChildren.forEach(function(child) {
            if (child.classList) {
              child.classList.remove('dark', 'Dark');
            }
            child.setAttribute('data-theme', 'light');
            child.style.colorScheme = 'light';
          });
        }
      });
    });
  });
  
  // Inicia observação
  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
  
  // Função específica para forçar cores claras em expanders
  function forceExpanderLightTheme() {
    // Seleciona todos os expanders
    const expanders = document.querySelectorAll('[data-testid="stExpander"], .streamlit-expanderHeader, .streamlit-expanderContent');
    expanders.forEach(function(expander) {
      // Remove classes de tema escuro
      if (expander.classList) {
        expander.classList.remove('dark', 'Dark');
      }
      // Força tema claro
      expander.setAttribute('data-theme', 'light');
      expander.style.colorScheme = 'light';
      
      // Força cores específicas no header
      if (expander.classList && expander.classList.contains('streamlit-expanderHeader')) {
        expander.style.background = 'linear-gradient(135deg, #ffffff, #f8fafc)';
        expander.style.backgroundColor = '#ffffff';
        expander.style.color = '#0f172a';
      }
      
      // Força cores específicas no content
      if (expander.classList && expander.classList.contains('streamlit-expanderContent')) {
        expander.style.background = '#ffffff';
        expander.style.backgroundColor = '#ffffff';
        expander.style.color = '#0f172a';
      }
      
      // Força cores em todos os botões dentro do expander
      const buttons = expander.querySelectorAll('button');
      buttons.forEach(function(btn) {
        btn.style.background = 'linear-gradient(135deg, #ffffff, #f8fafc)';
        btn.style.backgroundColor = '#ffffff';
        btn.style.color = '#0f172a';
        btn.style.border = 'none';
      });
      
      // Força cores em todos os elementos de texto dentro do expander
      const textElements = expander.querySelectorAll('span, div, p, label, strong, b');
      textElements.forEach(function(el) {
        el.style.color = '#0f172a';
        el.style.backgroundColor = 'transparent';
      });
    });
  }
  
  // Aplica tema claro em todos os elementos existentes
  function applyLightTheme() {
    const allElements = document.querySelectorAll('*');
    allElements.forEach(function(el) {
      if (el.classList) {
        el.classList.remove('dark', 'Dark');
      }
      el.setAttribute('data-theme', 'light');
      el.style.colorScheme = 'light';
    });
    
    // Aplica tema específico em expanders
    forceExpanderLightTheme();
  }
  
  // Aplica imediatamente
  applyLightTheme();
  
  // Aplica após carregamento completo
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', applyLightTheme);
  } else {
    applyLightTheme();
  }
  
  // Observa especificamente mudanças em expanders
  const expanderObserver = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
      mutation.addedNodes.forEach(function(node) {
        if (node.nodeType === 1) {
          if (node.matches && (node.matches('[data-testid="stExpander"]') || node.matches('.streamlit-expanderHeader') || node.matches('.streamlit-expanderContent'))) {
            forceExpanderLightTheme();
          }
          // Verifica se contém expanders
          const nestedExpanders = node.querySelectorAll ? node.querySelectorAll('[data-testid="stExpander"], .streamlit-expanderHeader, .streamlit-expanderContent') : [];
          if (nestedExpanders.length > 0) {
            forceExpanderLightTheme();
          }
        }
      });
    });
  });
  
  // Inicia observação específica para expanders
  expanderObserver.observe(document.body, {
    childList: true,
    subtree: true,
    attributes: true,
    attributeFilter: ['class', 'style', 'aria-expanded']
  });
  
  // Função para garantir visibilidade de dataframes
  function ensureDataFrameVisibility() {
    const dataFrames = document.querySelectorAll('[data-testid="stDataFrame"]');
    dataFrames.forEach(function(df) {
      df.style.zIndex = '20';
      df.style.position = 'relative';
      df.style.opacity = '1';
      df.style.visibility = 'visible';
      df.style.display = 'block';
      
      // Garante que elementos filhos também sejam visíveis
      const children = df.querySelectorAll('*');
      children.forEach(function(child) {
        child.style.opacity = '1';
        child.style.visibility = 'visible';
      });
    });
  }
  
  // Aplica periodicamente para garantir (fallback)
  setInterval(function() {
    applyLightTheme();
    forceExpanderLightTheme();
    ensureDataFrameVisibility();
  }, 500);
  
  // Aplica imediatamente
  ensureDataFrameVisibility();
  
  // Observa mudanças para garantir visibilidade de novos dataframes
  const dfObserver = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
      mutation.addedNodes.forEach(function(node) {
        if (node.nodeType === 1) {
          if (node.matches && node.matches('[data-testid="stDataFrame"]')) {
            ensureDataFrameVisibility();
          }
          const nestedDFs = node.querySelectorAll ? node.querySelectorAll('[data-testid="stDataFrame"]') : [];
          if (nestedDFs.length > 0) {
            ensureDataFrameVisibility();
          }
        }
      });
    });
  });
  
  dfObserver.observe(document.body, {
    childList: true,
    subtree: true
  });
})();
</script>
"""

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_ANON_KEY = st.secrets["SUPABASE_KEY"]

supabase: Optional[Client] = None
if "sb_access_token" not in st.session_state:
    st.session_state.sb_access_token = ""
if "sb_refresh_token" not in st.session_state:
    st.session_state.sb_refresh_token = ""

def initialize_supabase() -> None:
    """Initialize Supabase client if not already done."""
    global supabase
    if supabase is None and SUPABASE_URL and SUPABASE_ANON_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

def ensure_session() -> bool:
    """Ensure Supabase client has a valid session. Returns True if user is authenticated."""
    initialize_supabase()
    if not supabase:
        return False

    if st.session_state.sb_access_token and st.session_state.sb_refresh_token:
        try:
            supabase.auth.set_session(
                access_token=st.session_state.sb_access_token,
                refresh_token=st.session_state.sb_refresh_token,
            )
        except Exception as e:
            st.write(f"[debug] set_session failed: {e}")

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
                user_resp = supabase.auth.get_user()
                return bool(user_resp and getattr(user_resp, "user", None))
        except Exception:
            return False
    return False

def is_user_authenticated() -> bool:
    """Check if user is authenticated."""
    return ensure_session()

def authenticate_user(email: str, password: str) -> bool:
    """Authenticate user with Supabase."""
    if not supabase:
        initialize_supabase()
        if not supabase:
            return False

    try:
        response = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )

        session = response.session
        if session and session.access_token and session.refresh_token:
            st.session_state.sb_access_token = session.access_token
            st.session_state.sb_refresh_token = session.refresh_token
        else:
            current = supabase.auth.get_session()
            if current and current.access_token and current.refresh_token:
                st.session_state.sb_access_token = current.access_token
                st.session_state.sb_refresh_token = current.refresh_token

        return True
    except Exception as e:
        st.error(f"Authentication error: {e}")
        return False

def logout_user() -> None:
    """Logout user."""
    if supabase:
        try:
            supabase.auth.sign_out()
        except:
            pass

    st.session_state.sb_access_token = ""
    st.session_state.sb_refresh_token = ""

def auth_status_badge() -> str:
    """Return a short text with current ensure_session() status."""
    ok = ensure_session()
    token_ok = bool(st.session_state.sb_access_token and st.session_state.sb_refresh_token)
    return f"{'🟢' if ok else '🔴'} sessão {'ok' if ok else 'inválida'} • tokens {'ok' if token_ok else 'ausentes'}"

def get_aws_credentials() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Get AWS credentials from st.secrets. Returns (aws_key, aws_secret, lambda_function_name, region)."""
    try:
        aws_key = st.secrets["AWS_KEY"]
        aws_secret = st.secrets["AWS_SECRET"]
        lambda_function_name = st.secrets.get("LAMBDA_NAME", "plano-safra")
        region = st.secrets.get("AWS_REGION", "sa-east-1")
        return aws_key, aws_secret, lambda_function_name, region
    except (KeyError, AttributeError):
        return None, None, None, None

def trigger_lambda() -> Tuple[bool, str]:
    """Trigger Lambda function using boto3. Returns (success: bool, message: str)."""
    if not ensure_session():
        return False, "Usuário não autenticado. Faça login para executar esta ação."
    
    aws_key, aws_secret, lambda_function_name, region = get_aws_credentials()
    
    if not aws_key or not aws_secret:
        return False, "Credenciais AWS não configuradas em secrets.toml"
    
    try:
        lambda_client = boto3.client(
            "lambda",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=region
        )
        
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType="Event",
            Payload=json.dumps({})
        )
        
        status_code = response.get("StatusCode")
        
        if status_code == 202:
            return True, f"Lambda '{lambda_function_name}' disparada com sucesso! Processamento iniciado em segundo plano."
        else:
            return False, f"Resposta inesperada da Lambda (Status: {status_code})"
            
    except Exception as e:
        error_msg = str(e)
        if "UnrecognizedClientException" in error_msg or "InvalidClientTokenId" in error_msg:
            return False, "Credenciais AWS inválidas ou expiradas. Verifique secrets.toml"
        elif "ResourceNotFoundException" in error_msg:
            return False, f"Função Lambda '{lambda_function_name}' não encontrada na região {region}"
        elif "AccessDeniedException" in error_msg:
            return False, "Credenciais não têm permissão para invocar a função Lambda"
        else:
            return False, "Erro ao disparar Lambda. Verifique as configurações."

TARGET_YEAR = datetime.now().year

def check_product_exists(produto: str, local: str) -> bool:
    """Check if product already exists in monitored_products table."""
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
    """Insert a new product into monitored_products table."""
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
            payload["CRIADO_POR"] = user_id

        response = supabase.table("monitored_products").insert(payload).execute()

        if hasattr(response, "error") and response.error:
            st.error(f"Erro Supabase (insert): {response.error}")
            return False

        return bool(response.data)
    except Exception:
        return False

def render_product_insertion_form() -> None:
    """Render product insertion form."""
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
                st.rerun()
            else:
                st.error("Erro ao inserir o produto. Tente novamente.")
                st.caption("Se o erro persistir, verifique permissões RLS e o campo CRIADO_POR.")

    st.markdown("---")
    section_subtitle("Produtos Recentes Adicionados")

    with st.container():
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

def render_insert_product_view() -> None:
    """Render product insertion view."""
    section_title("Inserir Novo Produto")

    initialize_supabase()
    if not supabase:
        st.error("Configuração do Supabase não encontrada. Verifique as variáveis de ambiente.")
        return

    if "login_success" not in st.session_state:
        st.session_state.login_success = False
    if "user_email" not in st.session_state:
        st.session_state.user_email = ""

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

    if st.session_state.login_success:
        st.info("**Login realizado!** O formulário de inserção será mostrado abaixo.")

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

def section_title(text: str) -> None:
    st.markdown(f'<div class="section-title">{text}</div>', unsafe_allow_html=True)

def section_subtitle(text: str) -> None:
    st.markdown(f'<div class="section-subtitle">{text}</div>', unsafe_allow_html=True)

@st.cache_data
def load_data() -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Load calendar and analysis data from Supabase."""
    initialize_supabase()
    if not supabase:
        st.error("Conexão com Supabase não configurada.")
        return None, None

    try:
        dashboard_response = supabase.table("vw_dashboard_products").select("*").execute()
        dashboard_rows = dashboard_response.data or []

        calendar_response = supabase.table("vw_monitored_products").select("*").execute()
        calendar_rows = calendar_response.data or []

        analysis_data = {
            "metadata": {
                "data_geracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cenario_climatico": "Cenário climático atualizado via Supabase",
                "ano_alvo": TARGET_YEAR
            },
            "analises": []
        }

        for row in dashboard_rows:
            resultado_json = row.get("RESULTADO")
            if resultado_json:
                try:
                    analise = json.loads(resultado_json) if isinstance(resultado_json, str) else resultado_json
                    analysis_data["analises"].append(analise)
                except json.JSONDecodeError as e:
                    st.warning(f"Erro ao processar JSON para produto {row.get('PRODUTO', 'desconhecido')}: {e}")
                    continue

        calendar_data = {
            "metadata": {
                "gerado_em": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ano": TARGET_YEAR
            },
            "produtos": []
        }

        mapa_meses = {
            "JAN": 1, "FEV": 2, "MAR": 3, "ABR": 4, "MAI": 5, "JUN": 6,
            "JUL": 7, "AGO": 8, "SET": 9, "OUT": 10, "NOV": 11, "DEZ": 12
        }

        produtos_no_relatorio = {analise.get("produto", "").strip().upper() for analise in analysis_data["analises"]}

        for row in calendar_rows:
            produto = row.get("PRODUTO", "").strip()
            safra = row.get("COLHEITA", "")
            local = row.get("LOCAL", "")

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

def sentiment_icon(sent: str) -> str:
    return ""

def build_calendar_html(produtos: List[Dict[str, Any]]) -> str:
    """Generate HTML + JS for a calendar with stable hover."""
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


def render_calendar_list(produtos: List[Dict[str, Any]], analises: List[Dict[str, Any]]) -> None:
    """Render month-by-month list in compact cards."""
    emoji_sent = {"POSITIVO": "🟢", "NEUTRO": "⚪", "NEGATIVO": "🔴"}
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
            background: linear-gradient(135deg, #ffffff, #f8fafc);
            border: 1.5px solid #e5e7eb;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08), 0 2px 4px rgba(0,0,0,0.04);
            display: flex;
            flex-direction: column;
            min-height: 140px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            overflow: hidden;
          }}
          .cal-card-list:hover {{
            box-shadow: 0 8px 20px rgba(0,0,0,0.12), 0 4px 8px rgba(0,0,0,0.06);
            transform: translateY(-2px);
            border-color: #cbd5e1;
          }}
          .cal-card-header {{
            padding: 12px 14px;
            font-weight: 800;
            color: #0f172a;
            border-bottom: 2px solid rgba(203, 213, 225, 0.3);
            text-align: center;
            background: linear-gradient(135deg, #e0f2f1, #c8e6c9, #a7f3d0);
            border-top-left-radius: 12px;
            border-top-right-radius: 12px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            letter-spacing: 0.3px;
          }}
          .cal-card-body {{
            padding: 10px 14px 14px 14px;
            font-size: 13px;
            color: #374151;
            display: flex;
            flex-direction: column;
            gap: 6px;
            background: linear-gradient(135deg, #ffffff, #fafbfc);
          }}
          .cal-item {{
            padding: 6px 10px;
            border-radius: 8px;
            background: linear-gradient(135deg, #f9fafb, #f3f4f6);
            border: 1.5px solid #e5e7eb;
            transition: all 0.2s ease;
          }}
          .cal-item:hover {{
            background: linear-gradient(135deg, #f3f4f6, #e5e7eb);
            transform: translateX(2px);
            box-shadow: 0 2px 4px rgba(0,0,0,0.06);
          }}
          .cal-item.cal-tracked {{
            background: linear-gradient(135deg, #fef3c7, #fde68a);
            border-color: #facc15;
            color: #92400e;
            font-weight: 700;
            box-shadow: 0 2px 6px rgba(250, 204, 21, 0.2);
          }}
          .cal-item.cal-tracked:hover {{
            background: linear-gradient(135deg, #fde68a, #fcd34d);
            box-shadow: 0 4px 8px rgba(250, 204, 21, 0.3);
          }}
          .cal-item.cal-empty {{
            font-style: italic;
            color: #9ca3af;
            background: linear-gradient(135deg, #f3f4f6, #e5e7eb);
            border: 1.5px dashed #d1d5db;
          }}
        </style>
        <div class="cal-grid">
          {cards_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def enforce_plotly_theme(fig: Figure) -> None:
    fig.update_layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        font_color="#0f172a",
        legend=dict(font=dict(color="#0f172a")),
    )


def render_metrics(calendar_data: Dict[str, Any], analyses: List[Dict[str, Any]]) -> None:
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
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-top: 12px;
          }
          .metric-block {
            display: flex;
            flex-direction: column;
            gap: 8px;
          }
          .metric-title {
            font-size: 14px;
            color: #475569;
            margin: 0;
            font-weight: 700;
            text-align: center;
            letter-spacing: 0.2px;
            text-transform: uppercase;
            font-size: 12px;
          }
          .metric-card {
            background: linear-gradient(135deg, #ffffff, #f8fafc);
            border-radius: 12px;
            padding: 16px 12px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.1), 0 2px 4px rgba(0,0,0,0.06);
            border: 1.5px solid #e5e7eb;
            text-align: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            min-height: 120px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
          }
          .metric-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #3b82f6, #8b5cf6, #ec4899);
            opacity: 0;
            transition: opacity 0.3s ease;
          }
          .metric-card:hover {
            box-shadow: 0 8px 24px rgba(0,0,0,0.15), 0 4px 8px rgba(0,0,0,0.08);
            transform: translateY(-3px);
            border-color: #cbd5e1;
          }
          .metric-card:hover::before {
            opacity: 1;
          }
          .metric-value {
            font-size: 42px;
            background: linear-gradient(135deg, #0f172a, #1e293b);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin: 0;
            font-weight: 800;
            line-height: 1.05;
            letter-spacing: -0.5px;
          }
          .metric-sub {
            font-size: 12px;
            color: #6b7280;
            margin: 6px 0 0 0;
            font-weight: 500;
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


def render_filters_in_column(col: Any, analysis_data: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    col.markdown("### Filtros")
    sentimentos = ["POSITIVO", "NEUTRO", "NEGATIVO"]
    paises = sorted({a.get("pais", "") for a in analysis_data["analises"] if a.get("pais")})
    sent_sel = col.multiselect("Perspectiva", sentimentos, default=[])
    pais_sel = col.multiselect("País", paises, default=[])
    if not sent_sel:
        sent_sel = sentimentos
    if not pais_sel:
        pais_sel = paises
    return sent_sel, pais_sel


def render_analyses(analises: List[Dict[str, Any]]) -> None:
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


def render_stats(analises: List[Dict[str, Any]]) -> None:
    c1, c2 = st.columns(2)
    if analises:
        with c1:
            section_subtitle("Distribuição por Sentimento")
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
            enforce_plotly_theme(fig)
            fig.update_layout(showlegend=True, margin=dict(l=0, r=0, t=0, b=0), height=320)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            section_subtitle("Distribuição por País")
            df = pd.DataFrame([a.get("pais", "") for a in analises if a.get("pais")], columns=["pais"])
            df_pais_counts = df["pais"].value_counts().reset_index(name="count").rename(columns={"index": "pais"})
            bar_fig = px.bar(df_pais_counts, x="pais", y="count", text="count")
            bar_fig.update_traces(textposition="outside")
            enforce_plotly_theme(bar_fig)
            bar_fig.update_layout(height=320, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(bar_fig, use_container_width=True)


def render_alerts_view(cal: Dict[str, Any], ana: Dict[str, Any]) -> None:
    """Screen 1: alert products reminder (NEGATIVE sentiment)."""
    section_title("Produtos em Alerta")
    st.caption("Lista de produtos com perspectiva NEGATIVA. Utilize o resumo para identificar rapidamente o cenário e validar as informações nas notícias.")
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
                st.markdown("**Fontes (até 5):**")
                for link in a["links"][:5]:
                    st.markdown(f"- [{link['titulo']}]({link['url']})")
                    st.caption(f"  Data: {link.get('data', 'N/A')}")


def render_home(cal: Dict[str, Any], ana: Dict[str, Any]) -> None:
    """Screen 2: main (metrics, calendar, charts)."""
    section_title("Calendário")
    section_subtitle("Métricas Principais")
    render_metrics(cal, ana["analises"])

    st.markdown("---")
    section_subtitle("Calendário de Safra")
    st.caption("Aqui você vê, mês a mês, o calendário de safras, referente aos períodos de colheita de cada produto. Os produtos presentes no relatório deste mês estão destacados em amarelo, e a bolinha indica o status da safra.")
    render_calendar_list(cal["produtos"], ana["analises"])


def render_analysis_view(cal: Dict[str, Any], ana: Dict[str, Any]) -> None:
    """Screen 3: detailed analyses."""
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


def main() -> None:
    """Main entry point for Streamlit dashboard."""
    cal, ana = load_data()
    if not cal or not ana:
        st.stop()

    st.markdown(CSS, unsafe_allow_html=True)
    
    st.title("Dashboard Inteligência de Safra")
    st.markdown(f"Relatório referente ao mês de {RELATORIO_MES}")

    if "screen" not in st.session_state:
        st.session_state.screen = "inicio"

    nav_container = st.container()
    with nav_container:
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
        
        col_nav = st.columns([1, 1, 1, 1.3, 1.3, 1, 1])
        if col_nav[1].button("Tela Inicial", key="nav_inicio", use_container_width=True, type="secondary", help="Produtos em alerta"):
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
        render_home(cal, ana)

    st.markdown("---")
    st.caption(f"Dashboard gerado em {ana['metadata']['data_geracao']} • Ano alvo: {ana['metadata']['ano_alvo']}")


if __name__ == "__main__":
    main()
