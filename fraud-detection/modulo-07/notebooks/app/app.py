"""
Databricks App · revisor_transacciones
From Zero to Hero — Módulo 7

App de ejemplo (Streamlit) con dos partes:
  1) Un panorama del fraude (KPIs + gráficos) leído de gold_riesgo_diario.
  2) Un scorer que consulta un Model Serving endpoint para calificar una transacción.

Llamar al modelo por REST (endpoint) —en vez de cargarlo en la App— evita los problemas de
versiones de librerías (numpy/scikit-learn/pickle): la des-serialización ocurre en el endpoint.

Material de aprendizaje: datos y modelo 100% sintéticos.

Los participantes personalizan tres cosas marcadas con TODO (título, texto, umbral).

Configuración (variables de entorno, en app.yaml):
  Z2H_ENDPOINT      → nombre del serving endpoint (ej: zerotohero)
  Z2H_CATALOGO      → catálogo (para los KPIs)
  Z2H_SCHEMA        → schema (fin_<usuario>)
  Z2H_WAREHOUSE_ID  → ID de un SQL warehouse (para los KPIs)
"""

import os

import pandas as pd
import requests
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# ─────────────────────────────────────────────────────────────────────────────
# 📝 TODO 1 · cambia el título de tu App
TITULO = "Centro de control de fraude"
# 📝 TODO 2 · cambia el texto de bienvenida
BIENVENIDA = "Panorama del fraude y calificación de transacciones en vivo."
# 📝 TODO 3 · umbral: score a partir del cual una transacción se marca en ROJO (0 a 1)
UMBRAL_ROJO = 0.50
UMBRAL_AMARILLO = 0.20
# ─────────────────────────────────────────────────────────────────────────────

ENDPOINT = os.environ.get("Z2H_ENDPOINT", "").strip()
CATALOGO = os.environ.get("Z2H_CATALOGO", "").strip()
SCHEMA = os.environ.get("Z2H_SCHEMA", "").strip()
WAREHOUSE_ID = os.environ.get("Z2H_WAREHOUSE_ID", "").strip()

st.set_page_config(page_title=TITULO, page_icon="🛡️", layout="wide")

st.markdown(
    """
    <style>
      .stApp { background: #f7f8fa; }
      #MainMenu, footer { visibility: hidden; }
      .hero {
        background: linear-gradient(120deg, #1B3139 0%, #FF3621 140%);
        padding: 26px 32px; border-radius: 18px; color: #fff; margin-bottom: 20px;
        box-shadow: 0 8px 24px rgba(27,49,57,.18);
      }
      .hero h1 { color:#fff; font-size: 28px; margin: 0 0 4px 0; font-weight: 700; }
      .hero p  { color:#e9eef0; margin: 0; font-size: 14px; }
      .kpi {
        background:#fff; border-radius:16px; padding:16px 18px; height:100%;
        border:1px solid #eceef1; box-shadow:0 2px 10px rgba(27,49,57,.05);
      }
      .kpi .lbl { color:#5b6770; font-size:12px; text-transform:uppercase;
                  letter-spacing:.4px; margin-bottom:6px; }
      .kpi .val { color:#1B3139; font-size:26px; font-weight:700; line-height:1.1; }
      .kpi .sub { color:#8a9499; font-size:11.5px; margin-top:4px; }
      .kpi.alert .val { color:#FF3621; }
      .sec { font-size:18px; font-weight:700; color:#1B3139; margin: 6px 0 8px 0; }
      .veredicto {
        border-radius:16px; padding:24px 26px; text-align:center; color:#fff;
        box-shadow:0 6px 20px rgba(27,49,57,.15);
      }
      .veredicto .prob { font-size:48px; font-weight:800; line-height:1; margin:6px 0; }
      .veredicto .lbl  { font-size:13px; text-transform:uppercase; letter-spacing:.6px; opacity:.9; }
      .veredicto .tag  { font-size:18px; font-weight:700; margin-top:8px; }
      .rojo    { background: linear-gradient(135deg,#c81e0a,#FF3621); }
      .amarillo{ background: linear-gradient(135deg,#b8860b,#f0a500); }
      .verde   { background: linear-gradient(135deg,#00754a,#00A972); }
      .placeholder {
        background:#fff; border:1px dashed #cfd6da; border-radius:16px;
        padding:34px 26px; text-align:center; color:#8a9499;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

_cfg = Config()  # host + credenciales del service principal de la App


# ── Model Serving endpoint (scorer) ──
def llamar_endpoint(fila: dict) -> float:
    url = f"{_cfg.host}/serving-endpoints/{ENDPOINT}/invocations"
    payload = {"dataframe_split": {"columns": list(fila.keys()), "data": [list(fila.values())]}}
    headers = _cfg.authenticate()
    headers["Content-Type"] = "application/json"
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    pred = r.json()["predictions"][0]
    if isinstance(pred, dict):
        pred = pred.get("1", list(pred.values())[-1])
    return max(0.0, min(1.0, float(pred)))


# ── SQL warehouse (KPIs) ──
@st.cache_data(ttl=300)
def consultar(query: str) -> pd.DataFrame:
    with sql.connect(server_hostname=_cfg.host,
                     http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                     credentials_provider=lambda: _cfg.authenticate) as c:
        with c.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()


# ── Hero ──
st.markdown(f"""<div class="hero"><h1>🛡️ {TITULO}</h1><p>{BIENVENIDA}</p></div>""",
            unsafe_allow_html=True)

if not ENDPOINT:
    st.error("⚠️ Falta configurar `Z2H_ENDPOINT` en app.yaml (nombre del serving endpoint).")
    st.stop()


def kpi(col, label, value, sub="", tone=""):
    col.markdown(f"""<div class="kpi {tone}"><div class="lbl">{label}</div>
                 <div class="val">{value}</div><div class="sub">{sub}</div></div>""",
                 unsafe_allow_html=True)


# ═══ PARTE 1 · Panorama del fraude (KPIs + gráficos) ═══
FQN = f"`{CATALOGO}`.`{SCHEMA}`.gold_riesgo_diario"
hay_kpis = bool(CATALOGO and SCHEMA and WAREHOUSE_ID)

if hay_kpis:
    try:
        tot = consultar(f"""
            SELECT SUM(total_transacciones) tx, SUM(transacciones_fraude) fr,
                   SUM(monto_fraude) mf,
                   SUM(transacciones_fraude)/SUM(total_transacciones) tasa
            FROM {FQN}
        """)
        tx = float(tot["tx"].iloc[0] or 0)
        fr = float(tot["fr"].iloc[0] or 0)
        mf = float(tot["mf"].iloc[0] or 0)
        tasa = float(tot["tasa"].iloc[0] or 0)

        st.markdown('<div class="sec">📊 Panorama del fraude</div>', unsafe_allow_html=True)
        k1, k2, k3, k4 = st.columns(4)
        kpi(k1, "Transacciones", f"{tx:,.0f}", "total analizadas")
        kpi(k2, "Fraudes detectados", f"{fr:,.0f}", "marcados como fraude", tone="alert")
        kpi(k3, "Tasa de fraude", f"{tasa:.2%}", "sobre el total", tone="alert")
        kpi(k4, "Monto en fraude", f"${mf:,.0f}", "COP acumulado", tone="alert")

        g1, g2 = st.columns(2)
        with g1:
            st.markdown('<div class="sec">Fraude por categoría</div>', unsafe_allow_html=True)
            porcat = consultar(f"""
                SELECT categoria_comercio AS categoria, SUM(transacciones_fraude) AS fraudes
                FROM {FQN} GROUP BY categoria_comercio ORDER BY fraudes DESC LIMIT 10
            """).set_index("categoria")
            st.bar_chart(porcat, height=240, color="#FF3621")
        with g2:
            st.markdown('<div class="sec">Monto en fraude por día</div>', unsafe_allow_html=True)
            pordia = consultar(f"""
                SELECT dia, SUM(monto_fraude) AS monto_fraude
                FROM {FQN} GROUP BY dia ORDER BY dia
            """).set_index("dia")
            st.line_chart(pordia, height=240, color="#FF3621")
        st.divider()
    except Exception as e:
        st.info(f"ℹ️ El panorama de KPIs no está disponible (revisa Z2H_CATALOGO/SCHEMA/"
                f"WAREHOUSE_ID y permisos del warehouse). La calificación sí funciona.\n\n{str(e)[:150]}")
else:
    st.info("ℹ️ Configura `Z2H_CATALOGO`, `Z2H_SCHEMA` y `Z2H_WAREHOUSE_ID` en app.yaml para ver "
            "los KPIs del panorama. La calificación de transacciones ya funciona sin eso.")

# ═══ PARTE 2 · Calificar una transacción (scorer) ═══
st.markdown('<div class="sec">🔎 Calificar una transacción</div>', unsafe_allow_html=True)
izq, der = st.columns([1.1, 1], gap="large")

with izq:
    c1, c2 = st.columns(2)
    with c1:
        monto = st.number_input("Monto (COP)", min_value=0.0, value=150000.0, step=10000.0)
        categoria = st.selectbox(
            "Categoría de comercio",
            ["supermercado", "restaurante", "combustible", "electronica", "viajes",
             "salud", "ropa", "entretenimiento", "servicios", "hogar"])
        canal = st.selectbox("Canal", ["presencial", "online", "movil", "atm"])
        pais = st.selectbox("País", ["CO", "US", "MX", "PA", "ES"])
    with c2:
        hora = st.slider("Hora del día", 0, 23, 14)
        segmento = st.selectbox("Segmento del cliente", ["retail", "premium", "empresarial"])
        antiguedad = st.number_input("Antigüedad del cliente (meses)", min_value=0, value=36)
        score = st.number_input("Score crediticio", min_value=300, max_value=850, value=650)
    calcular = st.button("🔎 Calcular riesgo de fraude", type="primary", use_container_width=True)

with der:
    marcador = st.empty()
    marcador.markdown(
        '<div class="placeholder">Completa los datos y presiona <b>Calcular</b>.<br>'
        'La App consultará tu <b>serving endpoint</b>.</div>', unsafe_allow_html=True)

if calcular:
    try:
        promedios = {"supermercado": 148000, "restaurante": 54000, "combustible": 132000,
                     "electronica": 920000, "viajes": 1840000, "salud": 97000, "ropa": 285000,
                     "entretenimiento": 41000, "servicios": 88000, "hogar": 610000}
        desv = monto / promedios.get(categoria, 150000)
        fila = {"monto": float(monto), "desviacion_monto": float(desv), "hora": float(hora),
                "es_exterior": 0.0 if pais == "CO" else 1.0,
                "categoria_comercio": categoria, "canal": canal, "segmento": segmento,
                "antiguedad_meses": float(antiguedad), "score_crediticio": float(score)}
        prob = llamar_endpoint(fila)
        if prob >= UMBRAL_ROJO:
            tono, tag = "rojo", "🔴 ALTO riesgo — revisar antes de aprobar"
        elif prob >= UMBRAL_AMARILLO:
            tono, tag = "amarillo", "🟡 Riesgo medio — verificar"
        else:
            tono, tag = "verde", "🟢 Riesgo bajo — aprobar"
        marcador.markdown(
            f"""<div class="veredicto {tono}"><div class="lbl">Riesgo de fraude</div>
                <div class="prob">{prob:.0%}</div><div class="tag">{tag}</div></div>""",
            unsafe_allow_html=True)
        der.progress(prob)
        der.caption(f"Calculado por tu endpoint `{ENDPOINT}` (modelo del Módulo 6).")
    except Exception as e:
        marcador.error(f"No se pudo consultar el endpoint `{ENDPOINT}`:\n\n{str(e)[:250]}\n\n"
                       "💡 Revisa que el endpoint esté READY y que el service principal de la App "
                       "tenga **Can Query** (Paso C1.5).")

st.divider()
st.caption("Material de aprendizaje · datos y modelo 100% sintéticos · From Zero to Hero 🚀")
