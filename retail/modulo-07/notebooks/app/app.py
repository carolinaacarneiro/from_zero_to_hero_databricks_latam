"""
Databricks App · asistente_reabastecimiento
From Zero to Hero (retail) — Módulo 7

App de ejemplo (Streamlit) que recomienda QUÉ PRODUCTOS REABASTECER, cruzando el estado de
inventario (gold_inventario_estado) con el pronóstico de ventas (gold_pronostico_pais) que el
participante generó en el Módulo 6.

Material de aprendizaje: datos 100% sintéticos.

Los participantes personalizan tres cosas marcadas con TODO (título, texto, umbral de urgencia).

Configuración (variables de entorno, en app.yaml):
  Z2H_CATALOGO      → catálogo del participante
  Z2H_SCHEMA        → schema del participante (retail_<usuario>)
  Z2H_WAREHOUSE_ID  → ID de un SQL warehouse
"""

import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# ─────────────────────────────────────────────────────────────────────────────
# 📝 TODO 1 · cambia el título de tu App
TITULO = "Asistente de reabastecimiento"
# 📝 TODO 2 · cambia el texto de bienvenida
BIENVENIDA = "Detecta qué productos hay que reabastecer antes de quedarte sin stock."
# 📝 TODO 3 · a partir de cuántos DÍAS DE COBERTURA se marca en rojo (urgente)
DIAS_COBERTURA_ROJO = 7
DIAS_COBERTURA_AMARILLO = 15
# ─────────────────────────────────────────────────────────────────────────────

CATALOGO = os.environ.get("Z2H_CATALOGO", "").strip()
SCHEMA = os.environ.get("Z2H_SCHEMA", "").strip()
WAREHOUSE_ID = os.environ.get("Z2H_WAREHOUSE_ID", "").strip()

st.set_page_config(page_title=TITULO, page_icon="📦", layout="wide")

# ─────────────────────────────────────────────────────────────────────────────
# Estilo — paleta inspirada en Databricks (lava/orange sobre fondo claro)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
      .stApp { background: #f7f8fa; }
      #MainMenu, footer { visibility: hidden; }
      /* Hero */
      .hero {
        background: linear-gradient(120deg, #1B3139 0%, #FF3621 140%);
        padding: 28px 34px; border-radius: 18px; color: #fff; margin-bottom: 22px;
        box-shadow: 0 8px 24px rgba(27,49,57,.18);
      }
      .hero h1 { color:#fff; font-size: 30px; margin: 0 0 6px 0; font-weight: 700; }
      .hero p  { color:#e9eef0; margin: 0; font-size: 15px; }
      /* KPI cards */
      .kpi {
        background:#fff; border-radius:16px; padding:18px 20px; height:100%;
        border:1px solid #eceef1; box-shadow:0 2px 10px rgba(27,49,57,.05);
      }
      .kpi .lbl { color:#5b6770; font-size:12.5px; text-transform:uppercase;
                  letter-spacing:.4px; margin-bottom:6px; }
      .kpi .val { color:#1B3139; font-size:30px; font-weight:700; line-height:1.1; }
      .kpi .sub { color:#8a9499; font-size:12px; margin-top:4px; }
      .kpi.alert .val { color:#FF3621; }
      .kpi.ok .val { color:#00A972; }
      /* Section titles */
      .sec { font-size:19px; font-weight:700; color:#1B3139; margin: 8px 0 2px 0; }
      .badge {
        display:inline-block; padding:3px 10px; border-radius:999px;
        font-size:12px; font-weight:600; margin-left:8px;
      }
      .badge-red { background:#ffe5e1; color:#c81e0a; }
      .badge-green { background:#d9f5ea; color:#00754a; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _conn():
    """Conexión al SQL warehouse usando las credenciales del service principal de la App."""
    cfg = Config()  # toma host y credenciales del entorno de la App
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=300)
def consultar(query: str) -> pd.DataFrame:
    with _conn() as c:
        with c.cursor() as cur:
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()


# ── Hero header ──
st.markdown(
    f"""
    <div class="hero">
      <h1>📦 {TITULO}</h1>
      <p>{BIENVENIDA}</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if not (CATALOGO and SCHEMA and WAREHOUSE_ID):
    st.error("⚠️ Falta configurar `Z2H_CATALOGO`, `Z2H_SCHEMA` y `Z2H_WAREHOUSE_ID` en app.yaml.")
    st.stop()

FQN_INV = f"`{CATALOGO}`.`{SCHEMA}`.gold_inventario_estado"
FQN_PRON = f"`{CATALOGO}`.`{SCHEMA}`.gold_pronostico_pais"

# ── filtros en la barra lateral ──
try:
    paises = consultar(f"SELECT DISTINCT pais FROM {FQN_INV} ORDER BY pais")["pais"].tolist()
    cats = consultar(f"SELECT DISTINCT categoria FROM {FQN_INV} ORDER BY categoria")["categoria"].tolist()
except Exception as e:
    st.error(f"No pude leer las tablas gold. ¿Corriste los módulos 3 y 6, y le diste permisos "
             f"al service principal de la App (Paso C1.5)? Detalle: {e}")
    st.stop()

with st.sidebar:
    st.markdown("### 🎛️ Filtros")
    pais = st.selectbox("🌎 País", paises)
    categoria = st.selectbox("🏷️ Categoría", ["(todas)"] + cats)
    st.markdown("---")
    st.caption("Los datos vienen de tus tablas **gold** (Módulo 3) y del **pronóstico** "
               "que generaste en el Módulo 6.")

filtro_cat = "" if categoria == "(todas)" else f"AND categoria = '{categoria}'"
sufijo_cat = "" if categoria == "(todas)" else f" · {categoria}"

# ── datos ──
pron = consultar(f"""
    SELECT ROUND(SUM(monto_pronostico), 0) AS monto, ROUND(SUM(unidades_pronostico), 0) AS unidades
    FROM {FQN_PRON} WHERE pais = '{pais}'
""")
monto_pron = float(pron["monto"].iloc[0] or 0)
unid_pron = float(pron["unidades"].iloc[0] or 0)

resumen = consultar(f"""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN necesita_reabastecer THEN 1 ELSE 0 END) AS a_reabastecer,
        SUM(CASE WHEN necesita_reabastecer AND dias_cobertura < {DIAS_COBERTURA_ROJO}
                 THEN 1 ELSE 0 END) AS urgentes
    FROM {FQN_INV} WHERE pais = '{pais}' {filtro_cat}
""")
total_prod = int(resumen["total"].iloc[0] or 0)
n_reab = int(resumen["a_reabastecer"].iloc[0] or 0)
n_urg = int(resumen["urgentes"].iloc[0] or 0)


def kpi(col, label, value, sub="", tone=""):
    col.markdown(
        f"""<div class="kpi {tone}">
              <div class="lbl">{label}</div>
              <div class="val">{value}</div>
              <div class="sub">{sub}</div>
            </div>""",
        unsafe_allow_html=True,
    )


# ── KPIs ──
c1, c2, c3, c4 = st.columns(4)
kpi(c1, "Venta pronosticada · 90 días", f"${monto_pron:,.0f}", f"{pais}")
kpi(c2, "Unidades pronosticadas", f"{unid_pron:,.0f}", "próximos 90 días")
kpi(c3, "Productos a reabastecer", f"{n_reab}", f"de {total_prod} en {pais}{sufijo_cat}",
    tone="alert" if n_reab else "ok")
kpi(c4, "🔴 Urgentes", f"{n_urg}", f"cobertura < {DIAS_COBERTURA_ROJO} días",
    tone="alert" if n_urg else "ok")

st.write("")

# ── gráfico del pronóstico (serie diaria del país) ──
serie = consultar(f"""
    SELECT dia, monto_pronostico
    FROM {FQN_PRON} WHERE pais = '{pais}' ORDER BY dia
""")
if not serie.empty:
    st.markdown(f'<div class="sec">📈 Pronóstico de ventas diario · {pais}</div>',
                unsafe_allow_html=True)
    serie = serie.set_index("dia")
    st.line_chart(serie, height=240, color="#FF3621")

# ── tabla de reabastecimiento ──
inv = consultar(f"""
    SELECT nombre_producto, categoria, stock_actual, stock_minimo,
           demanda_diaria, dias_cobertura, lead_time_dias
    FROM {FQN_INV}
    WHERE pais = '{pais}' AND necesita_reabastecer {filtro_cat}
    ORDER BY dias_cobertura ASC NULLS FIRST
""")

badge = (f'<span class="badge badge-red">{len(inv)} por reabastecer</span>' if not inv.empty
         else '<span class="badge badge-green">todo en orden</span>')
st.markdown(f'<div class="sec">🛒 Reabastecimiento en {pais}{sufijo_cat}{badge}</div>',
            unsafe_allow_html=True)

if inv.empty:
    st.success("🟢 No hay productos que requieran reabastecimiento con estos filtros.")
else:
    def semaforo(d):
        if d is None or d < DIAS_COBERTURA_ROJO:
            return "🔴 Urgente"
        if d < DIAS_COBERTURA_AMARILLO:
            return "🟡 Pronto"
        return "🟢 Vigilar"

    inv.insert(0, "Estado", inv["dias_cobertura"].apply(semaforo))
    if n_urg:
        st.error(f"🔴 {n_urg} producto(s) urgente(s): el stock no cubre el tiempo de reposición "
                 f"({DIAS_COBERTURA_ROJO} días). Prioriza la compra de estos.")

    tope_stock = float(max(inv["stock_actual"].max(), inv["stock_minimo"].max(), 1))
    tope_dem = float(max(inv["demanda_diaria"].max(), 1))
    st.dataframe(
        inv, use_container_width=True, hide_index=True,
        column_config={
            "Estado": st.column_config.TextColumn("Estado", width="small"),
            "nombre_producto": st.column_config.TextColumn("Producto", width="medium"),
            "categoria": st.column_config.TextColumn("Categoría", width="small"),
            "stock_actual": st.column_config.ProgressColumn(
                "Stock actual", format="%d", min_value=0, max_value=tope_stock),
            "stock_minimo": st.column_config.NumberColumn("Stock mínimo", format="%d"),
            "demanda_diaria": st.column_config.ProgressColumn(
                "Demanda/día", format="%.1f", min_value=0, max_value=tope_dem),
            "dias_cobertura": st.column_config.NumberColumn("Días de cobertura", format="%.1f d"),
            "lead_time_dias": st.column_config.NumberColumn("Lead time", format="%d d"),
        },
    )
    st.caption("💡 «Días de cobertura» = cuánto dura el stock actual al ritmo de venta reciente. "
               "Si es menor que el *lead time* (lo que tarda en llegar el pedido), te quedas sin "
               "producto antes de que llegue la reposición.")

st.divider()
st.caption("Material de aprendizaje · datos 100% sintéticos · From Zero to Hero 🚀")
