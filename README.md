# From Zero to Hero · Databricks LATAM

Material **hands-on** para el taller *From Zero to Hero*: construir un proyecto de datos e IA de
punta a punta en Databricks — ingesta, transformación, gobierno, orquestación, machine
learning / AI Functions, y consumo (dashboards, Genie y apps).

Los notebooks son **didácticos y autoexplicativos**: cada uno enseña un concepto con un caso de
negocio realista, en español, pensado para personas **sin experiencia previa** en Databricks.

> ⚠️ **Material de aprendizaje — no es production-ready.** Todos los datos son **100%
> sintéticos**; no hay información real de ninguna persona ni empresa. Los notebooks enseñan
> conceptos y no están pensados para copiarse a producción tal cual (les falta manejo de
> errores, pruebas, control de costos y CI/CD). Los conceptos —Delta Lake, Unity Catalog,
> Lakeflow, MLflow, AI Functions— sí son exactamente los que se usan en producción.

## Casos de uso

| Carpeta | Caso | Estado |
|---|---|---|
| [`verificacion-ambiente/`](./verificacion-ambiente) | **Módulo 0** — verifica que tu workspace está listo (corre esto primero) | ✅ |
| [`retail/`](./retail) | **Retail — pronóstico de ventas** con AI Functions (`ai_forecast`) | ✅ Completo y validado |
| `fraud-detection/` | Financiero — detección de fraude con MLflow | 🔜 Próximamente |

> 👉 **Empieza por [`verificacion-ambiente/`](./verificacion-ambiente)** para confirmar permisos
> antes del taller. Luego elige un caso (por ahora, `retail/`).

## Ruta de los módulos (retail)

| Módulo | Tema | Qué construyes |
|---|---|---|
| **1** | Lakehouse 101 | Tu primera tabla Delta + time travel |
| **2** | Ingesta | Capa **bronze** con Auto Loader (incremental) |
| **3** | Pipeline | **silver** y **gold** con Lakeflow Declarative Pipelines + calidad |
| **4** | Gobierno | Tags, row filter, column masking, lineage (Unity Catalog) |
| **5** | Orquestación | Un **job** programado que encadena todo |
| **6** | AI Functions | **Pronóstico de ventas** con `ai_forecast` (solo SQL) |
| **7** | Consumo | **Dashboard** + **Genie** + una **App** de reabastecimiento |

Cada módulo empieza con `00_generar_datos` (Módulo 1) que crea el dataset sintético. Córrelo una
sola vez y sigue los notebooks en orden.

## Cómo usarlo

1. Importa la carpeta del caso (`retail/`) a tu workspace de Databricks.
2. Abre `retail/modulo-01/notebooks/00_generar_datos` y ejecútalo (crea catálogo, schema, volumen
   y los datos sintéticos).
3. Sigue los módulos 1 → 7 en orden. Cada notebook indica de dónde viene y a dónde va.

### Requisitos
- Un workspace de Databricks con **Unity Catalog** (no funciona en Free Edition para el Módulo 4).
- **Serverless** o un cluster; el Módulo 6 (`ai_forecast`) requiere un **SQL warehouse Pro o
  Serverless**. Cada notebook detalla sus requisitos.

---

*From Zero to Hero — Databricks LATAM.* Datos 100% sintéticos · material educativo.
