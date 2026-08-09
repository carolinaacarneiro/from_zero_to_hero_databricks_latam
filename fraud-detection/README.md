# Detección de fraude (financiero)

Caso de uso **financiero** para el taller *From Zero to Hero*: un banco que necesita **detectar
transacciones fraudulentas** antes de que el dinero se pierda.

Construyes el camino completo: de archivos crudos del sistema de pagos a un **modelo de ML**
(MLflow) que califica cada transacción, **servido como endpoint**, y una **app** que lo consulta
en vivo — además de un **dashboard** y un **Genie** que responde preguntas.

> ⚠️ Material de aprendizaje. Datos **100% sintéticos** — ninguna persona ni institución real.
> Los nombres de comercios son inventados.

## Orden de los notebooks

| # | Notebook | Qué hace |
|---|---|---|
| 1 | `modulo-01/notebooks/00_generar_datos` | Crea catálogo, schema, volumen y el dataset sintético (transacciones, clientes). **Córrelo primero.** |
| 1 | `modulo-01/notebooks/01_lakehouse_101` | Primera tabla Delta, time travel |
| 2 | `modulo-02/notebooks/02_ingesta` | Capa bronze con Auto Loader |
| 3 | `modulo-03/notebooks/03_guia_pipeline` | Guía para crear el pipeline (silver + gold) |
| 3 | `modulo-03/notebooks/pipeline_fraude` | El pipeline (con TODOs) |
| 3 | `modulo-03/notebooks/pipeline_fraude_RESPUESTA` | El pipeline resuelto (para quien prefiere no escribir el código) |
| 4 | `modulo-04/notebooks/04_gobierno` | Tags/PII, row filter, column masking, lineage |
| 5 | `modulo-05/notebooks/05_orquestacion` | Job de 3 tareas |
| 5 | `modulo-05/notebooks/validacion` | Tarea de validación del job |
| 6 | `modulo-06/notebooks/06_ml` | Entrena un modelo (MLflow), lo registra con alias `@champion` y lo **sirve como endpoint** |
| 7 | `modulo-07/notebooks/07_consumo` | Dashboard + Genie + App |
| 7 | `modulo-07/notebooks/app/` | Código de la Databricks App (Streamlit) que consulta el serving endpoint |
| 7 | `modulo-07/genie_instructions.md` | Guía para el facilitador (bloque Genie) |

## Notas
- El **Módulo 6** entrena con **MLflow + scikit-learn** (mide AUC, no accuracy, porque el fraude
  es ~1,5%) y **hospeda el modelo como un Serving Endpoint** (Paso 7, por la UI).
- La **App del Módulo 7** consulta ese endpoint por REST — así no depende de las versiones de
  librerías del modelo. Necesita permiso **Can Query** sobre el endpoint (ver Paso C1.5).
- El **Módulo 4** requiere Unity Catalog (no funciona en Free Edition).
- Todo lo que crees vive en tu propio schema `fin_<tu_usuario>`.
