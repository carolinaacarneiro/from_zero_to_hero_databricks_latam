# Retail · Pronóstico de ventas

Caso de uso de **retail** para el taller *From Zero to Hero*: una cadena con tiendas en varios
países de América Latina que quiere **pronosticar sus ventas** para planear el inventario.

Construyes el camino completo: de archivos crudos del punto de venta a un **pronóstico con AI
Functions**, un **dashboard**, un **Genie** que responde preguntas y una **app** que recomienda
qué reabastecer.

> ⚠️ Material de aprendizaje. Datos **100% sintéticos** — ninguna empresa, marca ni persona real.

## Orden de los notebooks

| # | Notebook | Qué hace |
|---|---|---|
| 1 | `modulo-01/notebooks/00_generar_datos` | Crea catálogo, schema, volumen y el dataset sintético (24 meses de ventas, productos, inventario). **Córrelo primero.** |
| 1 | `modulo-01/notebooks/01_lakehouse_101` | Primera tabla Delta, time travel |
| 2 | `modulo-02/notebooks/02_ingesta` | Capa bronze con Auto Loader |
| 3 | `modulo-03/notebooks/03_guia_pipeline` | Guía para crear el pipeline (silver + gold) |
| 3 | `modulo-03/notebooks/pipeline_retail` | El pipeline (con TODOs) |
| 3 | `modulo-03/notebooks/pipeline_retail_RESPUESTA` | El pipeline resuelto (para quien prefiere no escribir el código) |
| 4 | `modulo-04/notebooks/04_gobierno` | Tags, row filter, column masking, lineage |
| 5 | `modulo-05/notebooks/05_orquestacion` | Job de 3 tareas |
| 5 | `modulo-05/notebooks/validacion` | Tarea de validación del job |
| 6 | `modulo-06/notebooks/06_ml` | Pronóstico con `ai_forecast` (+ Plan B sin preview) |
| 7 | `modulo-07/notebooks/07_consumo` | Dashboard + Genie + App |
| 7 | `modulo-07/notebooks/app/` | Código de la Databricks App (Streamlit) |
| 7 | `modulo-07/genie_instructions.md` | Guía para el facilitador (bloque Genie) |

## Notas
- El **Módulo 6** usa `ai_forecast` (versión 1). Si el workspace tiene la función deshabilitada,
  el notebook trae un **Plan B en SQL puro** que produce el mismo resultado.
- El **Módulo 7** incluye los `GRANT` que necesita el service principal de la App (Paso C1.5).
- Todo lo que crees vive en tu propio schema `retail_<tu_usuario>`.
