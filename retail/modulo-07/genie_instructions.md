# Módulo 7 (retail) · Genie — guía para TAs

Este documento tiene lo que los TAs necesitan para el Bloque B (Genie): las 3 preguntas, qué se
espera de cada una **antes y después** de la instruction, y la instruction correcta.

> El objetivo pedagógico del bloque es que Genie **falle a propósito** en la pregunta 3, el
> participante diagnostique por qué, y lo corrija. Si Genie acierta la 3 por casualidad antes de
> la instruction, el ejercicio pierde su gracia — ver "Si Genie acierta de más" abajo.

Tablas del espacio Genie: `gold_ventas_diarias`, `gold_ventas_producto`, `gold_pronostico_pais`.

---

## Las 3 preguntas y su comportamiento esperado

| # | Pregunta (en español) | Antes de la instruction | Después |
|---|---|---|---|
| 1 | *¿Cuál fue el monto total de ventas por país?* | ✅ Responde bien — `monto` y `pais` existen | ✅ igual |
| 2 | *¿Qué categoría vendió más el último trimestre?* | ✅ Responde bien — agrupa por `categoria` | ✅ igual |
| 3 | *¿Cuáles son mis productos estrella?* | ❌ **Falla o inventa** — "estrella" no está en el esquema | ✅ Responde con la definición dada |

**Por qué falla la 3:** no hay ninguna columna `estrella` ni nada que la defina. Genie tiene dos
salidas malas: decir que no puede, o **inventar** una interpretación (p. ej. ordenar por monto sin
más). Ambas son el punto: Genie no adivina definiciones de negocio.

---

## La instruction correcta

En el espacio Genie → **Instructions**, agregar:

> Un producto estrella es aquel que está entre el 10% de mayor monto_total y además tiene un
> margen_pct por encima de 0.35. Usa la tabla gold_ventas_producto.

Y una **consulta de ejemplo** (Example SQL) que ancle la definición:

```sql
SELECT nombre_producto, monto_total, margen_pct
FROM gold_ventas_producto
WHERE margen_pct > 0.35
ORDER BY monto_total DESC
LIMIT 20
```

Después de guardar la instruction, **volver a hacer la pregunta 3**. Ahora Genie tiene una
definición y una plantilla, y responde de forma consistente.

---

## Comentarios de columna (paso B5)

El notebook agrega comentarios a `margen_pct` y `monto_pronostico`. Genie los usa para
desambiguar. Si un participante pregunta *"¿qué producto es más rentable?"*, con el comentario
Genie entiende que `margen_pct` mide rentabilidad.

---

## Si Genie acierta de más (la 3 no falla)

Si Genie, sin la instruction, responde algo plausible a la 3, **endurecer la trampa** con una
variante que dependa más de una definición de negocio:

- *¿Qué productos debería descontinuar?*
- *¿Cuáles son mis productos de bajo rendimiento?*
- *¿Qué productos están en riesgo de quiebre de stock?*

Ninguna se puede responder sin una definición previa. Elegir la que falle de forma más
reproducible en el workspace del evento.

---

## Si Genie no está disponible

Genie requiere un SQL warehouse y puede no estar habilitado en todos los workspaces (revisar en la
verificación de entorno del M0). Plan B: el facilitador hace el bloque como **demo** en su propio
espacio, y los participantes observan el fallo y la corrección. Se conserva la lección; se pierde
el hands-on.
