# Módulo 7 · Genie — guía para TAs

Este documento tiene lo que los TAs necesitan para el Bloque B (Genie): las 3 preguntas, qué
se espera de cada una **antes y después** de la instruction, y la instruction correcta.

> El objetivo pedagógico del bloque es que Genie **falle a propósito** en la pregunta 3, el
> participante diagnostique por qué, y lo corrija. Si Genie acierta la 3 por casualidad antes
> de la instruction, el ejercicio pierde su gracia — ver "Si Genie acierta de más" abajo.

---

## Las 3 preguntas y su comportamiento esperado

| # | Pregunta (en español) | Antes de la instruction | Después |
|---|---|---|---|
| 1 | *¿Cuál fue la tasa de fraude promedio?* | ✅ Responde bien — `tasa_fraude` existe en gold | ✅ igual |
| 2 | *¿Qué categoría de comercio tiene más fraude?* | ✅ Responde bien — agrupa por `categoria_comercio` | ✅ igual |
| 3 | *¿Cuántas transacciones sospechosas hubo esta semana?* | ❌ **Falla o inventa** — "sospechosa" no está en el esquema | ✅ Responde con la definición dada |

**Por qué falla la 3:** no hay ninguna columna `sospechosa` ni nada que la defina. Genie
tiene dos salidas malas: decir que no puede, o **inventar** una interpretación (p. ej. contar
todas las transacciones, o filtrar por algo arbitrario). Ambas son el punto: Genie no adivina
definiciones de negocio.

---

## La instruction correcta

En el espacio Genie → **Instructions**, agregar:

> Una transacción sospechosa es aquella cuyo monto supera 3 veces el promedio histórico de su
> categoría de comercio, o cuyo país es distinto de CO.

Y una **consulta de ejemplo** (Example SQL) que ancle la definición:

```sql
-- Transacciones sospechosas: monto muy alto para su categoría, o desde el exterior
WITH prom AS (
  SELECT categoria_comercio, AVG(monto) AS prom_cat
  FROM silver_transacciones GROUP BY categoria_comercio
)
SELECT COUNT(*) AS transacciones_sospechosas
FROM silver_transacciones s
JOIN prom p USING (categoria_comercio)
WHERE s.monto > 3 * p.prom_cat OR s.pais <> 'CO'
```

Después de guardar la instruction, **volver a hacer la pregunta 3**. Ahora Genie tiene una
definición y una plantilla, y responde de forma consistente.

---

## Comentarios de columna (paso B5)

El notebook agrega comentarios a `tasa_fraude` y `ticket_promedio`. Genie los usa para
desambiguar. Si un participante pregunta *"¿cuál es el ticket promedio más alto?"*, con el
comentario Genie entiende que `ticket_promedio` es el monto promedio por transacción.

---

## Si Genie acierta de más (la 3 no falla)

Puede pasar que Genie, sin la instruction, responda algo plausible a la pregunta 3. Si eso
ocurre en el ensayo, **endurecer la trampa** con una de estas variantes, que dependen aún más
de una definición de negocio:

- *¿Cuántas transacciones de alto riesgo hubo?*
- *¿Cuál fue el monto en riesgo el mes pasado?*
- *¿Qué clientes tuvieron actividad anómala?*

Ninguna de ellas se puede responder sin una definición previa de "alto riesgo" / "en riesgo" /
"anómala". Elegir la que falle de forma más reproducible en el workspace del evento.

---

## Si Genie no está disponible

Genie requiere un SQL warehouse y puede no estar habilitado en todos los workspaces (revisar
en la verificación de entorno del M0). Plan B: el facilitador hace el bloque como **demo** en
su propio espacio, y los participantes observan el fallo y la corrección. Se conserva la
lección; se pierde el hands-on.
