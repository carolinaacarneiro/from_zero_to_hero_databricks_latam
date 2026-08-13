# Notebooks del Módulo 1 — Caso: detección de fraude

## Los dos notebooks

| Notebook | Qué hace | Cuándo se corre | Duración |
|---|---|---|---|
| **`00_generar_datos.py`** | Genera los datos sintéticos del caso de fraude | **Una vez**, antes del Módulo 1 | 2–3 min |
| **`01_lakehouse_101.py`** | El ejercicio del Módulo 1: primera tabla Delta + time travel | En el Módulo 1 | 16 min |

Se suman al `00_verificar_ambiente.py` que ya está en `../notebooks/` (Módulo 0).

**Orden de ejecución en el día:**
```
00_verificar_ambiente   → Módulo 0, verifica permisos
00_generar_datos        → antes del Módulo 1, crea los datos
01_lakehouse_101        → Módulo 1, el ejercicio
```

---

## 🔴 Cambio de arquitectura respecto al plan original

`05-capstones.md` y `06-plan-modulos-financiero.md` asumen un **catálogo compartido de solo
lectura** llamado `z2h_shared`, con los datos crudos en
`/Volumes/z2h_shared/raw/financiero/`.

**Eso no funciona.** Cada participante trabaja en **su propio workspace** (el suyo, un trial
o Free Edition), así que no existe ningún catálogo compartido entre todos.

**La solución aplicada:** cada participante **genera sus propios datos** en su propio schema.

| Antes (no viable) | Ahora |
|---|---|
| `/Volumes/z2h_shared/raw/financiero/` | `/Volumes/<su catálogo>/fin_<usuario>/raw/` |
| Datos pre-cargados por nosotros | Cada uno corre `00_generar_datos` |
| Modelo pre-entrenado en `z2h_shared` | *Pendiente de resolver para el M6* |

**Ventajas inesperadas de este cambio:**
- Cada uno es dueño de sus datos: puede romperlos y regenerarlos sin afectar a nadie
- No hay que aprovisionar nada por adelantado
- El generador usa **semilla fija**, así que todos obtienen los mismos datos y los números
  de la sala coinciden
- Funciona igual en Free Edition, trial y workspace corporativo

⚠️ **Consecuencia pendiente:** la "celda de recuperación" de cada módulo — que reconstruía el
estado del módulo anterior desde `z2h_shared` — hay que rediseñarla. La alternativa es que
cada notebook reconstruya lo que necesita desde los archivos crudos del propio participante.

---

## `00_generar_datos.py` — el generador

### Qué crea

| Ruta | Contenido | Formato |
|---|---|---|
| `raw/transacciones/` | **12 archivos**, ~377.000 transacciones | JSON |
| `raw/transacciones_nuevas/` | **1 archivo**, ~20.000 transacciones | JSON |
| `raw/clientes/` | ~18.500 clientes | CSV |

Los datos se escriben como **archivos, no como tablas**. Es deliberado: así el Módulo 2
empieza como empieza en la vida real, con archivos que llegan de un sistema origen.

### El esquema

**`transacciones`** — `transaccion_id` · `cliente_id` · **`numero_tarjeta`** *(PII)* ·
`monto` · `moneda` · `comercio` · `categoria_comercio` · `canal` · `pais` · **`region`** ·
`fecha` · **`es_fraude`** *(etiqueta)* · `dispositivo_id`

**`clientes`** — `cliente_id` · `segmento` · `antiguedad_meses` · `region` · `edad` ·
`score_crediticio`

### Lo que está plantado a propósito

| Qué | Para qué módulo |
|---|---|
| **Señales de fraude correlacionadas** (monto, país, horario, canal, antigüedad, score) | **M6** — sin esto el modelo no aprende nada |
| **~1,5% de fraude** | **M6** — desbalance realista; permite enseñar por qué *accuracy* engaña |
| **~2% de filas con `monto <= 0`** | **M3** — la regla de calidad tiene que descartar algo real |
| **`region` con 3 valores** (`norte`/`centro`/`sur`) | **M4** — el row filter tiene que recortar visiblemente |
| **`numero_tarjeta`** | **M4** — el objetivo del column masking |
| **`transacciones_nuevas` aparte** | **M2** — demostrar que Auto Loader no reprocesa |
| **`dispositivo_id` nulo según canal** (96% presente en digital, 18% en presencial) | **M3** — manejo de nulos con lógica, no al azar |

### Control de calidad incluido

El notebook imprime la tasa de fraude por canal, por país y el monto promedio normal vs
fraude. **Si esos números no muestran diferencias claras, el generador está mal** y el M6 va
a fracasar. Es la verificación más importante del notebook.

Objetivo del M6: **AUC alcanzable de ~0,85–0,92** — bueno pero no perfecto.

---

## 🎲 Realismo de los datos — auditoría y correcciones

Los datos sintéticos mal hechos **se notan a simple vista**: cantidades redondas, todo
repartido en partes iguales, un solo prefijo de tarjeta. Se auditó el generador y se
corrigieron **ocho problemas**.

| # | Antes (se veía fabricado) | Ahora |
|---|---|---|
| 1 | Exactamente `20.000` clientes y `400.000` transacciones | **18.473** y **397.412** — cantidades sin forma |
| 2 | IDs consecutivos: `CLI000001`, `CLI000002`… | IDs **con huecos**: los datos reales tienen bajas y cuentas canceladas |
| 3 | Proporciones redondas: 60/30/10, 45/30/15/10 | Proporciones sin forma: **63,82 / 27,45 / 8,73** |
| 4 | **Hora uniforme**: las 24 horas con el mismo volumen | Curva con **dos picos** (almuerzo y tarde), madrugada marginal |
| 5 | **Comercios equiprobables**: cada uno 1/20 | Urna **ponderada**: el supermercado tiene 43% y la agencia de viajes 0,9% |
| 6 | Monto solo por segmento, con `pow(rand,3)` | **Log-normal** cruzando **segmento × categoría**: un café no cuesta como un vuelo |
| 7 | Canal y país independientes de todo | **Correlacionados con la categoría**: nadie compra combustible por internet, y el exterior se concentra en viajes |
| 8 | **Todas las tarjetas** empezaban con `4539` | **8 prefijos** por marca, y la tarjeta pertenece al cliente (siempre la misma) |

### Verificado con datos reales

**Distribución de hora** — tiene forma, no es plana:

```
 1h  246    ·  madrugada marginal
 5h  1078
 7h  1736
11h  4326   ← pico de almuerzo
13h  4514   ← máximo
15h  2710
18h  1373
23h  193
```

**Monto (log-normal)** — cola larga a la derecha, como el gasto real:

| mín | p25 | mediana | promedio | p95 | máx |
|---|---|---|---|---|---|
| 5.634 | 53.651 | **81.459** | 98.501 | 226.325 | **1.280.400** |

El promedio **por encima** de la mediana es la firma de una distribución sesgada. Si fueran
iguales, sería uniforme — y se vería falso.

**Comercios** — de 43% a 0,9%:

| Comercio | % de transacciones |
|---|---|
| SuperMercado Andino | 43,3% |
| Comidas Rápidas Sabor | 22,0% |
| MiniMarket 24h | 18,9% |
| Streaming PlayMax | 11,8% |
| Aerolínea Cielo Azul | 1,9% |
| Agencia Viajes Horizonte | **0,9%** |

### Otras correlaciones plantadas

- **Score crediticio** correlacionado con segmento **y** antigüedad, con dispersión normal
  (`randn`), no rango plano
- **Edad** en campana centrada en ~40, no uniforme entre 18 y 85
- **Antigüedad** sesgada a clientes nuevos: una cartera real capta más de lo que retiene
- **`dispositivo_id`** presente en 96% de canales digitales y solo 18% de presencial —
  antes era 30% de nulos al azar, sin lógica
- **Volumen creciente** en los meses recientes: el banco crece
- **Clientes con actividad desigual** (ley de Pareto): unos transan mucho más que otros

### Celda de control incluida

El notebook imprime las distribuciones de clientes por segmento y región, y de edad por
rango. **Si esas tablas se ven parejas, algo está mal.**

### 🔴 Corrección: `.cache()` no funciona en serverless

Al ejecutar el generador apareció:

```
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute
```

**El error era solo el síntoma.** El problema de fondo es más serio: `rand()` y `randn()` se
**reevalúan en cada acción**. Sin materializar los datos:

- cada `count()`, cada `display()` y cada escritura habrían producido **valores distintos**
- la tabla de clientes **no coincidiría** con las transacciones que la referencian
- el **umbral de fraude** se calculaba con un percentil sobre `_riesgo` y después se aplicaba
  como filtro: dos evaluaciones distintas → la tasa de fraude habría salido cualquier cosa,
  no 1,5%

**La solución tiene dos partes:**

1. **Materializar en tablas Delta temporales** (`_gen_clientes`, `_gen_tx`) en vez de
   `.cache()`. Escribir a disco congela los valores aleatorios una sola vez. Las tablas se
   borran automáticamente al final, después de escribir los archivos.

2. **Reemplazar `rand()` por `hash()` donde la decisión debe ser estable.** Las filas
   inválidas del Módulo 3 ahora se eligen con
   `ABS(HASH(CONCAT(transaccion_id,'m'))) % 1000 < 20` — determinístico, así el conteo que se
   imprime coincide exactamente con lo que se escribe a los archivos.

**Verificado en el workspace:** el patrón de hash produce **1,98%** de filas inválidas
(1.266 en cero + 715 negativos de 100.000) y da **el mismo resultado en ejecuciones
repetidas**. Con `rand()` los dos conteos habrían diferido.

**Orden correcto de operaciones**, que ahora respeta el notebook:
```
generar clientes  →  materializar  →  usar en el join
generar tx        →  materializar  →  calcular umbral  →  etiquetar fraude
```
El umbral **tiene que** calcularse después de materializar. Era el bug más difícil de
detectar: no lanza error, solo produce datos incoherentes.


### 📝 Rediseño del ejercicio del paso 5 (el `UPDATE`)

La primera versión decía *"escribe una instrucción que ponga es_fraude = true donde el monto
sea mayor a 1.000.000"* y mostraba la plantilla de `UPDATE`. **No quedaba claro sobre qué
tabla, ni por qué.** Se rediseñó en tres partes:

**1 · Medir antes de cambiar** (paso 5.1) — dos celdas nuevas:

```sql
-- cuántos fraudes hay ahora
SELECT COUNT(*) FROM mis_transacciones WHERE es_fraude = true

-- cuántas filas se van a tocar, y cuántas cambian de verdad
SELECT COUNT(*) AS pasan_de_un_millon,
       SUM(CASE WHEN es_fraude THEN 1 ELSE 0 END) AS de_esas_ya_eran_fraude,
       SUM(CASE WHEN NOT es_fraude THEN 1 ELSE 0 END) AS de_esas_van_a_cambiar
FROM mis_transacciones WHERE monto > 1000000
```

Enseña el hábito de tener una cifra de referencia antes de modificar datos.

**2 · Los tres datos que necesita, en tabla explícita:**

| | Valor | De dónde sale |
|---|---|---|
| Qué tabla | `mis_transacciones` | La que creó en el paso 3 |
| Qué columna y a qué valor | `es_fraude` → `true` | Lo que pide el equipo de riesgo |
| En qué filas | `monto > 1000000` | El umbral del mensaje |

Más la plantilla anotada, la advertencia de que **un `UPDATE` sin `WHERE` cambia todas las
filas**, y la nota de que en SQL `1000000` se escribe sin puntos.

**3 · El "por qué" se movió DESPUÉS del ejercicio**, para no competir con la instrucción.
Explica que el almacenamiento de objetos no permite modificar parte de un archivo, que antes
había que reescribir el conjunto completo, y de ahí a las leyes de privacidad (GDPR, Ley 1581
de Colombia) y el derecho al borrado.

#### 🔴 Dos correcciones que la prueba encontró

Se ejecutó el flujo real y aparecieron dos errores en el material:

| | Decía | Es |
|---|---|---|
| Filas afectadas | «unas **35** filas» | **29** |
| Con qué coincide `num_affected_rows` | con `de_esas_van_a_cambiar` (25) | con `pasan_de_un_millon` (**29**) |

El segundo es conceptual y **se convirtió en contenido**: `UPDATE` toca todas las filas del
`WHERE` sin revisar si el valor ya era el que se le asigna. Las 4 filas que ya tenían
`es_fraude = true` se reescriben igual. El notebook ahora lo explica y añade la práctica de
producción: `WHERE monto > 1000000 AND es_fraude = false` para no reescribir de más.

También se verificó que `LIMIT 1000` es **determinístico** en este caso — tres ejecuciones
dieron 29 filas — así que se puede prometer la cifra en el material.


### ❓ Aclaración: el Módulo 1 NO construye la capa bronze

El notebook era **ambiguo** en este punto. Decía *"la capa bronze guarda el dato como llegó"*
justo después de crear `mis_transacciones`, lo que sugería que esa tabla ya era bronze — pero
al final decía que bronze se construye en el Módulo 2. Contradictorio.

**Corregido: la respuesta aparece explícitamente en cuatro lugares del notebook.**

| Dónde | Qué dice |
|---|---|
| Tabla de apertura | «Creas tu primera tabla *(de práctica, con 1.000 filas)*» |
| Encabezado del paso 3 | «ℹ️ Esta es una tabla de práctica, **no tu capa bronze**» |
| Celda nueva tras `DESCRIBE EXTENDED` | Sección **«❓ ¿Esta tabla ya es mi capa bronze?»** → «**No todavía**» |
| Checkpoint y cierre | Diagrama de las capas con un «✋ estás acá» |

#### La celda nueva: «¿Esta tabla ya es mi capa bronze?»

Responde **no**, y explica la diferencia con una tabla comparativa verificada contra el código:

| | `mis_transacciones` | La bronze del M2 |
|---|---|---|
| Filas | 1.000 (`LIMIT`) | **375.000** |
| Columnas | **10** | **13** — las del origen |
| Carga | manual, una vez | **incremental** con Auto Loader |
| Para qué | aprender los conceptos | base real del resto del día |

Se verificó cuáles columnas quedan fuera del paso 3: **`numero_tarjeta`, `moneda` y
`dispositivo_id`** — las tres que el notebook nombra.

También sitúa las tres capas con el módulo donde se construye cada una (bronze → M2,
silver y gold → M3) y **argumenta por qué separar en capas**: la limpieza se hace con reglas,
las reglas cambian, y si transformas al ingerir y descartas el original no tienes de dónde
rehacerla.

Cierra con la precisión de que la medallion es una **recomendación, no una regla** — como dice
la documentación oficial.

#### Diagrama de ubicación, en el cierre

```
  archivos crudos  →  🥉 bronze  →  🥈 silver  →  🥇 gold
       (ya están)      (Módulo 2)   (Módulo 3)   (Módulo 3)

  ✋ estás acá: hiciste una tabla de PRÁCTICA con 1.000 filas
     para aprender Delta Lake. Todavía no construiste bronze.
```


### 🔍 Celda de explicación de `DESCRIBE EXTENDED`

Después de ejecutar `DESCRIBE EXTENDED` hay una celda que desglosa el resultado. Se escribió
**ejecutando el comando de verdad** contra el workspace y verificando que cada campo
mencionado aparece (26 filas de salida).

Cubre las dos partes del resultado:

**Parte 1 · columnas y tipos** — con un hallazgo aprovechado como material didáctico:

> **`fecha` se infiere como `string`, no como timestamp.** En JSON no existe el tipo fecha,
> así que Databricks leyó texto y guardó texto. El notebook explica que **hace bien en no
> adivinar** (¿`03/14` es 14 de marzo o 3 de febrero?), cuál es la consecuencia práctica (no
> se puede filtrar por rango ni agrupar por mes), y que **convertir tipos es el trabajo del
> M3**, al pasar de bronze a silver. Refuerza el principio de que **bronze guarda el dato
> como llegó**.

**Parte 2 · metadatos** — tabla con las 9 líneas que vale entender (`Provider`, `Catalog`,
`Database`, `Table`, `Type`, `Location`, `Owner`, `Statistics`, `Table Properties`,
`Predictive Optimization`).

**Tres se desarrollan como diferenciales**, verificados en la salida real:

| Campo | Valor real | El argumento |
|---|---|---|
| `Location` | `s3://serverless-stable-…` | Los datos están **en tu propia nube**, en formato abierto. Un warehouse tradicional los guarda en formato propietario. *"Si mañana quisieras irte, podrías."* |
| `Type` | `MANAGED` | Tabla comparativa MANAGED vs EXTERNAL en tres dimensiones: quién decide la ubicación, qué pasa al hacer `DROP`, y optimización automática |
| `Predictive Optimization` | `ENABLE (inherited…)` | Databricks observa cómo se consulta la tabla y la optimiza solo. En otras plataformas eso son mantenimientos programados y horas de alguien |

Cierra con un hábito concreto: **ante una tabla desconocida, corre `DESCRIBE EXTENDED`** — en
5 segundos sabes tipo, ubicación, dueño y tamaño.


### Widgets: elegir catálogo y schema

**Los widgets se crean en una celda y se leen en la SIGUIENTE.** Esto no es estético: en la
primera ejecución los campos **todavía no existen**, así que si se crean y se leen en la
misma celda, `dbutils.widgets.get()` devuelve el valor por defecto — y el notebook falla
antes de que el participante haya podido escribir nada.

Eso pasó en la prueba real: el default era `z2h`, que no existe en el workspace, y salía
`❌ El catálogo 'z2h' no existe` en la primera celda del notebook.

```
CELDA A · crea los campos  →  el participante los ajusta arriba  →  CELDA B · los lee
```

**Además, `01_lakehouse_101` ya no adivina el catálogo: lo BUSCA.** Recorre los catálogos
visibles buscando un schema llamado `fin_<usuario>` y deja los campos con los valores
correctos. Verificado en el workspace:

```
agent_tests                       —
fevm_shared_catalog               —
serverless_stable_5b5210_catalog  ✅ ENCONTRADO
→ widgets: catalogo='serverless_stable_5b5210_catalog', schema='fin_carolina_carneiro'
```

El participante no tiene que recordar qué escribió en el notebook anterior. Si el catálogo
no se puede listar por permisos, se ignora y sigue con el siguiente.

Los dos notebooks usan **widgets**, no detección automática:

| Widget | Valor por defecto |
|---|---|
| `catalogo` | `z2h` |
| `schema` | `fin_<usuario>` — derivado del usuario que ejecuta |

**`00_generar_datos` crea lo que falte:**
1. Si el catálogo no existe, **intenta crearlo**
2. Si no puede (lo normal en workspaces corporativos), **lista los catálogos donde el
   usuario sí puede crear schemas** y pide elegir uno
3. Crea el schema y el volumen `raw`

**`01_lakehouse_101` no crea nada** — comprueba que existan y que los archivos estén ahí. Si
faltan, dice explícitamente *"¿corriste 00_generar_datos con estos mismos valores?"*.

Se validan los nombres antes de usarlos (solo letras, números y guión bajo) y todos los
identificadores van entre backticks, para que un nombre con guión no rompa el SQL.

**Probado en el workspace:**

| Escenario | Resultado |
|---|---|
| `CREATE CATALOG z2h` sin permiso | ❌ falla con *"Metastore storage root URL does not exist"* → el notebook lo captura y sugiere alternativas |
| Detección de catálogos escribibles | ✅ encontró `serverless_stable_5b5210_catalog` |
| `CREATE SCHEMA` + `CREATE VOLUME` en uno válido | ✅ |


### Semilla fija

`SEMILLA = 42`. Todos generan **exactamente los mismos datos**, así que:
- Los números que uno ve coinciden con los del vecino
- El facilitador puede decir cifras concretas desde el escenario
- Un problema es reproducible

---

## `01_lakehouse_101.py` — el ejercicio

### Los 9 pasos

| Paso | Qué hace | Tipo |
|---|---|---|
| 1 | Prepara el espacio de trabajo | ya escrito |
| 2 | Mira los archivos crudos y su contenido | ya escrito |
| 3 | **Crea la primera tabla Delta** | 📝 **TODO** |
| 4 | Lee `DESCRIBE HISTORY` — una sola versión | ya escrito |
| 5 | **Modifica la tabla con `UPDATE`** | 📝 **TODO** |
| 6 | **Time travel** con `VERSION AS OF` y `TIMESTAMP AS OF` | ⭐ el momento clave |
| 7 | Le pide SQL a Genie Code | 📝 abierto |
| 8 | Encuentra la tabla en Catalog Explorer | exploración |
| 9 | **Verifica el checkpoint** | automático |

### Los 2 TODOs

Son de **completar**, no de escribir desde cero, y cada uno tiene la solución en un bloque
colapsable justo debajo.

1. **Paso 3** — indicar el formato de los archivos (`json`) en `FROM FORMATO.\`ruta\``
2. **Paso 5** — escribir `UPDATE ... SET es_fraude = true WHERE monto > 5000000`

### La narrativa del ejercicio

No es una lista de comandos: es una **situación**.

> Llega un ticket urgente: *"marca como fraude todo lo que pase de 5 millones"*. Lo haces.
> Al rato te dicen: *"nos equivocamos, ¿puedes ver cómo estaba antes?"*

En una base de datos tradicional esa conversación termina en *"tendría que restaurar un
backup"*. Con Delta es **una consulta**. Eso es lo que el participante siente en el paso 6.

### El checkpoint automático

El paso 9 comprueba tres cosas e imprime un resultado legible:
1. La tabla existe y tiene filas
2. Tiene **al menos 2 versiones** en el historial
3. Time travel devuelve conteos **distintos** entre v0 y ahora

Si los conteos son iguales, avisa específicamente: *"¿corriste el UPDATE del paso 5?"* — que
es el error más probable.

---

## Estado de las pruebas

**Probado en `fevm-serverless-stable-5b5210`** vía SQL warehouse:

| Qué | Resultado |
|---|---|
| `element_at` sobre array de literales (comercios) | ✅ |
| `create_map` + lookup (categoría por comercio) | ✅ |
| `pow(rand(), 3)` para sesgar montos | ✅ |
| `randn()` para el ruido del score de riesgo | ✅ |
| `date_sub` + `to_timestamp` (construcción de fechas) | ✅ |
| Crear tabla Delta, `INSERT`, `UPDATE` | ✅ |
| `DESCRIBE HISTORY` → 3 versiones | ✅ |
| Subqueries con `VERSION AS OF` (la consulta del paso 6) | ✅ |
| `RESTORE TABLE` (reto opcional) | ✅ el restore es una versión nueva |
| **`TIMESTAMP AS OF current_timestamp() - INTERVAL 1 MINUTE`** | ❌ **falló** |

### 🔴 El bug que la prueba encontró

`TIMESTAMP AS OF current_timestamp() - INTERVAL 1 MINUTE` **falla** con
`DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION`: la tabla se acaba de crear, así que "hace un
minuto" es **antes de que existiera**.

Habría fallado para los 40 participantes, en el paso estelar del módulo.

**Corregido:** ahora el notebook lee el timestamp real de la versión 0 con
`DESCRIBE HISTORY`, lo imprime, y lo usa en la consulta. Además explica el comportamiento —
que Delta avise en vez de devolver datos equivocados es justamente lo que se quiere.

### ⚠️ Falta probar

**Ejecutar `00_generar_datos` completo** en un notebook real. Se validaron las funciones SQL
que usa, pero no la generación de las 400.000 filas ni la escritura de los archivos. Toma
2–3 minutos y conviene hacerlo antes del ensayo.

Verificar sobre todo el **control de calidad** del paso 3: que la tasa de fraude por canal y
la diferencia de monto entre normal y fraude sean claras. Si no lo son, hay que ajustar los
pesos del `_riesgo`.

---

## ✅ Ejecución real verificada (2026-08-06)

El generador corrió completo en `fevm-serverless-stable-5b5210`. Resultado:

| Métrica | Valor | Esperado |
|---|---|---|
| Clientes | **18.473** | ~18.473 ✅ |
| Transacciones generadas | **397.412** | ~397.412 ✅ |
| → archivo principal (12 JSON) | **375.459** | ~377.000 ✅ |
| → lote incremental (1 JSON) | **21.953** | ~20.000 ✅ |
| Fraudes | **6.162 (1,55%)** | ~1,5% ✅ |
| Filas inválidas | **7.934 (2,00%)** | ~2% ✅ |
| Join sin pérdida de filas | **397.412 → 397.412** | sin pérdida ✅ |
| Comercios distintos | **25** | ≥20 ✅ |
| Columnas auxiliares filtradas a los archivos | **ninguna** | ninguna ✅ |

Esquema final en los archivos, 13 columnas: `canal` · `categoria_comercio` · `cliente_id` ·
`comercio` · `dispositivo_id` · `es_fraude` · `fecha` · `moneda` · `monto` ·
`numero_tarjeta` · `pais` · `region` · `transaccion_id`

### Lifts medidos sobre los datos REALES

| Señal | Tasa de fraude | Lift |
|---|---|---|
| Base (todas) | 1,55% | 1,0× |
| Exterior (`pais <> 'CO'`) | **16,99%** | **11,0×** |
| Madrugada (2–5h) | **10,74%** | **6,9×** |
| Monto en el top 2% | 7,22% | 4,7× |
| Canal online | 3,06% | 2,0× |
| Sin dispositivo | 1,46% | 0,9× |

Coinciden con la simulación previa. La única que no discrimina es *sin dispositivo* (0,9×):
su peso en el generador es 0,6, el más bajo, y queda tapado por el ruido. **No es un
problema** — un dataset donde todas las features predicen se vería artificial.

### 🔵 AUC alcanzable: depende de la ingeniería de features

Medido sobre los datos reales, con score lineal (sin entrenar modelo):

| Features usadas | AUC |
|---|---|
| Monto **absoluto** | 0,756 |
| Monto **relativo a su categoría** | 0,822 |
| **+ join a clientes** (antigüedad, score crediticio) | **0,859** |

**Consecuencia directa para el M6:** el AUC objetivo de 0,85–0,92 **solo se alcanza con el
join a la tabla de clientes y con el monto normalizado por categoría**. Si el ejercicio del
M6 entrena sobre el monto absoluto de la tabla bronze sin enriquecer, se queda en ~0,76.

Eso es pedagógicamente útil, no un defecto: es exactamente la lección de que **la ingeniería
de features importa más que el algoritmo**. El M6 puede mostrar la progresión: entrenar
primero con lo básico, ver 0,76, agregar el join y la normalización, ver 0,86.

Un GBT real debería superar estos números, porque encuentra interacciones y umbrales
óptimos que el score manual no tiene.

### 🔴 Hallazgo para el Módulo 2: `csv.\`ruta\`` ignora el encabezado

Al leer el CSV de clientes con la sintaxis directa:

```sql
SELECT cliente_id FROM csv.`/Volumes/.../raw/clientes`
```
```
[UNRESOLVED_COLUMN.WITH_SUGGESTION] cannot be resolved.
Did you mean: [`_c0`, `_c1`, `_c2`, `_c3`, `_c4`]?
```

La sintaxis `csv.\`ruta\`` **no acepta opciones**, así que trata el encabezado como una fila
de datos y nombra las columnas `_c0.._c5`. El JSON no tiene este problema.

**El Módulo 2 tiene que leer el CSV con `spark.read.option("header","true")` o con Auto
Loader configurado con esa opción** — no con la sintaxis directa. Conviene que el ejercicio
lo mencione, porque el error es confuso para alguien que ve Databricks por primera vez.


---

## 🔴 Corrección importante: el umbral del paso 5 era 5.000.000

Al probar el ejercicio contra los datos reales apareció un problema que **habría roto el
checkpoint para toda la sala**.

El paso 5 pedía marcar como fraude las transacciones de más de **5.000.000**. Sobre la
muestra de 1.000 filas:

| Umbral | Filas afectadas | Filas que realmente cambian |
|---|---|---|
| 5.000.000 | **1** | **1** |
| **1.000.000** | **36** | **35** |
| 500.000 | 81 | 77 |

Con 5 millones, el `UPDATE` afectaba **una sola fila**. Y si esa única fila ya tenía
`es_fraude = true`, el conteo antes y después sería idéntico → el checkpoint del paso 9
habría dicho *"los dos conteos son iguales, ¿ejecutaste el UPDATE?"* a alguien que **sí** lo
ejecutó. Un falso negativo, en el paso estelar del módulo.

**Corregido a 1.000.000.** Verificado end to end: 13 fraudes en v0 → 38 ahora, **diferencia
de 25 filas**. Visible y sin ambigüedad.

También se verificó el reto opcional: `RESTORE TABLE ... TO VERSION AS OF 0` deja el
historial en `v0 CREATE` · `v1 UPDATE` · `v2 RESTORE` — la versión 2, exactamente como dice
la respuesta del notebook.

---

## 📚 Rediseño didáctico del notebook 01

El notebook se reescribió partiendo de un supuesto explícito: **quien lo lee no ha usado
Databricks nunca** y tiene que poder avanzar solo, sin nadie al lado.

### Estructura de cada paso

Los 9 pasos siguen el mismo patrón, siempre en el mismo orden:

| Sección | Qué contiene |
|---|---|
| **Qué vas a hacer** | La acción, en una frase |
| **Por qué importa** | El concepto, y qué problema real resuelve |
| **🔷 Por qué es un diferencial de Databricks** | Solo donde aplica de verdad, no en todos |
| **El código** | Comentado línea por línea donde no es obvio |

### Lo que se agregó para quien empieza de cero

- **Cómo leer un notebook**: qué es una celda, `Shift+Enter`, por qué no usar "Run all"
- **Se explica cada elemento la primera vez que aparece**: `%sql`, `display()`, widgets,
  `dbutils.fs`, `USE`, qué es un VOLUME, qué hace `current_user()`
- **Tablas comparativas** en vez de párrafos: *archivo suelto vs tabla Delta* en cuatro
  dimensiones, *notebook de aprendizaje vs producción* en seis
- **La narrativa como situación, no como lista de comandos**: llega un ticket urgente →
  lo ejecutas → te avisan que estaba mal → lo recuperas con una consulta. En el paso 6 se
  invita explícitamente a **detenerse a pensar** cómo se resolvería eso sin Delta
- **Mensajes de error que dicen qué hacer**, no solo qué falló

### Los diferenciales que el notebook argumenta

No como afirmaciones de marketing, sino ancladas a lo que la persona acaba de ejecutar:

| Dónde | El argumento |
|---|---|
| Paso 3 | Antes había que elegir warehouse **o** data lake. Delta trajo las garantías del warehouse a formato abierto — y es **open source** (Linux Foundation, 2019), así que los datos quedan en almacenamiento propio y legible por otras herramientas |
| Paso 4 | La auditoría *"quién cambió qué y cuándo"* viene por defecto; en otras plataformas hay que construirla |
| Paso 5 | `UPDATE` sobre archivos en la nube **no se podía hacer** antes de Delta — es lo que permite cumplir el derecho al olvido |
| Paso 6 | Cuatro usos reales del time travel, incluida la **reproducibilidad de modelos de ML** |
| Paso 8 | Unity Catalog gobierna **tablas y archivos con las mismas reglas** — normalmente son dos mundos separados |

### Los cuatro disclaimers de "no es production ready"

1. **Al inicio**, en una tabla de cuatro filas junto al objetivo pedagógico y la
   advertencia de datos sintéticos
2. **Un párrafo** que explica *por qué* un notebook didáctico y una implementación
   productiva se parecen poco
3. **Al final**, una tabla de seis dimensiones: errores, pruebas, despliegue,
   configuración, datos y costos — este notebook vs producción
4. **La aclaración que evita el malentendido**: los *conceptos* sí se trasladan tal cual;
   lo que cambia es la ingeniería alrededor

### Aplicabilidad a otras industrias

Aparece **dos veces**, a propósito:

- **Al inicio**, para que nadie del sector no financiero sienta que el taller no es para
  ellos
- **Al final**, con una tabla de seis industrias — manufactura, retail, telecom, salud,
  logística, seguros — y la observación de que el time travel resuelve la misma pregunta en
  todas: *"¿cómo estaban los datos cuando se tomó esta decisión?"*


---

## Regenerar

Si un participante rompe sus datos, basta con volver a correr `00_generar_datos`. Sobrescribe
todo y es idempotente.
