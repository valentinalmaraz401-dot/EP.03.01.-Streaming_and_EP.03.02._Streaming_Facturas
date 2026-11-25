# EP.03.01. Streaming en Apache Spark (PySpark Structured Streaming)

Bienvenido a este repositorio. Aquí encontrarás el desarrollo completo de la práctica **EP.03.01. Streaming**, donde implementamos el procesamiento de flujos de datos en **tiempo real** utilizando **Apache Spark Structured Streaming**, **Python (PySpark)** y sockets TCP con **Netcat (`nc`)**.

---

## ¿De qué trata este proyecto? (Explicación sencilla para cualquier persona)

Imagina la diferencia entre **llenar una cubeta de agua y luego analizarla** frente a **analizar el agua mientras corre por una tubería**:

* **Procesamiento tradicional (Batch):** Es como analizar la cubeta llena. Tienes un archivo guardado (por ejemplo, las ventas del mes pasado), lo abres, haces tus cálculos y obtienes un reporte.
* **Procesamiento en tiempo real (Streaming):** Es como poner un sensor dentro de la tubería. Los datos llegan sin parar (mensajes de chat, clics en una web, sensores de temperatura o transacciones bancarias) y **Apache Spark** los va leyendo, limpiando y contando **al instante**, en pequeños bloques llamados *micro-batches*.

En este proyecto construimos una tubería de datos en vivo:
1. **El emisor (Netcat / Python):** Simula usuarios o sensores enviando palabras en tiempo real a través de un puerto de red (`localhost:9999`).
2. **El cerebro (Spark Structured Streaming):** Escucha ese puerto continuamente, separa las frases en palabras, las filtra o cuenta cuántas veces aparece cada una, y muestra los resultados actualizados en pantalla cada pocos segundos.

---

## Conceptos Clave de Structured Streaming

* **Fuente de datos (`readStream`):** Punto de entrada continuo (en esta práctica usamos un **Socket TCP** en `localhost:9999` para pruebas; en producción se usan sistemas como Apache Kafka o directorios de archivos).
* **Transformaciones:**
  * *Sin estado (`stateless`):* Cada dato nuevo se procesa de forma independiente (ej. `filter`, `select`, `upper`, `explode`).
  * *Con estado (`stateful`):* Spark recuerda resultados anteriores entre micro-batches para acumular totales (ej. `groupBy().count()`).
* **Modos de salida (`outputMode`):**
  * **`complete`:** Imprime la tabla completa actualizada en cada micro-batch (ideal para conteos acumulados).
  * **`update`:** Imprime únicamente las filas nuevas o que cambiaron su valor en el último micro-batch.
  * **`append`:** Solo inserta filas nuevas que no van a modificarse después (usado en transformaciones sin estado).
* **Triggers (`trigger`):** Controlan **cada cuánto tiempo** Spark procesa los datos acumulados (por defecto lo hace apenas llegan; con `processingTime="10 seconds"` espera 10 segundos para agrupar varios mensajes en un solo lote).
* **Destino / Sink (`writeStream`):** Lugar donde se envían los resultados procesados (consola, memoria, archivos Parquet/CSV o Kafka).

---

## Estructura del Repositorio

```text
EP.03.01.-Streaming-/
├── capturas/                               # Evidencias visuales de cada fase
│   ├── 01_entorno_streaming.png            # Creación de carpeta y verificación de Netcat (nc)
│   ├── 02_prueba_netcat_python.png         # Cliente Python enviando palabras al servidor Netcat
│   ├── 02_prueba_netcat_python_2.png       # Recepción continua de mensajes en puerto TCP 8888
│   ├── 03_caso1_wordcount_streaming.png    # Caso 1: Configuración de Spark readStream/writeStream
│   ├── 03_caso1_wordcount_streaming_2.png  # Caso 1: Salida de micro-batches en modo complete
│   ├── 04_ejercicio1_modo_update.png       # Ejercicio 1: Comparación con outputMode("update")
│   ├── 05_ejercicio1_modo_append.png       # Ejercicio 1: Transformación sin estado con outputMode("append")
│   └── 06_ejercicio2_triggers.png          # Ejercicio 2: Agrupación por intervalo (processingTime="10 seconds")
├── emisor.py                               # Script en Python que simula un flujo continuo por sockets TCP
├── Streaming1.ipynb                        # Notebook principal de JupyterLab con todo el código ejecutado
├── Streaming1.pdf                          # Exportación completa del notebook con sus salidas
└── README.md                               # Documentación del proyecto
```

---

## Desarrollo de la Práctica

### 1. Preparación del entorno y pruebas con Netcat (`nc`)
* Se verificó la disponibilidad de `nc` (OpenBSD netcat) en Ubuntu Linux.
* Se desarrolló `emisor.py` usando la librería estándar `socket` para enviar palabras aleatorias con intervalos de 0.5 a 1.5 segundos hacia un servidor en escucha (`nc -lk 8888`).

### 2. Caso 1: Hola Spark Streaming (`WordCount` en tiempo real)
* Se configuró `SparkSession` con `.master("local[2]")` (dos hilos: uno dedicado a recibir datos del socket y otro a procesarlos).
* Se leyó texto en vivo desde `localhost:9999`, se dividieron las líneas en palabras con `split` y `explode`, y se calculó el conteo acumulado con `groupBy("palabra").count()` usando `.outputMode("complete")`.

### 3. Ejercicio 1: Comparación de Modos de Salida (`complete`, `update`, `append`)
* **`complete`:** Reimprime todo el diccionario histórico de palabras en cada lote.
* **`update`:** Muestra únicamente las palabras nuevas o actualizadas en el lote actual, reduciendo la carga de salida.
* **`append`:** Aplicado sobre una tubería sin estado (`filter(length("palabra") > 4)` + `upper("palabra")`), emitiendo solo registros nuevos filtrados y en mayúsculas.

### 4. Ejercicio 2: Control de frecuencia con Triggers (`processingTime`)
* Se configuró `.trigger(processingTime="10 seconds")`, comprobando cómo Spark retiene los mensajes entrantes durante ventanas de 10 segundos antes de disparar cada micro-batch.

---

## Cómo ejecutar este proyecto en local

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/valentinalmaraz401-dot/EP.03.01.-Streaming-.git
   cd EP.03.01.-Streaming-
   ```
2. **Abrir una terminal como servidor de datos (Netcat):**
   ```bash
   nc -lk 9999
   ```
3. **Activar el entorno virtual con PySpark y abrir JupyterLab:**
   ```bash
   source ~/Documents/"manejo masivo de datos"/spark-practicas/venv/bin/activate
   jupyter lab
   ```
4. **Ejecutar `Streaming1.ipynb`:** Corre las celdas en orden y escribe frases en la terminal de Netcat para ver el conteo en tiempo real.

---

## Referencias
* Apache Software Foundation. *Structured Streaming Programming Guide*. Apache Spark 3.5.3.
* Armbrust, M., et al. (2018). *Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark*. SIGMOD '18.
* Medrano, A. *Streaming en Spark — IABD*.