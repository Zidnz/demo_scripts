import sys
import logging
import socketio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, hour, log, udf, to_timestamp, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType
from pyspark.ml.tuning import CrossValidatorModel

# === 1. Conexión al Dashboard Web ===
sio = socketio.Client()
connected_to_dashboard = False
try:
    sio.connect('http://localhost:5000')
    print("✅ Conectado exitosamente al Dashboard Web")
    connected_to_dashboard = True
except Exception as e:
    print(f"⚠️ AVISO: No se pudo conectar al Dashboard en localhost:5000. \nError: {e}")
    print("➡️  El script seguirá corriendo pero no verás datos en el navegador.")

# === 2. Configuración de Spark ===
print("🚀 Iniciando Spark Streaming...")
spark = (
    SparkSession.builder.appName("DetectorFraudeConDashboard")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# === 3. Schema y Modelo ===
schema = StructType([
    StructField("id_transaccion", IntegerType(), True),
    StructField("fecha", StringType(), True),
    StructField("hora_movimiento", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("tipo_transaccion", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("canal_transaccion", StringType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("medio_pago", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("saldo_previo", DoubleType(), True),
    StructField("saldo_posterior", DoubleType(), True)
])

# RUTA DEL MODELO (Verifica que sea correcta en tu máquina)
MODEL_PATH = "/home/zidnz/DanaPP/proyecto_fraude/modelos/random_forest_fraude_cv"

try:
    cv_model = CrossValidatorModel.load(MODEL_PATH)
    print("✅ Modelo cargado correctamente.")
except Exception as e:
    print(f"❌ Error cargando modelo en {MODEL_PATH}: {e}")
    sys.exit(1)

# === 4. Feature Engineering ===
def aplicar_transformaciones(df):
    df = df.withColumn("timestamp_str", concat(col("fecha"), lit(" "), col("hora_movimiento")))
    df = df.withColumn("hora_del_dia", hour(to_timestamp(col("timestamp_str"), "dd/MM/yyyy HH:mm:ss")))
    
    # Flags de riesgo
    df = df.withColumn("feat_horario_riesgo", when((col("hora_del_dia") >= 0) & (col("hora_del_dia") <= 5), 1).otherwise(0))
    df = df.withColumn("feat_tipo_riesgo", when(col("tipo_transaccion").isin("Transferencia internacional", "Transferencia", "Retiro"), 1).otherwise(0))
    df = df.withColumn("feat_canal_riesgo", when(col("canal_transaccion").isin("Web", "App movil"), 0).otherwise(1))
    
    df = df.withColumn("feat_perfil_riesgo_completo", 
        when((col("feat_horario_riesgo") == 1) & (col("feat_tipo_riesgo") == 1), 1).otherwise(0)
    )
    
    # Log del monto
    df = df.withColumn("feat_log_monto", when(col("monto") > 0, log(col("monto") + 1)).otherwise(0))

    # Ratios
    df = df.withColumn("monto_promedio_tipo", 
        when(col("tipo_transaccion") == 'Compra en linea', 500.0)
        .when(col("tipo_transaccion") == 'Retiro', 1500.0)
        .when(col("tipo_transaccion").contains('Transferencia'), 8000.0)
        .otherwise(1000.0)
    )
    df = df.withColumn("feat_ratio_monto_vs_tipo", col("monto") / col("monto_promedio_tipo"))
    
    return df.fillna(0)

@udf(returnType=FloatType())
def get_fraud_prob(v):
    try: return float(v[1]) 
    except: return 0.0

# === 5. Leer Stream Kafka ===
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transacciones_bancarias")
    .load()
)

# === 6. Procesar y Predecir ===
json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
clean_df = json_df.filter(col("id_transaccion").isNotNull())
features_df = aplicar_transformaciones(clean_df)
pred_df = cv_model.transform(features_df)

# Preparar DataFrame final limpio para enviar
output_df = pred_df.withColumn("fraud_score", get_fraud_prob(col("probability"))).select(
    col("id_transaccion").alias("id"),
    col("monto").alias("amount"),
    col("ciudad").alias("state"),
    col("canal_transaccion").alias("platform"),
    col("fecha").alias("date"),
    col("hora_movimiento").alias("time"),
    col("prediction").cast("int").alias("is_fraud"),
    col("fraud_score").alias("score")
)

# === 7. Función de Envío (ForeachBatch) ===
def enviar_a_dashboard(batch_df, batch_id):
    rows = batch_df.collect()
    if rows:
        print(f"\n--- Procesando lote de {len(rows)} transacciones ---")
        for row in rows:
            data_dict = row.asDict()
            # Ajustes de formato para JSON
            data_dict['score'] = round(data_dict['score'], 4)
            data_dict['id'] = str(data_dict['id'])
            
            # Log visual en consola
            status_icon = "🚨" if data_dict['is_fraud'] == 1 else "✅"
            print(f"{status_icon} ID: {data_dict['id']} | ${data_dict['amount']} | Score: {data_dict['score']}")
            
            # Enviar por SocketIO si estamos conectados
            if connected_to_dashboard:
                try:
                    sio.emit('data_from_spark', data_dict)
                except Exception as e:
                    print(f"Error de envío: {e}")

# === 8. Iniciar Streaming ===
print("📡 Escuchando Kafka...")
query = (
    output_df.writeStream
    .outputMode("append")
    .foreachBatch(enviar_a_dashboard)
    .option("checkpointLocation", "/tmp/checkpoints_fraude_dashboard_v3") 
    .start()
)

query.awaitTermination()