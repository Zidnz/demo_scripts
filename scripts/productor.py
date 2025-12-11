import json
import time
import random
import datetime
from faker import Faker
from kafka import KafkaProducer

# Configuración
fake = Faker('es_MX')
TOPIC_NAME = 'transacciones_bancarias'
KAFKA_SERVER = 'localhost:9092'
PROBABILIDAD_FRAUDE = 0.09 

CIUDADES = [
    'Toluca', 'Puebla', 'Guadalajara', 'Aguascalientes', 'Chihuahua', 'Mazatlan',
    'Culiacan', 'Ciudad de Mexico', 'Ciudad Juarez', 'Queretaro', 'Cancun',
    'Merida', 'San Luis Potosi', 'Morelia', 'Saltillo', 'Hermosillo', 'Leon',
    'Tijuana', 'Veracruz', 'Monterrey', 'Ciudad no especificada'
]

TIPOS_TRANSACCION = [
    'Retiro', 'Transferencia internacional', 'Compra en linea', 'Pago de nomina',
    'Pago de servicio', 'Deposito', 'Transferencia'
]


TIPOS_RIESGO = ['Transferencia internacional', 'Transferencia', 'Retiro']

CATEGORIAS = ['Ingreso', 'Nomina', 'Servicios', 'Remesas', 'Consumo', 'Ahorro', 'Transferencia personal', 'Efectivo']

CONCEPTOS = [
    'Compra tienda online', 'Nomina mensual', 'Envio remesas', 'Pago a proveedor',
    'Pago trabajador', 'Deposito por transferencia', 'Compra Amazon', 'Retiro por emergencia',
    'Pago internet', 'Deposito en efectivo', 'SWIFT a extranjero', 'Compra Mercado Libre',
    'Retiro en cajero', 'Deposito por cheque', 'Abono nomina', 'Pago TV por cable',
    'Pago luz', 'Suscripcion streaming', 'Transferencia a familiar', 'Pago agua',
    'Pago telefonia', 'Pago a tercero', 'Nomina quincenal', 'Transaccion no especificada',
    'Abono a tarjeta', 'Pago internacional'
]

CANALES = ['App movil', 'Domiciliacion', 'Sucursal bancaria', 'Web', 'Corresponsal', 'Cajero automatico']
MEDIOS_PAGO = ['Domiciliacion', 'SPEI', 'Cheque', 'Tarjeta credito', 'Efectivo', 'Tarjeta debito', 'SWIFT', 'Deposito en ventanilla', 'Pago movil', 'Transferencia internacional', 'Transferencia bancaria']
ESTATUS = ['Completada', 'Cancelada', 'Pendiente', 'Rechazada']

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def generar_transaccion_realista():
    es_simulacion_fraude = random.random() < PROBABILIDAD_FRAUDE

    if es_simulacion_fraude:
        monto = round(random.uniform(15000, 50000), 2)
        tipo = random.choice(TIPOS_RIESGO)
        estatus_txn = random.choice(['Completada', 'Pendiente']) 
    else:
        # Comportamiento normal
        monto = round(random.uniform(70, 25000), 2)
        tipo = random.choice(TIPOS_TRANSACCION)
        estatus_txn = random.choice(ESTATUS)

    fecha_actual = datetime.datetime.now()
    saldo_previo = round(random.uniform(-5000, 100000), 2)
    
    if tipo in ['Deposito', 'Pago de nomina']: 
        saldo_posterior = saldo_previo + monto
    else:
        saldo_posterior = saldo_previo - monto

    txn = {
        'id_transaccion': fake.random_int(min=200000, max=999999),
        'id_cliente': fake.random_int(min=1000, max=50000),
        'nombre_cliente': fake.name(),
        'cuenta_origen': fake.random_number(digits=18),
        'fecha': fecha_actual.strftime("%d/%m/%Y"),
        'monto': monto,
        'divisa': 'MXN',
        'tipo_transaccion': tipo,
        'ciudad': random.choice(CIUDADES),
        'cuenta_destino': fake.random_number(digits=16),
        'categoria': random.choice(CATEGORIAS),
        'hora_movimiento': fecha_actual.strftime("%H:%M:%S"),
        'concepto_movimiento': random.choice(CONCEPTOS),
        'saldo_previo': saldo_previo,
        'saldo_posterior': round(saldo_posterior, 2),
        'canal_transaccion': random.choice(CANALES),
        'medio_pago': random.choice(MEDIOS_PAGO),
        'estatus': estatus_txn
    }
    
    return txn, es_simulacion_fraude

print(f"📡 Productor Activo: Enviando datos SIN etiqueta...")

try:
    while True:
        data, es_fraude = generar_transaccion_realista()
        
        producer.send(TOPIC_NAME, data)

        print(f"Enviado: ${data['monto']:>9.2f} | {data['tipo_transaccion']}")
        
        time.sleep(random.uniform(0.5, 1.5))

except KeyboardInterrupt:
    print("\n🛑 Productor detenido.")
finally:
    producer.close()