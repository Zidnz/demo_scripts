import eventlet
# Parche necesario para rendimiento asíncrono óptimo
eventlet.monkey_patch()

from flask import Flask, render_template
from flask_socketio import SocketIO, emit

# Configuración inicial
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secreto_fraude_dashboard'

# Habilitar CORS para permitir conexiones locales sin restricciones
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# --- RUTA WEB ---
@app.route('/')
def index():
    # Flask buscará 'index.html' dentro de la carpeta 'templates'
    return render_template('index.html')

# --- EVENTOS DE COMUNICACIÓN ---

@socketio.on('connect')
def handle_connect():
    print("✅ Cliente WEB conectado (Dashboard)")

@socketio.on('disconnect')
def handle_disconnect():
    print("❌ Cliente desconectado")

@socketio.on('data_from_spark')
def handle_spark_data(data):
    """
    Recibe el diccionario JSON desde consumidor.py (Spark)
    y lo reenvía al navegador para pintar las gráficas.
    """
    # 1. Enviar a todos los clientes conectados (navegadores)
    socketio.emit('new_transaction', data)
    
    # 2. Si es fraude, enviar alerta específica
    if data.get('is_fraud') == 1:
        print(f"🚨 ALERTA DE FRAUDE: ID {data.get('id')} - Score: {data.get('score')}")
        socketio.emit('new_alert', data)

# --- ARRANQUE ---
if __name__ == '__main__':
    print("🚀 Servidor Dashboard corriendo en http://localhost:5000")
    socketio.run(app, host='0.0.0.0', port=5000)