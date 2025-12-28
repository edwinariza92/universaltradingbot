import time
import pandas as pd
import numpy as np
from binance.client import Client
from binance.enums import *
from datetime import datetime
import csv
import os
import requests
import traceback
import sys
import threading
import queue
import json
from requests.exceptions import ConnectionError, Timeout

# ======== CONFIGURACIÓN ========
api_key = 'Lw3sQdyAZcEJ2s522igX6E28ZL629ZL5JJ9UaqLyM7PXeNRLDu30LmPYFNJ4ixAx'
api_secret = 'Adw4DXL2BI9oS4sCJlS3dlBeoJQo6iPezmykfL1bhhm0NQe7aTHpaWULLQ0dYOIt'
symbol = 'DOGEUSDT'
intervalo = '30m'
riesgo_pct = 0.01  # 1% de riesgo por operación
umbral_volatilidad = 0.02  # ATR máximo permitido para operar
bb_length = 22  # Periodo por defecto para Bandas de Bollinger
bb_mult = 3.3  # Multiplicador por defecto para Bandas de Bollinger
atr_length = 3  # Periodo por defecto para ATR
ma_trend_length = 50  # Periodo por defecto para MA de tendencia
tp_multiplier = 3.6  # Multiplicador por defecto para Take Profit
sl_multiplier = 1.6  # Multiplicador por defecto para Stop Loss
usar_ma_trend = False  # Nuevo: usar filtro MA de tendencia (False por defecto)
# Nuevas configuraciones para gestión de riesgos
drawdown_max_pct = 0.05  # 5% drawdown máximo
modo_seguro_atr = 0.01  # ATR máximo para modo seguro (baja volatilidad)
riesgo_dinamico_reduccion = 0.5  # Reducir riesgo a la mitad tras pérdidas consecutivas
usar_kelly = False  # Activar position sizing basado en Kelly
kelly_fraction = 0.5  # Usar half-Kelly para reducir riesgo (0.5 = 50% de Kelly)
riesgo_max_kelly = 0.05  # Máximo riesgo por operación con Kelly (5%)
# ===============================

def api_call_with_retry(func, *args, **kwargs):
    """Ejecuta una llamada a la API con reintentos en caso de errores de conexión"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (ConnectionError, Timeout) as e:
            if attempt < max_retries - 1:
                log_consola(f"Error de conexión en API call (intento {attempt+1}/{max_retries}): {e}, reintentando en 10 segundos...")
                time.sleep(10)
            else:
                log_consola(f"Error de conexión persistente en API call: {e}")
                raise e
        except Exception as e:
            # Para otros errores, no reintentar
            log_consola(f"Error en API call: {e}")
            raise e

client = Client(api_key, api_secret, requests_params={'timeout': 30})
client.API_URL = 'https://fapi.binance.com/fapi'  # FUTUROS

TELEGRAM_TOKEN = '8446826605:AAEzABJ6KXtB_5fh85B07eMlXuP-IE8UiHk' 
TELEGRAM_CHAT_ID = '1715798949'

# === Variables de control del bot ===
bot_activo = False
bot_thread = None
mensajes_consola = queue.Queue(maxsize=50)  # Cola para almacenar mensajes de consola
ultimo_mensaje_consola = "Bot no iniciado"
registro_lock = threading.Lock()  # Lock para proteger escritura del CSV
ultimo_tp = None  # Para almacenar el TP de la última operación
ultimo_sl = None  # Para almacenar el SL de la última operación
# Variables para gestión de riesgos
saldo_inicial = None  # Saldo inicial al iniciar el bot
drawdown_actual = 0.0  # Drawdown actual
# ===================================

def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": mensaje}
    try:
        requests.post(url, data=data)
    except Exception as e:
        log_consola(f"❌ Error enviando notificación Telegram: {e}")

def log_consola(mensaje):
    """Registra mensajes en la consola y los almacena para consulta"""
    global ultimo_mensaje_consola
    timestamp = datetime.now().strftime('%H:%M:%S')
    mensaje_completo = f"[{timestamp}] {mensaje}"
    print(mensaje_completo)
    ultimo_mensaje_consola = mensaje_completo
    
    # Agregar a la cola de mensajes (si está llena, remover el más antiguo)
    try:
        if mensajes_consola.full():
            mensajes_consola.get_nowait()
        mensajes_consola.put_nowait(mensaje_completo)
    except Exception:
        pass

def obtener_ultimos_mensajes(num_mensajes=10):
    """Obtiene los últimos mensajes de la consola"""
    mensajes = []
    temp_queue = queue.Queue()
    
    # Copiar mensajes de la cola
    while not mensajes_consola.empty():
        try:
            msg = mensajes_consola.get_nowait()
            mensajes.append(msg)
            temp_queue.put_nowait(msg)
        except Exception:
            break
    
    # Restaurar mensajes a la cola original
    while not temp_queue.empty():
        try:
            mensajes_consola.put_nowait(temp_queue.get_nowait())
        except Exception:
            break
    
    # Retornar los últimos N mensajes
    return mensajes[-num_mensajes:] if mensajes else [ultimo_mensaje_consola]

def procesar_comando_telegram(comando):
    """Procesa comandos recibidos por Telegram"""
    global bot_activo, bot_thread
    global symbol, intervalo, riesgo_pct, bb_length, bb_mult, atr_length, ma_trend_length, umbral_volatilidad, tp_multiplier, sl_multiplier, usar_ma_trend
    global drawdown_max_pct, modo_seguro_atr, riesgo_dinamico_reduccion, usar_kelly, kelly_fraction, riesgo_max_kelly

    comando = comando.lower().strip()

    if comando == "iniciar":
        if bot_activo:
            return "⚠️ El bot ya está ejecutándose."
        bot_activo = True
        bot_thread = threading.Thread(target=ejecutar_bot_trading, daemon=True)
        bot_thread.start()
        return "✅ Bot iniciado correctamente. Monitoreando mercado..."

    elif comando == "consultar":
        mensajes = obtener_ultimos_mensajes(5)
        if mensajes:
            respuesta = "📊 **Últimos mensajes de la consola:**\n\n"
            for msg in mensajes:
                respuesta += f"• {msg}\n"
            return respuesta
        else:
            return "📊 No hay mensajes recientes disponibles."

    elif comando == "finalizar":
        if not bot_activo:
            return "⚠️ El bot no está ejecutándose."
        bot_activo = False
        return "🛑 Bot detenido. Esperando confirmación..."

    elif comando == "estado":
        estado = "🟢 ACTIVO" if bot_activo else "🔴 DETENIDO"
        return (f"🤖 **Estado del Bot:** {estado}\n"
                f"📊 Símbolo: {symbol}\n"
                f"⏱️ Intervalo: {intervalo}\n"
                f"• Riesgo: {riesgo_pct}\n"
                f"• BB: {bb_length} / {bb_mult}\n"
                f"• ATR: {atr_length}\n"
                f"• MA Tendencia: {ma_trend_length} ({'ON' if usar_ma_trend else 'OFF'})\n"
                f"• Umbral ATR: {umbral_volatilidad}\n"
                f"• TP Mult: {tp_multiplier} | SL Mult: {sl_multiplier}\n"
                f"• Drawdown Máx: {drawdown_max_pct*100:.1f}%\n"
                f"• Modo Seguro ATR: {modo_seguro_atr}\n"
                f"• Reducción Riesgo Dinámico: {riesgo_dinamico_reduccion}\n"
                f"• Kelly: {'ON' if usar_kelly else 'OFF'} (Fracción: {kelly_fraction}, Máx: {riesgo_max_kelly*100:.1f}%)\n"
                "v27.12.25")

    elif comando == "configurar":
        return (
            "⚙️ **Configuración actual:**\n"
            f"• Símbolo: `{symbol}`\n"
            f"• Intervalo: `{intervalo}`\n"
            f"• Riesgo por operación: `{riesgo_pct}`\n"
            f"• Periodo BB: `{bb_length}`\n"
            f"• Desviación BB: `{bb_mult}`\n"
            f"• Periodo ATR: `{atr_length}`\n"
            f"• Periodo MA Tendencia: `{ma_trend_length}` ({'ON' if usar_ma_trend else 'OFF'})\n"
            f"• Umbral ATR: `{umbral_volatilidad}`\n"
            f"• TP Mult: `{tp_multiplier}` | SL Mult: `{sl_multiplier}`\n"
            f"• Drawdown Máx: `{drawdown_max_pct*100:.1f}%`\n"
            f"• Modo Seguro ATR: `{modo_seguro_atr}`\n"
            f"• Reducción Riesgo Dinámico: `{riesgo_dinamico_reduccion}`\n"
            f"• Kelly: `{'ON' if usar_kelly else 'OFF'}` (Fracción: `{kelly_fraction}`, Máx: `{riesgo_max_kelly*100:.1f}%`)\n\n"
            "Para cambiar un parámetro, escribe:\n"
            "`set parametro valor`\n"
            "Ejemplo: `set simbolo BTCUSDT`"
        )

    elif comando.startswith("set "):
        partes = comando.split()
        if len(partes) < 3:
            return "❌ Formato incorrecto. Usa: `set parametro valor`"
        param = partes[1]
        valor_raw = " ".join(partes[2:]).strip()
        try:
            if param == "simbolo":
                symbol = valor_raw.upper()
            elif param == "intervalo":
                intervalo = valor_raw
            elif param == "riesgo":
                riesgo_pct = float(valor_raw) / 100 if float(valor_raw) >= 1 else float(valor_raw)
            elif param == "bb":
                bb_length = int(valor_raw)
            elif param == "bbmult":
                bb_mult = float(valor_raw)
            elif param == "atr":
                atr_length = int(valor_raw)
            elif param == "ma":
                ma_trend_length = int(valor_raw)
            elif param == "umbral":
                umbral_volatilidad = float(valor_raw)
            elif param == "tp":
                tp_multiplier = float(valor_raw)
            elif param == "sl":
                sl_multiplier = float(valor_raw)
            elif param == "mafilter":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_ma_trend = True
                elif v in ("0", "false", "off", "no"):
                    usar_ma_trend = False
                else:
                    return "❌ Valor para mafilter no válido. Usa on/off o 1/0."
            elif param == "drawdownmax":
                drawdown_max_pct = float(valor_raw) / 100 if float(valor_raw) >= 1 else float(valor_raw)
            elif param == "modoseguroatr":
                modo_seguro_atr = float(valor_raw)
            elif param == "riesgodinamico":
                riesgo_dinamico_reduccion = float(valor_raw)
            elif param == "kelly":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_kelly = True
                elif v in ("0", "false", "off", "no"):
                    usar_kelly = False
                else:
                    return "❌ Valor para kelly no válido. Usa on/off o 1/0."
            elif param == "kellyfrac":
                kelly_fraction = float(valor_raw)
            elif param == "kellymax":
                riesgo_max_kelly = float(valor_raw) / 100 if float(valor_raw) >= 1 else float(valor_raw)
            else:
                return "❌ Parámetro no reconocido."
            return f"✅ Parámetro `{param}` actualizado a `{valor_raw}`."
        except Exception as e:
            return f"❌ Error al actualizar: {e}"

    elif comando.startswith("registro"):
        partes = comando.split()
        num = 5
        if len(partes) > 1 and partes[1].isdigit():
            num = int(partes[1])
        return obtener_resumen_operaciones(num)

    elif comando == "analizar":
        return analizar_operaciones()

    elif comando == "descargar_registro":
        archivo = 'registro_operaciones.csv'
        if not os.path.exists(archivo):
            return "❌ No hay registro de operaciones aún."
        enviar_archivo_telegram(archivo)
        return "📄 Registro enviado por Telegram."

    elif comando == "eliminar_registro":
        archivo = 'registro_operaciones.csv'
        if not os.path.exists(archivo):
            return "❌ No hay registro de operaciones para eliminar."
        try:
            os.remove(archivo)
            return "🗑️ Registro de operaciones eliminado correctamente."
        except Exception as e:
            return f"❌ Error al eliminar el registro: {e}"

    elif comando == "cancelar":
        return cancelar_operaciones(symbol)

    else:
        return """🤖 **Comandos disponibles:**

• `iniciar` - Inicia el bot de trading
• `consultar` - Muestra los últimos mensajes de la consola
• `finalizar` - Detiene el bot de trading
• `estado` - Muestra el estado actual del bot
• `configurar` - Muestra y permite cambiar la configuración
• `set parametro valor` - Cambia un parámetro de configuración
    Ejemplo: `set simbolo BTCUSDT`
• `registro` - Muestra las últimas 5 operaciones
• `registro 10` - Muestra las últimas 10 operaciones
• `analizar` - Muestra un resumen de resultados del registro
• `descargar_registro` - Descarga el registro de operaciones (CSV)
• `eliminar_registro` - Elimina el registro de operaciones
• `cancelar` - Cierra la posición abierta y cancela órdenes TP/SL pendientes
"""

def bot_telegram_control():
    """Bot de Telegram para controlar el bot de trading"""
    offset = 0
    
    while True:
        try:
            # Obtener actualizaciones de Telegram
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params = {"offset": offset, "timeout": 30}
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        offset = update["update_id"] + 1
                        
                        if "message" in update and "text" in update["message"]:
                            chat_id = update["message"]["chat"]["id"]
                            texto = update["message"]["text"]
                            
                            # Solo procesar mensajes del chat autorizado
                            if str(chat_id) == TELEGRAM_CHAT_ID:
                                respuesta = procesar_comando_telegram(texto)
                                enviar_telegram(respuesta)
            
            time.sleep(1)  # Pequeña pausa para no sobrecargar la API
            
        except Exception as e:
            log_consola(f"❌ Error en bot de Telegram: {e}")
            time.sleep(5)

def enviar_error_telegram(error, contexto=""):
    """Envía notificaciones de error a Telegram con detalles"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    mensaje = f"🚨 **ERROR EN BOT {symbol}** 🚨\n"
    mensaje += f"⏰ **Hora:** {timestamp}\n"
    mensaje += f"📊 **Símbolo:** {symbol}\n"
    if contexto:
        mensaje += f"📍 **Contexto:** {contexto}\n"
    mensaje += f"❌ **Error:** {str(error)}\n"
    mensaje += f"🔍 **Tipo:** {type(error).__name__}\n"
    
    # Obtener el traceback para más detalles
    tb = traceback.format_exc()
    if tb and tb != "NoneType: None\n":
        # Limitar el traceback para que no sea muy largo
        tb_lines = tb.split('\n')[:10]  # Primeras 10 líneas
        mensaje += f"📋 **Detalles:**\n```\n{chr(10).join(tb_lines)}\n```"
    
    try:
        enviar_telegram(mensaje)
    except Exception as e:
        print(f"❌ Error enviando notificación de error: {e}")

def manejar_excepcion(func):
    """Decorador para manejar excepciones y enviar notificaciones"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            enviar_error_telegram(e, f"Función: {func.__name__}")
            raise
    return wrapper

def obtener_datos(symbol, intervalo, limite=100):
    klines = api_call_with_retry(client.futures_klines, symbol=symbol, interval=intervalo, limit=limite)
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                       'close_time', 'quote_asset_volume', 'number_of_trades',
                                       'taker_buy_base', 'taker_buy_quote', 'ignore'])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    return df[['close', 'high', 'low']]

def calcular_senal(df, umbral=None):
    """
    Calcula la señal usando Bandas de Bollinger, ATR y (opcional) filtro MA de tendencia.
    """
    global bb_length, bb_mult, atr_length, umbral_volatilidad, usar_ma_trend, ma_trend_length

    if umbral is None:
        umbral = umbral_volatilidad

    df = df.copy()
    # Bandas BB
    df['ma_bb'] = df['close'].rolling(window=bb_length).mean()
    df['std'] = df['close'].rolling(window=bb_length).std()
    df['upper'] = df['ma_bb'] + bb_mult * df['std']
    df['lower'] = df['ma_bb'] - bb_mult * df['std']

    # ATR
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=atr_length).mean()

    if len(df) < max(bb_length, atr_length, ma_trend_length) + 1:
        return 'neutral'

    close_now = df['close'].iloc[-1]
    close_prev = df['close'].iloc[-2]
    upper_now = df['upper'].iloc[-1]
    upper_prev = df['upper'].iloc[-2]
    lower_now = df['lower'].iloc[-1]
    lower_prev = df['lower'].iloc[-2]
    atr_now = df['atr'].iloc[-1]

    filtro_volatilidad = (atr_now < umbral)

    # filtro MA de tendencia (opcional)
    if usar_ma_trend:
        ma_trend = df['close'].rolling(window=ma_trend_length).mean().iloc[-1]
        filtro_trend_long = close_now > ma_trend
        filtro_trend_short = close_now < ma_trend
    else:
        filtro_trend_long = filtro_trend_short = True

    if close_prev <= upper_prev and close_now > upper_now and filtro_volatilidad and filtro_trend_long:
        return 'long'
    elif close_prev >= lower_prev and close_now < lower_now and filtro_volatilidad and filtro_trend_short:
        return 'short'
    else:
        return 'neutral'

def calcular_atr(df, periodo=None):
    """
    Calcula el ATR y retorna el último valor.
    Si periodo es None usa la variable global atr_length.
    """
    global atr_length
    if periodo is None:
        periodo = atr_length

    df = df.copy()
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=periodo).mean()
    return float(df['atr'].iloc[-1]) if not df['atr'].isna().all() else None

def calcular_cantidad_riesgo(saldo_usdt, riesgo_pct, distancia_sl):
    riesgo_usdt = saldo_usdt * riesgo_pct
    if distancia_sl == 0:
        return 0
    cantidad = riesgo_usdt / distancia_sl
    return round(cantidad, 3)

def ejecutar_orden(senal, symbol, cantidad, reintentos=5, espera=1):
    """
    Ejecuta una orden de mercado y espera/reintenta para confirmar la apertura de la posición.
    Retorna (precio_entrada, cantidad_real) o (None, None) en fallo.
    """
    try:
        side = SIDE_BUY if senal == 'long' else SIDE_SELL
        try:
            orden = api_call_with_retry(client.futures_create_order,
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=cantidad
            )
        except Exception as e:
            log_consola(f"❌ Error al crear la orden de mercado: {e}")
            return None, None

        # Esperar y reintentar para asegurar que la posición se refleje
        for _ in range(reintentos):
            time.sleep(espera)
            try:
                info_pos = api_call_with_retry(client.futures_position_information, symbol=symbol)
            except Exception as e:
                log_consola(f"❌ Error consultando posición tras orden: {e}")
                continue

            if info_pos and float(info_pos[0]['positionAmt']) != 0:
                precio = float(info_pos[0]['entryPrice'])
                cantidad_actual = abs(float(info_pos[0]['positionAmt']))
                log_consola(f"✅ Operación {senal.upper()} ejecutada a {precio} (cantidad: {cantidad_actual})")
                return precio, cantidad_actual

        # Si no se abrió posición tras reintentos
        log_consola("❌ La orden fue enviada pero no se abrió posición. Puede ser por cantidad mínima o error de Binance.")
        return None, None

    except Exception as e:
        log_consola(f"❌ Error inesperado en ejecutar_orden: {e}")
        try:
            enviar_telegram(f"❌ Error inesperado en ejecutar_orden: {e}")
        except Exception:
            pass
        return None, None

def registrar_operacion(fecha, tipo, precio_entrada, cantidad, tp, sl, resultado=None, pnl=None, symbol=None):
    archivo = 'registro_operaciones.csv'  # Cambia el nombre si usas uno diferente por bot
    with registro_lock:
        existe = os.path.isfile(archivo)
        with open(archivo, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not existe:
                writer.writerow(['Fecha', 'Símbolo', 'Tipo', 'Precio Entrada', 'Cantidad', 'Take Profit', 'Stop Loss', 'Resultado', 'PnL'])
            writer.writerow([fecha, symbol, tipo, precio_entrada, cantidad, tp, sl, resultado if resultado else "", pnl if pnl is not None else ""])

def obtener_precisiones(symbol):
    info = api_call_with_retry(client.futures_exchange_info)
    cantidad_decimales = 3
    precio_decimales = 3
    for s in info['symbols']:
        if s['symbol'] == symbol:
            for f in s['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    cantidad_decimales = abs(int(np.log10(step_size)))
                if f['filterType'] == 'PRICE_FILTER':
                    tick_size = float(f['tickSize'])
                    precio_decimales = abs(int(np.log10(tick_size)))
    return cantidad_decimales, precio_decimales

def calcular_atr(df, periodo=14):
    """Calcula el ATR usando la fórmula estándar (True Range)"""
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    
    # True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['prev_close'])
    df['tr3'] = abs(df['low'] - df['prev_close'])
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    # ATR = Media móvil del True Range
    df['atr'] = df['tr'].rolling(window=periodo).mean()
    return df['atr'].iloc[-1]

def calcular_kelly_fraction():
    """
    Calcula la fracción Kelly basada en el registro de operaciones.
    Retorna la fracción Kelly o 0 si no hay suficientes datos.
    """
    archivo = 'registro_operaciones.csv'
    if not os.path.exists(archivo):
        return 0.0

    try:
        df = pd.read_csv(archivo)
        if df.empty or len(df) < 10:  # Mínimo 10 operaciones para estadísticas confiables
            return 0.0

        # Filtrar operaciones con resultado válido
        df_valid = df[df['Resultado'].isin(['TP', 'SL'])]

        if len(df_valid) < 10:
            return 0.0

        # Calcular win rate (p)
        ganadoras = df_valid['Resultado'].eq('TP').sum()
        total = len(df_valid)
        p = ganadoras / total
        q = 1 - p

        # Calcular ratio b (promedio ganancia / promedio pérdida)
        ganancias = df_valid[df_valid['Resultado'] == 'TP']['PnL']
        perdidas = df_valid[df_valid['Resultado'] == 'SL']['PnL'].abs()  # Abs para pérdidas positivas

        if ganancias.empty or perdidas.empty:
            return 0.0

        avg_win = ganancias.mean()
        avg_loss = perdidas.mean()

        if avg_loss == 0:
            return 0.0

        b = avg_win / avg_loss

        # Fórmula Kelly
        kelly = (b * p - q) / b if b > 0 else 0.0

        # Aplicar fracción (half-Kelly por defecto)
        kelly_ajustado = kelly * kelly_fraction

        # Limitar a riesgo máximo
        return min(kelly_ajustado, riesgo_max_kelly)

    except Exception as e:
        log_consola(f"❌ Error calculando Kelly: {e}")
        return 0.0

# ============ FUNCIÓN PRINCIPAL DEL BOT ============
    """
    Retorna (pnl, precio_ejecucion, trade_time) del trade de cierre más probable.
    - tiempo_minimo: timestamp (segundos) para filtrar trades posteriores.
    - espera_segundos: tiempo a esperar antes de consultar para que Binance procese el trade.
    """
    try:
        time.sleep(espera_segundos)
        trades = api_call_with_retry(client.futures_account_trades, symbol=symbol)
        if not trades:
            return None, None, None
        # Filtrar trades con realizedPnl distinto de 0
        trades_cierre = [t for t in trades if float(t.get('realizedPnl', 0)) != 0]
        if tiempo_minimo:
            trades_cierre = [t for t in trades_cierre if int(t['time'])/1000 > tiempo_minimo]
        if not trades_cierre:
            return None, None, None
        ultimo_trade = trades_cierre[-1]
        pnl = float(ultimo_trade.get('realizedPnl', 0))
        precio = float(ultimo_trade.get('price', 0))
        trade_time = int(ultimo_trade['time'])/1000
        # No enviar notificación aquí si la lógica de notificación la maneja el ciclo principal
        return pnl, precio, trade_time
    except Exception as e:
        log_consola(f"❌ Error obteniendo PnL: {e}")
        return None, None, None

# ============ FUNCIÓN PRINCIPAL DEL BOT ============
def ejecutar_bot_trading():
    """Función principal del bot de trading que se ejecuta en un hilo separado"""
    global bot_activo, saldo_inicial, drawdown_actual

    ultima_posicion_cerrada = True
    datos_ultima_operacion = {}
    hubo_posicion_abierta = False
    tiempo_ultima_apertura = None
    ultimo_tp = None
    ultimo_sl = None
    perdidas_consecutivas = 0  # Al inicio de ejecutar_bot_trading

    # Obtener saldo inicial y resetear drawdown
    try:
        balance = api_call_with_retry(client.futures_account_balance)
        saldo_inicial = next((float(b['balance']) for b in balance if b['asset'] == 'USDT'), 0)
        drawdown_actual = 0.0
        log_consola(f"Saldo inicial: {saldo_inicial} USDT")
    except Exception as e:
        log_consola(f"❌ Error obteniendo saldo inicial: {e}")
        saldo_inicial = None

    # Notificar inicio del bot
    enviar_telegram(f"🤖 **Bot {symbol} iniciado**\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n📊 Símbolo: {symbol}\n⏱️ Intervalo: {intervalo}")
    log_consola("Bot de trading iniciado")

    while bot_activo:
        try:
            df = obtener_datos(symbol, intervalo)

            if len(df) < 51:
                log_consola("⏳ Esperando más datos...")
                time.sleep(60)
                continue

            precio_actual = df['close'].iloc[-1]

            info_pos = api_call_with_retry(client.futures_position_information, symbol=symbol)
            if not info_pos:
                log_consola("Sin posición abierta.")
                pos_abierta = 0.0
            else:
                posicion = info_pos[0]
                pos_abierta = float(posicion['positionAmt'])
                if pos_abierta != 0:
                    log_consola(f"Posición actual: cantidad={posicion['positionAmt']}, precio entrada={posicion['entryPrice']}, PnL={posicion['unRealizedProfit']}")
                else:
                    log_consola("Sin posición abierta.")

            # --- CHECK FOR EXIT IF POSITION OPEN ---
            if pos_abierta != 0 and ultimo_tp is not None and ultimo_sl is not None:
                cerrar_posicion = False
                if pos_abierta > 0:  # long
                    if precio_actual >= ultimo_tp:
                        resultado_cierre = "TP"
                        cerrar_posicion = True
                    elif precio_actual <= ultimo_sl:
                        resultado_cierre = "SL"
                        cerrar_posicion = True
                elif pos_abierta < 0:  # short
                    if precio_actual <= ultimo_tp:
                        resultado_cierre = "TP"
                        cerrar_posicion = True
                    elif precio_actual >= ultimo_sl:
                        resultado_cierre = "SL"
                        cerrar_posicion = True

                if cerrar_posicion:
                    try:
                        side_cierre = 'SELL' if pos_abierta > 0 else 'BUY'
                        order = api_call_with_retry(client.futures_create_order,
                            symbol=symbol,
                            side=side_cierre,
                            type='MARKET',
                            quantity=abs(pos_abierta),
                            reduceOnly=True
                        )
                        log_consola(f"🚪 Cerrando posición por {resultado_cierre}: precio {precio_actual:.4f}")
                        enviar_telegram(f"🚪 Posición cerrada en {symbol} por {resultado_cierre} a precio {precio_actual:.4f}")
                        # Reset for closure processing
                        ultima_posicion_cerrada = False
                        tiempo_ultima_apertura = time.time()
                    except Exception as e:
                        log_consola(f"❌ Error al cerrar posición: {e}")
                        enviar_error_telegram(e, "Cerrar posición")
                    continue  # Skip to next iteration to process closure

            # --- 1. PROCESAR CIERRE SI HAY UNO PENDIENTE ---
            tiempo_actual = time.time()
            if (pos_abierta == 0 and 
                not ultima_posicion_cerrada and 
                datos_ultima_operacion and 
                hubo_posicion_abierta and
                tiempo_ultima_apertura and
                (tiempo_actual - tiempo_ultima_apertura) > 10):

                time.sleep(8)  # Aumenta el delay si es necesario
                trades = api_call_with_retry(client.futures_account_trades, symbol=symbol)
                # Filtra solo los trades de cierre reales
                trades_cierre = [t for t in trades if float(t.get('realizedPnl', 0)) != 0 and int(t['time'])/1000 > tiempo_ultima_apertura]
                if trades_cierre:
                    ultimo_trade = trades_cierre[-1]
                    pnl = float(ultimo_trade.get('realizedPnl', 0))
                    precio_ejecucion = float(ultimo_trade['price'])
                    tp = datos_ultima_operacion["tp"]
                    sl = datos_ultima_operacion["sl"]
                    senal_original = datos_ultima_operacion["senal"]

                    trade_time = int(ultimo_trade['time']) / 1000
                    if trade_time > tiempo_ultima_apertura:
                        precio_entrada = datos_ultima_operacion["precio_entrada"]
                        if pnl > 0:
                            resultado = "TP"
                            enviar_telegram(f"🎉 ¡Take Profit alcanzado en {symbol}! Ganancia: {pnl:.4f} USDT")
                        elif pnl < 0:
                            resultado = "SL"
                            enviar_telegram(f"⚠️ Stop Loss alcanzado en {symbol}. Pérdida: {pnl:.4f} USDT")
                        else:
                            resultado = "NEUTRAL"
                            enviar_telegram(f"🔔 Posición cerrada en {symbol}. PnL: {pnl:.4f} USDT")
                        log_consola(f"📊 Detalles del cierre: Precio entrada={precio_entrada:.4f}, Precio ejecución={precio_ejecucion:.4f}, {resultado}")
                    else:
                        resultado = ""
                        pnl = None
                        log_consola("⚠️ Trade detectado no corresponde a la posición actual")
                else:
                    # Calcular PnL aproximadamente con el precio actual
                    precio_actual = df['close'].iloc[-1]
                    precio_entrada = datos_ultima_operacion["precio_entrada"]
                    cantidad = datos_ultima_operacion["cantidad_real"]
                    senal_original = datos_ultima_operacion["senal"]
                    if senal_original == 'long':
                        pnl = (precio_actual - precio_entrada) * cantidad
                    else:
                        pnl = (precio_entrada - precio_actual) * cantidad
                    precio_ejecucion = precio_actual
                    if pnl > 0:
                        resultado = "TP"
                        enviar_telegram(f"🎉 ¡Take Profit alcanzado en {symbol}! Ganancia aproximada: {pnl:.4f} USDT")
                    elif pnl < 0:
                        resultado = "SL"
                        enviar_telegram(f"⚠️ Stop Loss alcanzado en {symbol}. Pérdida aproximada: {pnl:.4f} USDT")
                    else:
                        resultado = "NEUTRAL"
                        enviar_telegram(f"🔔 Posición cerrada en {symbol}. PnL aproximado: {pnl:.4f} USDT")
                    log_consola(f"⚠️ No se encontró trade de cierre, PnL calculado: {pnl:.4f}")

                if resultado == "SL":
                    perdidas_consecutivas += 1
                else:
                    perdidas_consecutivas = 0

                if perdidas_consecutivas >= 3:
                    # Registrar la última operación ANTES de detener el bot
                    registrar_operacion(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datos_ultima_operacion["senal"],
                        datos_ultima_operacion["precio_entrada"],
                        datos_ultima_operacion["cantidad_real"],
                        datos_ultima_operacion["tp"],
                        datos_ultima_operacion["sl"],
                        resultado=resultado,
                        pnl=pnl,
                        symbol=symbol
                    )

                    # Intentar cancelar TODAS las órdenes pendientes para este símbolo (TP/SL u otras)
                    try:
                        ordenes_abiertas = api_call_with_retry(client.futures_get_open_orders, symbol=symbol)
                        canceladas = 0
                        for orden in ordenes_abiertas:
                            try:
                                api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=orden['orderId'])
                                canceladas += 1
                            except Exception as e:
                                log_consola(f"❌ Error al cancelar orden {orden.get('orderId')}: {e}")
                        if canceladas > 0:
                            log_consola(f"🗑️ {canceladas} órdenes pendientes canceladas antes de detener el bot.")
                            try:
                                enviar_telegram(f"🗑️ {canceladas} órdenes pendientes canceladas en {symbol} antes de detener el bot.")
                            except Exception:
                                pass
                        else:
                            log_consola("ℹ️ No había órdenes pendientes para cancelar.")
                    except Exception as e:
                        log_consola(f"❌ Error consultando/cancelando órdenes pendientes: {e}")
                        try:
                            enviar_telegram(f"❌ Error cancelando órdenes pendientes en {symbol}: {e}")
                        except Exception:
                            pass

                    enviar_telegram(f"⚠️ Bot {symbol} detenido tras 3 pérdidas consecutivas. Revisión sugerida")
                    log_consola(f"⚠️ Bot {symbol} detenido tras 3 pérdidas consecutivas.")
                    # limpiar estados y detener
                    ultima_posicion_cerrada = True
                    datos_ultima_operacion = {}
                    hubo_posicion_abierta = False
                    tiempo_ultima_apertura = None
                    ultimo_tp = None
                    ultimo_sl = None
                    bot_activo = False
                    break
                else:
                    registrar_operacion(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datos_ultima_operacion["senal"],
                        datos_ultima_operacion["precio_entrada"],
                        datos_ultima_operacion["cantidad_real"],
                        datos_ultima_operacion["tp"],
                        datos_ultima_operacion["sl"],
                        resultado=resultado,
                        pnl=pnl,
                        symbol=symbol
                    )
                ultima_posicion_cerrada = True
                datos_ultima_operacion = {}
                hubo_posicion_abierta = False
                tiempo_ultima_apertura = None
                ultimo_tp = None
                ultimo_sl = None

            # --- 2. SOLO SI NO HAY CIERRE PENDIENTE, PROCESA NUEVA SEÑAL ---
            senal = calcular_senal(df)
            log_consola(f"Señal detectada: {senal.upper()}")

            # Evitar duplicar posiciones en la misma dirección
            if (senal == 'long' and pos_abierta > 0) or (senal == 'short' and pos_abierta < 0):
                log_consola("⚠️ Ya hay una posición abierta en la misma dirección. No se ejecuta nueva orden.")
                time.sleep(60)
                continue

            if senal in ['long', 'short'] and pos_abierta == 0:
                # Calcular drawdown y verificar límite
                if saldo_inicial:
                    balance = api_call_with_retry(client.futures_account_balance)
                    saldo_actual = next((float(b['balance']) for b in balance if b['asset'] == 'USDT'), 0)
                    drawdown_actual = (saldo_inicial - saldo_actual) / saldo_inicial
                    if drawdown_actual > drawdown_max_pct:
                        log_consola(f"⚠️ Drawdown máximo alcanzado ({drawdown_actual*100:.2f}%), pausando bot.")
                        enviar_telegram(f"⚠️ Drawdown máximo alcanzado ({drawdown_actual*100:.2f}%), bot pausado.")
                        bot_activo = False
                        break

                atr = calcular_atr(df)
                if atr > umbral_volatilidad:
                    log_consola("Mercado demasiado volátil, no se opera.")
                    time.sleep(60)
                    continue

                # Modo seguro: operar solo en baja volatilidad
                if atr > modo_seguro_atr:
                    log_consola(f"Modo seguro activado: ATR {atr:.4f} > {modo_seguro_atr}, no se opera.")
                    time.sleep(60)
                    continue

                balance = api_call_with_retry(client.futures_account_balance)
                saldo_usdt = next((float(b['balance']) for b in balance if b['asset'] == 'USDT'), 0)

                precio_actual = float(df['close'].iloc[-1])
                atr = df['atr'].iloc[-1]

                # Riesgo dinámico: reducir si hay pérdidas consecutivas
                riesgo_actual = riesgo_pct
                if perdidas_consecutivas > 0:
                    riesgo_actual *= riesgo_dinamico_reduccion
                    log_consola(f"Riesgo dinámico reducido a {riesgo_actual*100:.2f}% por {perdidas_consecutivas} pérdidas consecutivas.")

                # Aplicar Kelly si está activado
                if usar_kelly:
                    kelly_calc = calcular_kelly_fraction()
                    if kelly_calc > 0:
                        riesgo_actual = min(riesgo_actual, kelly_calc)
                        log_consola(f"Kelly aplicado: riesgo ajustado a {riesgo_actual*100:.2f}%")
                    else:
                        log_consola("Kelly no aplicado: insuficientes datos o cálculo inválido.")

                if senal == 'long':
                    sl = precio_actual - atr * sl_multiplier
                    tp = precio_actual + atr * tp_multiplier
                    distancia_sl = atr * sl_multiplier
                else:
                    sl = precio_actual + atr * sl_multiplier
                    tp = precio_actual - atr * tp_multiplier
                    distancia_sl = atr * sl_multiplier

                cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
                cantidad = calcular_cantidad_riesgo(saldo_usdt, riesgo_actual, distancia_sl)
                cantidad = round(cantidad, cantidad_decimales)
                sl = round(sl, precio_decimales)
                tp = round(tp, precio_decimales)

                notional = precio_actual * cantidad
                if notional < 5:
                    cantidad_minima = round(5 / precio_actual, cantidad_decimales)
                    log_consola(f"⚠️ Ajustando cantidad al mínimo permitido: {cantidad_minima} contratos ({5:.2f} USDT)")
                    cantidad = cantidad_minima
                    notional = precio_actual * cantidad

                if notional < 5:
                    log_consola(f"⚠️ Orden rechazada: el valor notional ({notional:.2f} USDT) sigue siendo menor al mínimo permitido por Binance (5 USDT).")
                    continue

                log_consola(f"💰 Saldo disponible: {saldo_usdt} USDT | Usando {cantidad} contratos para la operación ({riesgo_actual*100:.1f}% de riesgo, SL={sl:.4f}, TP={tp:.4f})")

                precio_entrada, cantidad_real = ejecutar_orden(senal, symbol, cantidad)

                if precio_entrada:
                    ultima_posicion_cerrada = False
                    hubo_posicion_abierta = True
                    tiempo_ultima_apertura = time.time()
                    datos_ultima_operacion = {
                        "senal": senal,
                        "precio_entrada": precio_entrada,
                        "cantidad_real": cantidad_real,
                        "tp": tp,
                        "sl": sl
                    }
                    ultimo_tp = tp
                    ultimo_sl = sl

                    log_consola(f"✅ Orden {senal.upper()} ejecutada correctamente.")
                    log_consola(f"🎯 Take Profit: {tp:.4f} | 🛑 Stop Loss: {sl:.4f}")
                    enviar_telegram(f"✅ Orden {senal.upper()} ejecutada a {precio_entrada}.\nTP: {tp} | SL: {sl}")
                else:
                    log_consola(f"❌ No se pudo ejecutar la orden {senal.upper()}.")

            time.sleep(60)

        except Exception as e:
            error_msg = f"🚨 **ERROR CRÍTICO EN BOT {symbol}** 🚨\n"
            error_msg += f"⏰ **Hora:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            error_msg += f"❌ **Error:** {str(e)}\n"
            error_msg += f"🔍 **Tipo:** {type(e).__name__}\n"
            error_msg += f"📋 **Traceback:**\n```\n{traceback.format_exc()[:500]}...\n```"

            log_consola(f"❌ Error crítico: {e}")
            print(traceback.format_exc())

            try:
                enviar_telegram(error_msg)
            except Exception as telegram_error:
                log_consola(f"❌ Error enviando notificación de error crítico: {telegram_error}")

            log_consola("🔄 Reintentando en 60 segundos...")
            time.sleep(60)
            continue

    enviar_telegram(f"🛑 **Bot {symbol} detenido**\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_consola("Bot de trading detenido")

def enviar_archivo_telegram(ruta_archivo, nombre_archivo=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    if not os.path.exists(ruta_archivo):
        enviar_telegram("❌ El archivo no existe.")
        return
    with open(ruta_archivo, "rb") as f:
        files = {"document": (nombre_archivo or os.path.basename(ruta_archivo), f)}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        try:
            response = requests.post(url, data=data, files=files)
            if response.status_code == 200:
                log_consola("✅ Registro enviado por Telegram.")
            else:
                log_consola(f"❌ Error enviando archivo: {response.text}")
        except Exception as e:
            log_consola(f"❌ Error enviando archivo por Telegram: {e}")

def obtener_resumen_operaciones(num=5):
    archivo = 'registro_operaciones.csv'
    if not os.path.exists(archivo):
        return "❌ No hay registro de operaciones aún."
    try:
        df = pd.read_csv(archivo)
        if df.empty:
            return "❌ El registro de operaciones está vacío."
        ultimas = df.tail(num)
        resumen = "📋 **Últimas operaciones:**\n"
        for _, row in ultimas.iterrows():
            resumen += (f"{row['Fecha']} | {row['Símbolo']} | {row['Tipo']} | Entrada: {row['Precio Entrada']} | "
                        f"TP: {row['Take Profit']} | SL: {row['Stop Loss']} | "
                        f"Resultado: {row['Resultado']} | PnL: {row['PnL']}\n")
        return resumen
    except Exception as e:
        return f"❌ Error leyendo el registro: {e}"

def analizar_operaciones():
    archivo = 'registro_operaciones.csv'
    if not os.path.exists(archivo):
        return "❌ No hay registro de operaciones aún."
    try:
        df = pd.read_csv(archivo)
        if df.empty:
            return "❌ El registro de operaciones está vacío."
        total = len(df)
        ganadoras = df['Resultado'].str.upper().eq('TP').sum()
        perdedoras = df['Resultado'].str.upper().eq('SL').sum()
        pnl_total = pd.to_numeric(df['PnL'], errors='coerce').sum()
        resumen = (
            f"📊 **Análisis de Operaciones:**\n"
            f"• Total: {total}\n"
            f"• Ganadoras (TP): {ganadoras}\n"
            f"• Perdedoras (SL): {perdedoras}\n"
            f"• PnL total: {pnl_total:.4f} USDT"
        )
        return resumen
    except Exception as e:
        return f"❌ Error analizando el registro: {e}"

def cancelar_operaciones(symbol):
    """Cancela la posición abierta y todas las órdenes TP/SL pendientes"""
    mensajes = []
    # 1. Cerrar posición abierta
    info_pos = api_call_with_retry(client.futures_position_information, symbol=symbol)
    if info_pos and float(info_pos[0]['positionAmt']) != 0:
        position_amt = float(info_pos[0]['positionAmt'])
        cantidad = abs(position_amt)
        tipo_pos = "long" if position_amt > 0 else "short"
        side = SIDE_SELL if position_amt > 0 else SIDE_BUY
        try:
            # Ejecutar cierre de mercado
            api_call_with_retry(client.futures_create_order,
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=cantidad,
                reduceOnly=True
            )
            mensajes.append("✅ Posición cerrada correctamente.")
        except Exception as e:
            mensajes.append(f"❌ Error al cerrar posición: {e}")
            # continuar para intentar cancelar órdenes pendientes
            cantidad = None

        # Intentar obtener PnL del trade de cierre y registrar la operación
        try:
            if cantidad:
                time.sleep(6)  # esperar a que Binance registre el trade
                trades = api_call_with_retry(client.futures_account_trades, symbol=symbol)
                # Filtrar trades con realizedPnl distinto de 0 (trades de cierre)
                trades_cierre = [t for t in trades if float(t.get('realizedPnl', 0)) != 0]
                if trades_cierre:
                    ultimo = trades_cierre[-1]
                    pnl = float(ultimo.get('realizedPnl', 0))
                    precio_ejecucion = float(ultimo.get('price', 0))
                    resultado = "TP" if pnl > 0 else "SL" if pnl < 0 else "NEUTRAL"
                    # Registrar operación (no siempre se dispone del precio de entrada aquí)
                    registrar_operacion(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        tipo_pos,                   # tipo (long/short)
                        "",                         # precio_entrada (no disponible aquí)
                        cantidad,
                        "",                         # tp (no aplica)
                        "",                         # sl (no aplica)
                        resultado=resultado,
                        pnl=pnl,
                        symbol=symbol
                    )
                    mensajes.append(f"🔔 Registro creado: Resultado {resultado}, PnL {pnl:.4f} USDT")
                else:
                    mensajes.append("⚠️ No se encontró trade de cierre para obtener PnL.")
        except Exception as e:
            mensajes.append(f"❌ Error obteniendo PnL tras cierre: {e}")
    else:
        mensajes.append("ℹ️ No hay posición abierta para cerrar.")

    # 2. Cancelar órdenes TP/SL pendientes
    try:
        ordenes_abiertas = api_call_with_retry(client.futures_get_open_orders, symbol=symbol)
        canceladas = 0
        for orden in ordenes_abiertas:
            if orden['type'] in ['STOP_MARKET', 'TAKE_PROFIT_MARKET']:
                try:
                    api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=orden['orderId'])
                    canceladas += 1
                except Exception as e:
                    mensajes.append(f"❌ Error al cancelar orden {orden['type']}: {e}")
        if canceladas > 0:
            mensajes.append(f"🗑️ {canceladas} órdenes TP/SL canceladas.")
        else:
            mensajes.append("ℹ️ No había órdenes TP/SL pendientes.")
    except Exception as e:
        mensajes.append(f"❌ Error consultando/cancelando órdenes pendientes: {e}")

    return "\n".join(mensajes)

# ============ INICIO DEL PROGRAMA ============
if __name__ == "__main__":
    print("🤖 Bot de Control iniciado")
    print("📱 Envía comandos por Telegram:")
    print("   • 'iniciar' - Inicia el bot de trading")
    print("   • 'consultar' - Muestra los últimos mensajes")
    print("   • 'finalizar' - Detiene el bot de trading")
    print("   • 'estado' - Muestra el estado actual")
    print(f"   • 'mafilter' - Filtro MA tendencia: {'ON' if usar_ma_trend else 'OFF'} (usa: set mafilter on/off)")
    
    # Iniciar el bot de control de Telegram
    bot_telegram_control()
