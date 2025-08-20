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

# ======== CONFIGURACIÓN ========
api_key = 'Lw3sQdyAZcEJ2s522igX6E28ZL629ZL5JJ9UaqLyM7PXeNRLDu30LmPYFNJ4ixAx'
api_secret = 'Adw4DXL2BI9oS4sCJlS3dlBeoJQo6iPezmykfL1bhhm0NQe7aTHpaWULLQ0dYOIt'
symbol = 'RUNEUSDT'
intervalo = '15m'
riesgo_pct = 0.03  # 3% de riesgo por operación

# === Parámetros de la estrategia adaptada de TradingView ===
bb_length = 21  # Periodo Bandas Bollinger
bb_mult = 2.4   # Desviación Bandas Bollinger
atr_length = 16  # Periodo ATR
ma_trend_length = 50  # Periodo MA Tendencia
umbral_volatilidad = 0.04  # Umbral ATR (filtro volatilidad)

# Multiplicadores TP/SL ajustados según la estrategia
tp_multiplier = 2.2  # Take Profit multiplicador
sl_multiplier = 1.5  # Stop Loss multiplicador
# ===============================

client = Client(api_key, api_secret)
client.API_URL = 'https://fapi.binance.com/fapi'  # FUTUROS

TELEGRAM_TOKEN = '8446826605:AAEzABJ6KXtB_5fh85B07eMlXuP-IE8UiHk'
TELEGRAM_CHAT_ID = '1715798949'

# === Variables de control del bot ===
bot_activo = False
bot_thread = None
mensajes_consola = queue.Queue(maxsize=50)  # Cola para almacenar mensajes de consola
ultimo_mensaje_consola = "Bot no iniciado"
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
    except:
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
        except:
            break
    
    # Restaurar mensajes a la cola original
    while not temp_queue.empty():
        try:
            mensajes_consola.put_nowait(temp_queue.get_nowait())
        except:
            break
    
    # Retornar los últimos N mensajes
    return mensajes[-num_mensajes:] if mensajes else [ultimo_mensaje_consola]

def procesar_comando_telegram(comando):
    """Procesa comandos recibidos por Telegram"""
    global bot_activo, bot_thread
    global symbol, intervalo, riesgo_pct, bb_length, bb_mult, atr_length, ma_trend_length, umbral_volatilidad, tp_multiplier, sl_multiplier

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
                f"• MA Tendencia: {ma_trend_length}\n"
                f"• Umbral ATR: {umbral_volatilidad}\n"
                f"• TP Mult: {tp_multiplier} | SL Mult: {sl_multiplier}")

    elif comando == "configurar":
        return (
            "⚙️ **Configuración actual:**\n"
            f"• Símbolo: `{symbol}`\n"
            f"• Intervalo: `{intervalo}`\n"
            f"• Riesgo por operación: `{riesgo_pct}`\n"
            f"• Periodo BB: `{bb_length}`\n"
            f"• Desviación BB: `{bb_mult}`\n"
            f"• Periodo ATR: `{atr_length}`\n"
            f"• Periodo MA Tendencia: `{ma_trend_length}`\n"
            f"• Umbral ATR: `{umbral_volatilidad}`\n"
            f"• TP Mult: `{tp_multiplier}` | SL Mult: `{sl_multiplier}`\n\n"
            "Para cambiar un parámetro, escribe:\n"
            "`set parametro valor`\n"
            "Ejemplo: `set simbolo BTCUSDT`"
        )

    elif comando.startswith("set "):
        partes = comando.split()
        if len(partes) < 3:
            return "❌ Formato incorrecto. Usa: `set parametro valor`"
        param = partes[1]
        valor = " ".join(partes[2:])
        try:
            if param == "simbolo":
                symbol = valor.upper()
            elif param == "intervalo":
                intervalo = valor
            elif param == "riesgo":
                riesgo_pct = float(valor)
            elif param == "bb":
                bb_length = int(valor)
            elif param == "bbmult":
                bb_mult = float(valor)
            elif param == "atr":
                atr_length = int(valor)
            elif param == "ma":
                ma_trend_length = int(valor)
            elif param == "umbral":
                umbral_volatilidad = float(valor)
            elif param == "tp":
                tp_multiplier = float(valor)
            elif param == "sl":
                sl_multiplier = float(valor)
            else:
                return "❌ Parámetro no reconocido."
            return f"✅ Parámetro `{param}` actualizado a `{valor}`."
        except Exception as e:
            return f"❌ Error al actualizar: {e}"

    else:
        return """🤖 **Comandos disponibles:**

• `iniciar` - Inicia el bot de trading
• `consultar` - Muestra los últimos mensajes de la consola
• `finalizar` - Detiene el bot de trading
• `estado` - Muestra el estado actual del bot
• `configurar` - Muestra y permite cambiar la configuración
• `set parametro valor` - Cambia un parámetro de configuración
    Ejemplo: `set simbolo BTCUSDT`
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
        log_consola(f"❌ Error enviando notificación de error: {e}")

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
    klines = client.futures_klines(symbol=symbol, interval=intervalo, limit=limite)
    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                       'close_time', 'quote_asset_volume', 'number_of_trades',
                                       'taker_buy_base', 'taker_buy_quote', 'ignore'])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    return df[['close', 'high', 'low']]

def calcular_senal(df, umbral_volatilidad=0.04):
    # === Indicadores según la estrategia de TradingView ===
    # Bandas de Bollinger con parámetros ajustados
    df['ma'] = df['close'].rolling(window=bb_length).mean()
    df['std'] = df['close'].rolling(window=bb_length).std()
    df['upper'] = df['ma'] + bb_mult * df['std']
    df['lower'] = df['ma'] - bb_mult * df['std']
    
    # ATR usando la fórmula estándar (True Range)
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = abs(df['high'] - df['prev_close'])
    df['tr3'] = abs(df['low'] - df['prev_close'])
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['atr'] = df['tr'].rolling(window=atr_length).mean()
    
    # Media móvil de tendencia
    df['ma_trend'] = df['close'].rolling(window=ma_trend_length).mean()
    
    if len(df) < max(bb_length, ma_trend_length) + 1:
        return 'neutral'
    
    # === Filtros ===
    close_now = df['close'].iloc[-1]
    close_prev = df['close'].iloc[-2]
    upper_now = df['upper'].iloc[-1]
    upper_prev = df['upper'].iloc[-2]
    lower_now = df['lower'].iloc[-1]
    lower_prev = df['lower'].iloc[-2]
    atr_now = df['atr'].iloc[-1]
    ma_trend_now = df['ma_trend'].iloc[-1]
    
    # Filtro de volatilidad
    filtro_volatilidad = atr_now < umbral_volatilidad
    
    # Filtros de tendencia
    filtro_long = close_now > ma_trend_now
    filtro_short = close_now < ma_trend_now
    
    # === Señales (cruce + filtro de tendencia) ===
    # Señal long: cruce arriba banda superior + filtro volatilidad + filtro tendencia alcista
    long_signal = (close_prev <= upper_prev and close_now > upper_now and 
                   filtro_volatilidad and filtro_long)
    
    # Señal short: cruce abajo banda inferior + filtro volatilidad + filtro tendencia bajista
    short_signal = (close_prev >= lower_prev and close_now < lower_now and 
                    filtro_volatilidad and filtro_short)
    
    if long_signal:
        return 'long'
    elif short_signal:
        return 'short'
    else:
        return 'neutral'

def calcular_cantidad_riesgo(saldo_usdt, riesgo_pct, distancia_sl):
    riesgo_usdt = saldo_usdt * riesgo_pct
    if distancia_sl == 0:
        return 0
    cantidad = riesgo_usdt / distancia_sl
    return round(cantidad, 3)

def ejecutar_orden(senal, symbol, cantidad):
    try:
        side = SIDE_BUY if senal == 'long' else SIDE_SELL
        try:
            orden = client.futures_create_order(
                symbol=symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=cantidad
            )
        except Exception as e:
            log_consola(f"❌ Error al crear la orden de mercado: {e}")
            return None, None

        # Verifica que la posición realmente se abrió
        info_pos = client.futures_position_information(symbol=symbol)
        if not info_pos or float(info_pos[0]['positionAmt']) == 0:
            log_consola("❌ La orden fue enviada pero no se abrió posición. Puede ser por cantidad mínima o error de Binance.")
            return None, None

        precio = float(info_pos[0]['entryPrice'])
        log_consola(f"✅ Operación {senal.upper()} ejecutada a {precio}")
        return precio, cantidad

    except Exception as e:
        log_consola(f"❌ Error inesperado: {e}")
        enviar_telegram(f"❌ Error inesperado: {e}")

def registrar_operacion(fecha, tipo, precio_entrada, cantidad, tp, sl, resultado=None, pnl=None, symbol=None):
    archivo = 'registro_operaciones_universal.csv'
    existe = os.path.isfile(archivo)
    with open(archivo, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not existe:
            writer.writerow(['Fecha', 'Símbolo', 'Tipo', 'Precio Entrada', 'Cantidad', 'Take Profit', 'Stop Loss', 'Resultado', 'PnL'])
        writer.writerow([fecha, symbol, tipo, precio_entrada, cantidad, tp, sl, resultado if resultado else "", pnl if pnl is not None else ""])

def obtener_precisiones(symbol):
    info = client.futures_exchange_info()
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

def notificar_pnl(symbol):
    trades = client.futures_account_trades(symbol=symbol)
    if trades:
        ultimo_trade = trades[-1]
        pnl = float(ultimo_trade.get('realizedPnl', 0))
        enviar_telegram(f"🔔 Posición cerrada en {symbol}. PnL: {pnl:.4f} USDT")
        return pnl
    else:
        enviar_telegram(f"🔔 Posición cerrada en {symbol}. No se pudo obtener el PnL.")
        return None

# ============ FUNCIÓN PRINCIPAL DEL BOT ============
def ejecutar_bot_trading():
    """Función principal del bot de trading que se ejecuta en un hilo separado"""
    global bot_activo
    
    ultima_posicion_cerrada = True
    datos_ultima_operacion = {}
    hubo_posicion_abierta = False
    tiempo_ultima_apertura = None

    # Notificar inicio del bot
    enviar_telegram(f"🤖 **Bot {symbol} - Estrategia ATOM Adaptada**\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n📊 Símbolo: {symbol}\n⏱️ Intervalo: {intervalo}\n🎯 TP: {tp_multiplier}×ATR | 🛑 SL: {sl_multiplier}×ATR\n📈 BB: {bb_length} períodos, {bb_mult}σ\n📊 MA Tendencia: {ma_trend_length} períodos")
    log_consola("Bot de trading iniciado")

    while bot_activo:
        try:
            df = obtener_datos(symbol, intervalo)

            if len(df) < 51:
                log_consola("⏳ Esperando más datos...")
                time.sleep(60)
                continue

            senal = calcular_senal(df)
            log_consola(f"Señal detectada: {senal.upper()}")

            info_pos = client.futures_position_information(symbol=symbol)
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

            # Cancelar órdenes TP/SL pendientes si no hay posición abierta
            if pos_abierta == 0:
                ordenes_abiertas = client.futures_get_open_orders(symbol=symbol)
                for orden in ordenes_abiertas:
                    if orden['type'] in ['STOP_MARKET', 'TAKE_PROFIT_MARKET']:
                        try:
                            client.futures_cancel_order(symbol=symbol, orderId=orden['orderId'])
                            log_consola(f"🗑️ Orden pendiente cancelada: {orden['type']}")
                        except Exception as e:
                            log_consola(f"❌ Error al cancelar orden pendiente: {e}")
                            enviar_error_telegram(e, "Cancelar orden pendiente")

            # Evitar duplicar posiciones en la misma dirección
            if (senal == 'long' and pos_abierta > 0) or (senal == 'short' and pos_abierta < 0):
                log_consola("⚠️ Ya hay una posición abierta en la misma dirección. No se ejecuta nueva orden.")
                time.sleep(60)
                continue

            # === Gestión dinámica y avanzada ===
            if senal in ['long', 'short'] and pos_abierta == 0:
                atr = calcular_atr(df)
                if atr > umbral_volatilidad:
                    log_consola("Mercado demasiado volátil, no se opera.")
                    time.sleep(60)
                    continue

                # Gestión de riesgo avanzada
                balance = client.futures_account_balance()
                saldo_usdt = next((float(b['balance']) for b in balance if b['asset'] == 'USDT'), 0)

                # Calcula distancia SL en precio (ajustable)
                precio_actual = float(df['close'].iloc[-1])
                # Calcula ATR para la última vela
                atr = df['atr'].iloc[-1]

                if senal == 'long':
                    sl = precio_actual - atr * sl_multiplier
                    tp = precio_actual + atr * tp_multiplier
                    distancia_sl = atr * sl_multiplier
                else:
                    sl = precio_actual + atr * sl_multiplier
                    tp = precio_actual - atr * tp_multiplier
                    distancia_sl = atr * sl_multiplier

                # Redondeo de precios y cantidad según precisión del símbolo
                cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
                cantidad = calcular_cantidad_riesgo(saldo_usdt, riesgo_pct, distancia_sl)
                cantidad = round(cantidad, cantidad_decimales)
                sl = round(sl, precio_decimales)
                tp = round(tp, precio_decimales)

                # Ajuste para cumplir el mínimo notional de Binance
                notional = precio_actual * cantidad
                if notional < 5:
                    cantidad_minima = round(5 / precio_actual, cantidad_decimales)
                    log_consola(f"⚠️ Ajustando cantidad al mínimo permitido: {cantidad_minima} contratos ({5:.2f} USDT)")
                    cantidad = cantidad_minima
                    notional = precio_actual * cantidad

                if notional < 5:
                    log_consola(f"⚠️ Orden rechazada: el valor notional ({notional:.2f} USDT) sigue siendo menor al mínimo permitido por Binance (5 USDT).")
                    continue

                log_consola(f"💰 Saldo disponible: {saldo_usdt} USDT | Usando {cantidad} contratos para la operación ({riesgo_pct*100:.1f}% de riesgo, SL={sl:.4f}, TP={tp:.4f})")

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
                    # Cancelar órdenes TP/SL abiertas antes de crear nuevas
                    ordenes_abiertas = client.futures_get_open_orders(symbol=symbol)
                    for orden in ordenes_abiertas:
                        if orden['type'] in ['STOP_MARKET', 'TAKE_PROFIT_MARKET']:
                            try:
                                client.futures_cancel_order(symbol=symbol, orderId=orden['orderId'])
                            except Exception as e:
                                log_consola(f"❌ Error al cancelar orden previa: {e}")
                                enviar_error_telegram(e, "Cancelar orden previa")

                    # Crear TP/SL según la dirección de la señal
                    try:
                        if senal == 'long':
                            client.futures_create_order(
                                symbol=symbol,
                                side='SELL',
                                type='TAKE_PROFIT_MARKET',
                                stopPrice=tp,
                                quantity=cantidad_real,
                                reduceOnly=True
                            )
                            client.futures_create_order(
                                symbol=symbol,
                                side='SELL',
                                type='STOP_MARKET',
                                stopPrice=sl,
                                quantity=cantidad_real,
                                reduceOnly=True
                            )
                        else:
                            client.futures_create_order(
                                symbol=symbol,
                                side='BUY',
                                type='TAKE_PROFIT_MARKET',
                                stopPrice=tp,
                                quantity=cantidad_real,
                                reduceOnly=True
                            )
                            client.futures_create_order(
                                symbol=symbol,
                                side='BUY',
                                type='STOP_MARKET',
                                stopPrice=sl,
                                quantity=cantidad_real,
                                reduceOnly=True
                            )
                    except Exception as e:
                        log_consola(f"❌ Error al crear TP/SL: {e}")
                        enviar_error_telegram(e, "Crear TP/SL")

                    log_consola(f"✅ Orden {senal.upper()} ejecutada correctamente.")
                    log_consola(f"🎯 Take Profit: {tp:.4f} | 🛑 Stop Loss: {sl:.4f}")
                    enviar_telegram(f"✅ Orden {senal.upper()} ejecutada a {precio_entrada}.\nTP: {tp} | SL: {sl}")
                else:
                    log_consola(f"❌ No se pudo ejecutar la orden {senal.upper()}.")

            # Verificar cierre de posición solo si ha pasado suficiente tiempo desde la apertura
            tiempo_actual = time.time()
            if (pos_abierta == 0 and 
                not ultima_posicion_cerrada and 
                datos_ultima_operacion and 
                hubo_posicion_abierta and
                tiempo_ultima_apertura and
                (tiempo_actual - tiempo_ultima_apertura) > 10):  # Esperar al menos 10 segundos
                
                # Espera unos segundos para que Binance registre el trade de cierre real
                time.sleep(5)
                trades = client.futures_account_trades(symbol=symbol)
                if trades:
                    ultimo_trade = trades[-1]
                    pnl = float(ultimo_trade.get('realizedPnl', 0))
                    precio_ejecucion = float(ultimo_trade['price'])
                    tp = datos_ultima_operacion["tp"]
                    sl = datos_ultima_operacion["sl"]
                    senal_original = datos_ultima_operacion["senal"]
                    
                    # Verificar que el trade realmente corresponde a la posición que abrimos
                    trade_time = int(ultimo_trade['time']) / 1000  # Convertir a segundos
                    if trade_time > tiempo_ultima_apertura:
                        # Lógica mejorada para determinar TP vs SL
                        # Para LONG: TP > precio_entrada, SL < precio_entrada
                        # Para SHORT: TP < precio_entrada, SL > precio_entrada
                        precio_entrada = datos_ultima_operacion["precio_entrada"]
                        
                        if senal_original == 'long':
                            # En posición LONG, si el precio de ejecución es mayor al precio de entrada, probablemente fue TP
                            if precio_ejecucion > precio_entrada:
                                resultado = "TP"
                                enviar_telegram(f"🎉 ¡Take Profit alcanzado en {symbol}! Ganancia: {pnl:.4f} USDT")
                            else:
                                resultado = "SL"
                                enviar_telegram(f"⚠️ Stop Loss alcanzado en {symbol}. Pérdida: {pnl:.4f} USDT")
                        else:  # senal_original == 'short'
                            # En posición SHORT, si el precio de ejecución es menor al precio de entrada, probablemente fue TP
                            if precio_ejecucion < precio_entrada:
                                resultado = "TP"
                                enviar_telegram(f"🎉 ¡Take Profit alcanzado en {symbol}! Ganancia: {pnl:.4f} USDT")
                            else:
                                resultado = "SL"
                                enviar_telegram(f"⚠️ Stop Loss alcanzado en {symbol}. Pérdida: {pnl:.4f} USDT")
                        
                        log_consola(f"📊 Detalles del cierre: Precio entrada={precio_entrada:.4f}, Precio ejecución={precio_ejecucion:.4f}, {resultado}")
                    else:
                        resultado = ""
                        pnl = None
                        log_consola("⚠️ Trade detectado no corresponde a la posición actual")
                else:
                    resultado = ""
                    pnl = None
                    enviar_telegram(f"🔔 Posición cerrada en {symbol}. No se pudo obtener el PnL.")

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

            time.sleep(60)  # Esperar antes de la siguiente verificación
            
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
    
    # Notificar que el bot se detuvo
    enviar_telegram(f"🛑 **Bot {symbol} - Estrategia ATOM detenido**\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_consola("Bot de trading detenido")

# ============ INICIO DEL PROGRAMA ============
if __name__ == "__main__":
    print("🤖 Bot de Control ATOMUSDT iniciado")
    print("📱 Envía comandos por Telegram:")
    print("   • 'iniciar' - Inicia el bot de trading")
    print("   • 'consultar' - Muestra los últimos mensajes")
    print("   • 'finalizar' - Detiene el bot de trading")
    print("   • 'estado' - Muestra el estado actual")
    
    # Iniciar el bot de control de Telegram
    bot_telegram_control()
