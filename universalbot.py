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

# Intentar cargar python-dotenv si está disponible
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Si no está instalado, continuar sin él (usará valores por defecto o variables de entorno del sistema)
    pass

# ======== CONFIGURACIÓN SEGURA (Variables de Entorno) ========
# Cargar desde variables de entorno, con valores por defecto como fallback
api_key = os.getenv('BINANCE_API_KEY', 'Lw3sQdyAZcEJ2s522igX6E28ZL629ZL5JJ9UaqLyM7PXeNRLDu30LmPYFNJ4ixAx')
api_secret = os.getenv('BINANCE_API_SECRET', 'Adw4DXL2BI9oS4sCJlS3dlBeoJQo6iPezmykfL1bhhm0NQe7aTHpaWULLQ0dYOIt')
symbol = os.getenv('SYMBOL', 'DOGEUSDT')
intervalo = os.getenv('INTERVALO', '30m')
riesgo_pct = float(os.getenv('RIESGO_PCT', '0.01'))  # 1% de riesgo por operación
umbral_volatilidad = float(os.getenv('UMBRAL_VOLATILIDAD', '0.02'))  # ATR máximo permitido para operar
bb_length = int(os.getenv('BB_LENGTH', '22'))  # Periodo por defecto para Bandas de Bollinger
bb_mult = float(os.getenv('BB_MULT', '3.3'))  # Multiplicador por defecto para Bandas de Bollinger
atr_length = int(os.getenv('ATR_LENGTH', '3'))  # Periodo por defecto para ATR
ma_trend_length = int(os.getenv('MA_TREND_LENGTH', '50'))  # Periodo por defecto para MA de tendencia
tp_multiplier = float(os.getenv('TP_MULTIPLIER', '3.6'))  # Multiplicador por defecto para Take Profit
sl_multiplier = float(os.getenv('SL_MULTIPLIER', '1.6'))  # Multiplicador por defecto para Stop Loss
usar_ma_trend = os.getenv('USAR_MA_TREND', 'True').lower() == 'true'  # Usar filtro MA de tendencia
# Nuevas configuraciones para gestión de riesgos
riesgo_dinamico_reduccion = float(os.getenv('RIESGO_DINAMICO_REDUCCION', '0.5'))  # Reducir riesgo a la mitad tras pérdidas consecutivas
usar_kelly = os.getenv('USAR_KELLY', 'False').lower() == 'true'  # Activar position sizing basado en Kelly
kelly_fraction = float(os.getenv('KELLY_FRACTION', '0.5'))  # Usar half-Kelly para reducir riesgo (0.5 = 50% de Kelly)
riesgo_max_kelly = float(os.getenv('RIESGO_MAX_KELLY', '0.05'))  # Máximo riesgo por operación con Kelly (5%)
# Nuevas configuraciones para indicadores adicionales
usar_rsi = os.getenv('USAR_RSI', 'False').lower() == 'true'  # Activar filtro RSI
rsi_length = int(os.getenv('RSI_LENGTH', '14'))  # Periodo para RSI
rsi_overbought = int(os.getenv('RSI_OVERBOUGHT', '70'))  # Nivel de sobrecompra
rsi_oversold = int(os.getenv('RSI_OVERSOLD', '30'))  # Nivel de sobreventa
usar_macd = os.getenv('USAR_MACD', 'False').lower() == 'true'  # Activar filtro MACD
macd_fast = int(os.getenv('MACD_FAST', '12'))  # Periodo rápido MACD
macd_slow = int(os.getenv('MACD_SLOW', '26'))  # Periodo lento MACD
macd_signal = int(os.getenv('MACD_SIGNAL', '9'))  # Periodo señal MACD
usar_volumen_filtro = os.getenv('USAR_VOLUMEN_FILTRO', 'False').lower() == 'true'  # Activar filtro de volumen
volumen_periodos = int(os.getenv('VOLUMEN_PERIODOS', '20'))  # Periodos para promedio de volumen
usar_multitimeframe = os.getenv('USAR_MULTITIMEFRAME', 'False').lower() == 'true'  # Activar confirmación multi-timeframe
timeframe_superior = os.getenv('TIMEFRAME_SUPERIOR', '1h')  # Timeframe superior para confirmación

# Configuraciones para nuevas funcionalidades
usar_trailing_stop = os.getenv('USAR_TRAILING_STOP', 'False').lower() == 'true'  # Activar trailing stop loss
trailing_stop_pct = float(os.getenv('TRAILING_STOP_PCT', '0.5'))  # Porcentaje para trailing stop
health_check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # Intervalo de health check en segundos (5 min)
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

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8446826605:AAEzABJ6KXtB_5fh85B07eMlXuP-IE8UiHk')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '1715798949')

# === Variables de control del bot ===
bot_activo = False
bot_thread = None
mensajes_consola = queue.Queue(maxsize=50)  # Cola para almacenar mensajes de consola
ultimo_mensaje_consola = "Bot no iniciado"
registro_lock = threading.Lock()  # Lock para proteger escritura del CSV
ultimo_tp = None  # Para almacenar el TP de la última operación
ultimo_sl = None  # Para almacenar el SL de la última operación
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
    global riesgo_dinamico_reduccion, usar_kelly, kelly_fraction, riesgo_max_kelly
    global usar_rsi, rsi_length, rsi_overbought, rsi_oversold, usar_macd, macd_fast, macd_slow, macd_signal
    global usar_volumen_filtro, volumen_periodos, usar_multitimeframe, timeframe_superior

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
                f"• Reducción Riesgo Dinámico: {riesgo_dinamico_reduccion}\n"
                f"• Kelly: {'ON' if usar_kelly else 'OFF'} (Fracción: {kelly_fraction}, Máx: {riesgo_max_kelly*100:.1f}%)\n"
                f"• RSI: {'ON' if usar_rsi else 'OFF'} ({rsi_length}/{rsi_overbought}/{rsi_oversold})\n"
                f"• MACD: {'ON' if usar_macd else 'OFF'} ({macd_fast}/{macd_slow}/{macd_signal})\n"
                f"• Volumen Filtro: {'ON' if usar_volumen_filtro else 'OFF'} ({volumen_periodos} períodos)\n"
                f"• Multi-Timeframe: {'ON' if usar_multitimeframe else 'OFF'} ({timeframe_superior})\n"
                "v03.01.26 (version mejorada)")

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
            f"• Reducción Riesgo Dinámico: `{riesgo_dinamico_reduccion}`\n"
            f"• Kelly: `{'ON' if usar_kelly else 'OFF'}` (Fracción: `{kelly_fraction}`, Máx: `{riesgo_max_kelly*100:.1f}%`)\n"
            f"• RSI: `{'ON' if usar_rsi else 'OFF'}` ({rsi_length}/{rsi_overbought}/{rsi_oversold})\n"
            f"• MACD: `{'ON' if usar_macd else 'OFF'}` ({macd_fast}/{macd_slow}/{macd_signal})\n"
            f"• Volumen Filtro: `{'ON' if usar_volumen_filtro else 'OFF'}` ({volumen_periodos} períodos)\n"
            f"• Multi-Timeframe: `{'ON' if usar_multitimeframe else 'OFF'}` ({timeframe_superior})\n\n"
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
            elif param == "rsi":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_rsi = True
                elif v in ("0", "false", "off", "no"):
                    usar_rsi = False
                else:
                    return "❌ Valor para rsi no válido. Usa on/off o 1/0."
            elif param == "rsilength":
                rsi_length = int(valor_raw)
            elif param == "rsioverbought":
                rsi_overbought = int(valor_raw)
            elif param == "rsioversold":
                rsi_oversold = int(valor_raw)
            elif param == "macd":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_macd = True
                elif v in ("0", "false", "off", "no"):
                    usar_macd = False
                else:
                    return "❌ Valor para macd no válido. Usa on/off o 1/0."
            elif param == "macdfast":
                macd_fast = int(valor_raw)
            elif param == "macdslow":
                macd_slow = int(valor_raw)
            elif param == "macdsignal":
                macd_signal = int(valor_raw)
            elif param == "volumenfiltro":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_volumen_filtro = True
                elif v in ("0", "false", "off", "no"):
                    usar_volumen_filtro = False
                else:
                    return "❌ Valor para volumenfiltro no válido. Usa on/off o 1/0."
            elif param == "volumenperiodos":
                volumen_periodos = int(valor_raw)
            elif param == "multitimeframe":
                v = valor_raw.lower()
                if v in ("1", "true", "on", "yes"):
                    usar_multitimeframe = True
                elif v in ("0", "false", "off", "no"):
                    usar_multitimeframe = False
                else:
                    return "❌ Valor para multitimeframe no válido. Usa on/off o 1/0."
            elif param == "timeframesuperior":
                timeframe_superior = valor_raw
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
    
    elif comando == "performance":
        return obtener_resumen_performance()
    
    elif comando == "health":
        ok, mensaje = verificar_estado_posicion(symbol)
        estado_emoji = "✅" if ok else "⚠️"
        return f"{estado_emoji} **Health Check:** {mensaje}"
    
    elif comando.startswith("backtest"):
        partes = comando.split()
        if len(partes) >= 2:
            try:
                dias = int(partes[1])
                fecha_inicio = datetime.now() - pd.Timedelta(days=dias)
                resultado = backtest_estrategia(symbol, intervalo, fecha_inicio)
                if "error" in resultado:
                    return f"❌ Error en backtest: {resultado['error']}"
                mensaje = f"📊 **Resultados del Backtest ({dias} días):**\n\n"
                mensaje += f"💰 Capital inicial: {resultado['capital_inicial']:.2f} USDT\n"
                mensaje += f"💰 Capital final: {resultado['capital_final']:.2f} USDT\n"
                mensaje += f"📈 ROI: {resultado['roi']:.2f}%\n\n"
                mensaje += f"📊 Operaciones: {resultado['total_operaciones']}\n"
                mensaje += f"✅ Ganadoras: {resultado['ganadoras']}\n"
                mensaje += f"❌ Perdedoras: {resultado['perdedoras']}\n"
                mensaje += f"🎯 Win Rate: {resultado['win_rate']:.2f}%\n"
                mensaje += f"💵 PnL Total: {resultado['pnl_total']:.4f} USDT"
                return mensaje
            except Exception as e:
                return f"❌ Error: {e}"
        else:
            return "❌ Uso: backtest <dias>\nEjemplo: backtest 30"

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
• `performance` - Análisis de performance en tiempo real
• `health` - Verifica el estado de las posiciones (Health Check)
• `backtest <dias>` - Ejecuta backtesting (ej: backtest 30)
• `descargar_registro` - Descarga el registro de operaciones (CSV)
• `eliminar_registro` - Elimina el registro de operaciones
• `cancelar` - Cierra la posición abierta y cancela órdenes TP/SL pendientes
"""

def bot_telegram_control():
    """Bot de Telegram para controlar el bot de trading con mejor manejo de errores"""
    offset = 0
    errores_consecutivos = 0
    max_errores_consecutivos = 5
    
    while True:
        try:
            # Obtener actualizaciones de Telegram
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            params = {"offset": offset, "timeout": 30}
            
            response = requests.get(url, params=params, timeout=35)
            if response.status_code == 200:
                data = response.json()
                errores_consecutivos = 0  # Resetear contador de errores
                
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        offset = update["update_id"] + 1
                        
                        if "message" in update and "text" in update["message"]:
                            chat_id = update["message"]["chat"]["id"]
                            texto = update["message"]["text"]
                            
                            # Solo procesar mensajes del chat autorizado
                            if str(chat_id) == TELEGRAM_CHAT_ID:
                                try:
                                    respuesta = procesar_comando_telegram(texto)
                                    enviar_telegram(respuesta)
                                except Exception as e:
                                    log_consola(f"❌ Error procesando comando Telegram: {e}")
                                    enviar_telegram(f"❌ Error procesando comando: {str(e)}")
            
            time.sleep(1)  # Pequeña pausa para no sobrecargar la API
            
        except requests.exceptions.Timeout:
            log_consola("⏰ Timeout en petición a Telegram API")
            time.sleep(5)
        except requests.exceptions.ConnectionError as e:
            errores_consecutivos += 1
            log_consola(f"🌐 Error de conexión Telegram ({errores_consecutivos}/{max_errores_consecutivos}): {e}")
            if errores_consecutivos >= max_errores_consecutivos:
                log_consola("🚨 Demasiados errores de conexión consecutivos. Reiniciando en 30 segundos...")
                enviar_telegram("🚨 Problemas de conexión con Telegram. Reiniciando bot...")
                time.sleep(30)
                # Forzar reinicio del programa
                os._exit(1)
            time.sleep(10)
        except Exception as e:
            errores_consecutivos += 1
            log_consola(f"❌ Error en bot de Telegram ({errores_consecutivos}/{max_errores_consecutivos}): {e}")
            if errores_consecutivos >= max_errores_consecutivos:
                log_consola("🚨 Demasiados errores consecutivos. Reiniciando...")
                time.sleep(30)
                os._exit(1)
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
    df['volume'] = df['volume'].astype(float)
    return df[['close', 'high', 'low', 'volume']]

def calcular_senal(df, umbral=None):
    """
    Calcula la señal usando Bandas de Bollinger, ATR, y filtros opcionales.
    
    Estrategia adaptada de TradingView Pine Script :
    - Señal LONG: crossover(price, upper_band) cuando ATR < umbral y precio > MA_trend (si está activo)
    - Señal SHORT: crossunder(price, lower_band) cuando ATR < umbral y precio < MA_trend (si está activo)
    - TP = ATR * tp_multiplier, SL = ATR * sl_multiplier
    """
    global bb_length, bb_mult, atr_length, umbral_volatilidad, usar_ma_trend, ma_trend_length
    global usar_rsi, rsi_length, rsi_overbought, rsi_oversold, usar_macd, macd_fast, macd_slow, macd_signal
    global usar_volumen_filtro, volumen_periodos, usar_multitimeframe, timeframe_superior, symbol, intervalo

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

    # RSI (opcional)
    if usar_rsi:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=rsi_length).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_length).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

    # MACD (opcional)
    if usar_macd:
        df['ema_fast'] = df['close'].ewm(span=macd_fast).mean()
        df['ema_slow'] = df['close'].ewm(span=macd_slow).mean()
        df['macd'] = df['ema_fast'] - df['ema_slow']
        df['macd_signal'] = df['macd'].ewm(span=macd_signal).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

    # Filtro de volumen (opcional)
    if usar_volumen_filtro and 'volume' in df.columns:
        df['volume'] = df['volume'].astype(float)
        df['volume_avg'] = df['volume'].rolling(window=volumen_periodos).mean()

    min_periods = max(bb_length, atr_length, ma_trend_length)
    if usar_rsi:
        min_periods = max(min_periods, rsi_length)
    if usar_macd:
        min_periods = max(min_periods, macd_slow + macd_signal)

    if len(df) < min_periods + 1:
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

    # Filtro RSI (opcional)
    filtro_rsi_long = filtro_rsi_short = True
    if usar_rsi:
        rsi_now = df['rsi'].iloc[-1]
        filtro_rsi_long = rsi_now < rsi_overbought  # No sobrecomprado para long
        filtro_rsi_short = rsi_now > rsi_oversold   # No sobrevendido para short

    # Filtro MACD (opcional)
    filtro_macd_long = filtro_macd_short = True
    if usar_macd:
        macd_now = df['macd'].iloc[-1]
        macd_signal_now = df['macd_signal'].iloc[-1]
        filtro_macd_long = macd_now > macd_signal_now  # MACD arriba de señal para long
        filtro_macd_short = macd_now < macd_signal_now # MACD abajo de señal para short

    # Filtro de volumen (opcional)
    filtro_volumen = True
    if usar_volumen_filtro and 'volume_avg' in df.columns:
        volume_now = df['volume'].iloc[-1]
        volume_avg = df['volume_avg'].iloc[-1]
        filtro_volumen = volume_now > volume_avg

    # Multi-timeframe (opcional)
    filtro_multitimeframe = True
    if usar_multitimeframe:
        try:
            # Obtener datos del timeframe superior
            df_superior = obtener_datos(symbol, timeframe_superior, limite=50)
            if len(df_superior) >= 10:
                senal_superior = calcular_senal(df_superior, umbral=umbral_volatilidad)
                filtro_multitimeframe = senal_superior in ['long', 'short']
        except Exception as e:
            log_consola(f"⚠️ Error en multi-timeframe: {e}")
            filtro_multitimeframe = True  # Si falla, permitir la señal

    # Detectar señales de crossover/crossunder (como en Pine Script)
    # Long: crossover(price, upper) - precio cruza por encima de la banda superior
    # Short: crossunder(price, lower) - precio cruza por debajo de la banda inferior
    crossover_long = close_prev <= upper_prev and close_now > upper_now
    crossunder_short = close_prev >= lower_prev and close_now < lower_now
    
    # Combinar todos los filtros (solo filtros básicos activos por defecto)
    # La estrategia DOGE solo usa: filtro_volatilidad y filtro_ma_trend
    if (crossover_long and
        filtro_volatilidad and filtro_trend_long and 
        (not usar_rsi or filtro_rsi_long) and
        (not usar_macd or filtro_macd_long) and
        (not usar_volumen_filtro or filtro_volumen) and
        (not usar_multitimeframe or filtro_multitimeframe)):
        return 'long'
    elif (crossunder_short and
          filtro_volatilidad and filtro_trend_short and
          (not usar_rsi or filtro_rsi_short) and
          (not usar_macd or filtro_macd_short) and
          (not usar_volumen_filtro or filtro_volumen) and
          (not usar_multitimeframe or filtro_multitimeframe)):
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

def crear_orden_oco(symbol, side, quantity, tp_price, sl_price):
    """
    Crea una orden OCO (One Cancels Other) para Take Profit y Stop Loss.
    """
    try:
        cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
        tp_price_rounded = round(tp_price, precio_decimales)
        sl_price_rounded = round(sl_price, precio_decimales)
        quantity_rounded = round(quantity, cantidad_decimales)
        
        log_consola(f"🔧 Intentando crear OCO: TP={tp_price_rounded:.{precio_decimales}f}, SL={sl_price_rounded:.{precio_decimales}f}, Quantity={quantity_rounded:.{cantidad_decimales}f}")
        
        order = api_call_with_retry(client.futures_create_oco_order,
            symbol=symbol,
            side=side,  # 'SELL' para long, 'BUY' para short
            quantity=quantity_rounded,
            price=tp_price_rounded,  # TP price
            stopPrice=sl_price_rounded,  # SL price
            stopLimitPrice=sl_price_rounded,  # SL limit price
            stopLimitTimeInForce='GTC'
        )
        log_consola(f"✅ Orden OCO creada exitosamente: TP={tp_price_rounded:.4f}, SL={sl_price_rounded:.4f}")
        return order
    except Exception as e:
        error_msg = str(e)
        log_consola(f"❌ Error creando orden OCO: {error_msg}")
        log_consola(f"   Detalles: symbol={symbol}, side={side}, quantity={quantity}, tp={tp_price}, sl={sl_price}")
        return None

def crear_ordenes_tp_sl_separadas(symbol, side, quantity, tp_price, sl_price):
    """
    Crea órdenes TP y SL separadas cuando la OCO falla.
    Retorna True si ambas órdenes se crearon correctamente, False en caso contrario.
    """
    tp_order = None
    sl_order = None
    try:
        cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
        tp_price_rounded = round(tp_price, precio_decimales)
        sl_price_rounded = round(sl_price, precio_decimales)
        quantity_rounded = round(quantity, cantidad_decimales)
        
        log_consola(f"🔧 Intentando crear órdenes TP/SL separadas: TP={tp_price_rounded:.{precio_decimales}f}, SL={sl_price_rounded:.{precio_decimales}f}, Quantity={quantity_rounded:.{cantidad_decimales}f}")
        
        # Crear orden de Take Profit
        try:
            tp_order = api_call_with_retry(client.futures_create_order,
                symbol=symbol,
                side=side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=tp_price_rounded,
                quantity=quantity_rounded,
                reduceOnly=True
            )
            log_consola(f"✅ Orden TP creada exitosamente: {tp_price_rounded:.4f}")
        except Exception as e:
            error_msg = str(e)
            log_consola(f"❌ Error creando orden TP: {error_msg}")
            log_consola(f"   Detalles: symbol={symbol}, side={side}, type=TAKE_PROFIT_MARKET, stopPrice={tp_price_rounded}, quantity={quantity_rounded}")
            return False
        
        # Crear orden de Stop Loss
        try:
            sl_order = api_call_with_retry(client.futures_create_order,
                symbol=symbol,
                side=side,
                type='STOP_MARKET',
                stopPrice=sl_price_rounded,
                quantity=quantity_rounded,
                reduceOnly=True
            )
            log_consola(f"✅ Orden SL creada exitosamente: {sl_price_rounded:.4f}")
        except Exception as e:
            error_msg = str(e)
            log_consola(f"❌ Error creando orden SL: {error_msg}")
            log_consola(f"   Detalles: symbol={symbol}, side={side}, type=STOP_MARKET, stopPrice={sl_price_rounded}, quantity={quantity_rounded}")
            # Intentar cancelar la orden TP si se creó pero falló el SL
            if tp_order:
                try:
                    api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=tp_order['orderId'])
                    log_consola("🗑️ Orden TP cancelada debido a error en SL")
                except Exception as cancel_error:
                    log_consola(f"⚠️ Error cancelando orden TP: {cancel_error}")
            return False
        
        log_consola(f"✅ Ambas órdenes TP/SL creadas correctamente")
        return True
    except Exception as e:
        error_msg = str(e)
        log_consola(f"❌ Error inesperado creando órdenes TP/SL separadas: {error_msg}")
        # Intentar limpiar órdenes creadas si hay error
        if tp_order:
            try:
                api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=tp_order['orderId'])
                log_consola("🗑️ Orden TP cancelada debido a error general")
            except:
                pass
        if sl_order:
            try:
                api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=sl_order['orderId'])
                log_consola("🗑️ Orden SL cancelada debido a error general")
            except:
                pass
        return False

def calcular_kelly_fraction():
    """Calcula la fracción de Kelly basada en el historial de operaciones"""
    archivo = 'registro_operaciones.csv'
    if not os.path.exists(archivo):
        return 0.0

    profits = []
    try:
        with open(archivo, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('Resultado', '').strip() in ['SL', 'TP']:
                    tipo = row['Tipo']
                    precio_entrada = float(row['Precio Entrada'])
                    cantidad = float(row['Cantidad'])
                    take_profit = float(row['Take Profit'])
                    stop_loss = float(row['Stop Loss'])
                    resultado = row['Resultado']

                    if resultado == 'TP':
                        precio_salida = take_profit
                    elif resultado == 'SL':
                        precio_salida = stop_loss
                    else:
                        continue

                    if tipo == 'long':
                        profit = (precio_salida - precio_entrada) * cantidad
                    elif tipo == 'short':
                        profit = (precio_entrada - precio_salida) * cantidad
                    else:
                        continue

                    profits.append(profit)
    except Exception as e:
        log_consola(f"Error leyendo registro para Kelly: {e}")
        return 0.0

    if len(profits) < 10:  # Necesitamos al menos 10 operaciones para calcular Kelly
        return 0.0

    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p < 0]

    if not wins or not losses:
        return 0.0

    p = len(wins) / len(profits)
    avg_win = np.mean(wins)
    avg_loss = abs(np.mean(losses))
    b = avg_win / avg_loss if avg_loss > 0 else 0

    if b <= 0:
        return 0.0

    kelly = (b * p - (1 - p)) / b
    kelly = max(0, kelly)  # No negativo

    # Aplicar half-Kelly y límite máximo
    kelly_ajustado = kelly * kelly_fraction
    kelly_ajustado = min(kelly_ajustado, riesgo_max_kelly)

    return kelly_ajustado

# ============ NUEVAS FUNCIONALIDADES ============

def verificar_estado_posicion(symbol):
    """
    Health Check: Verifica que las posiciones abiertas tengan órdenes TP/SL activas.
    Retorna (ok, mensaje) donde ok es True si todo está bien.
    """
    try:
        posicion = api_call_with_retry(client.futures_position_information, symbol=symbol)
        ordenes = api_call_with_retry(client.futures_get_open_orders, symbol=symbol)
        
        if not posicion:
            return True, "No hay información de posición"
        
        pos_abierta = float(posicion[0]['positionAmt']) != 0
        
        if not pos_abierta:
            return True, "No hay posición abierta"
        
        # Verificar que hay órdenes TP/SL
        tiene_tp_sl = any(o['type'] in ['TAKE_PROFIT_MARKET', 'STOP_MARKET', 'TAKE_PROFIT', 'STOP'] 
                         for o in ordenes)
        
        if not tiene_tp_sl:
            mensaje = f"🚨 ALERTA: {symbol} tiene posición abierta sin órdenes TP/SL activas"
            log_consola(mensaje)
            enviar_telegram(mensaje)
            return False, "Posición sin protección TP/SL"
        
        return True, "OK"
        
    except Exception as e:
        log_consola(f"❌ Error en health check: {e}")
        return False, f"Error: {str(e)}"

def actualizar_trailing_stop(symbol, precio_entrada, senal, precio_actual, sl_actual, porcentaje_trailing=None):
    """
    Actualiza el stop loss siguiendo el precio favorablemente (Trailing Stop).
    Retorna (nuevo_sl, actualizado) donde actualizado es True si se actualizó.
    """
    if porcentaje_trailing is None:
        porcentaje_trailing = trailing_stop_pct
    
    try:
        if senal == 'long':
            nuevo_sl = precio_actual * (1 - porcentaje_trailing / 100)
            if nuevo_sl > sl_actual and nuevo_sl < precio_actual:
                log_consola(f"📈 Trailing Stop: Actualizando SL de {sl_actual:.4f} a {nuevo_sl:.4f} (LONG)")
                return nuevo_sl, True
        else:  # short
            nuevo_sl = precio_actual * (1 + porcentaje_trailing / 100)
            if nuevo_sl < sl_actual and nuevo_sl > precio_actual:
                log_consola(f"📉 Trailing Stop: Actualizando SL de {sl_actual:.4f} a {nuevo_sl:.4f} (SHORT)")
                return nuevo_sl, True
        
        return sl_actual, False
    except Exception as e:
        log_consola(f"❌ Error en trailing stop: {e}")
        return sl_actual, False

def aplicar_trailing_stop(symbol, datos_operacion):
    """Aplica trailing stop loss a una posición abierta."""
    try:
        precio_actual = float(api_call_with_retry(client.futures_symbol_ticker, symbol=symbol)['price'])
        precio_entrada = datos_operacion['precio_entrada']
        senal = datos_operacion['senal']
        sl_actual = datos_operacion['sl']
        cantidad = datos_operacion['cantidad_real']
        
        nuevo_sl, debe_actualizar = actualizar_trailing_stop(
            symbol, precio_entrada, senal, precio_actual, sl_actual
        )
        
        if not debe_actualizar:
            return False
        
        # Cancelar orden SL actual y crear nueva
        ordenes = api_call_with_retry(client.futures_get_open_orders, symbol=symbol)
        for orden in ordenes:
            if orden['type'] in ['STOP_MARKET', 'STOP']:
                try:
                    api_call_with_retry(client.futures_cancel_order, symbol=symbol, orderId=orden['orderId'])
                except:
                    pass
        
        side_oco = 'SELL' if senal == 'long' else 'BUY'
        cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
        nuevo_sl_rounded = round(nuevo_sl, precio_decimales)
        
        api_call_with_retry(client.futures_create_order,
            symbol=symbol,
            side=side_oco,
            type='STOP_MARKET',
            stopPrice=nuevo_sl_rounded,
            quantity=round(cantidad, cantidad_decimales),
            reduceOnly=True
        )
        
        datos_operacion['sl'] = nuevo_sl_rounded
        enviar_telegram(f"📈 Trailing Stop actualizado en {symbol}: SL={nuevo_sl_rounded:.4f}")
        return True
            
    except Exception as e:
        log_consola(f"❌ Error aplicando trailing stop: {e}")
        return False

def analizar_performance_tiempo_real():
    """Calcula métricas de performance en tiempo real."""
    archivo = 'registro_operaciones.csv'
    if not os.path.exists(archivo):
        return None
    
    try:
        df = pd.read_csv(archivo)
        if df.empty:
            return None
        
        df_completadas = df[df['Resultado'].isin(['TP', 'SL', 'NEUTRAL'])]
        if len(df_completadas) == 0:
            return None
        
        total_trades = len(df_completadas)
        ganadoras = len(df_completadas[df_completadas['Resultado'] == 'TP'])
        perdedoras = len(df_completadas[df_completadas['Resultado'] == 'SL'])
        win_rate = (ganadoras / total_trades * 100) if total_trades > 0 else 0
        
        df_completadas['PnL'] = pd.to_numeric(df_completadas['PnL'], errors='coerce')
        pnl_total = df_completadas['PnL'].sum()
        pnl_promedio = df_completadas['PnL'].mean()
        
        ganancias = df_completadas[df_completadas['PnL'] > 0]['PnL'].sum()
        perdidas = abs(df_completadas[df_completadas['PnL'] < 0]['PnL'].sum())
        profit_factor = ganancias / perdidas if perdidas > 0 else float('inf')
        
        if len(df_completadas) > 1:
            retornos = df_completadas['PnL'].pct_change().dropna()
            sharpe_ratio = (retornos.mean() / retornos.std() * np.sqrt(252)) if len(retornos) > 0 and retornos.std() > 0 else 0
        else:
            sharpe_ratio = 0
        
        df_completadas = df_completadas.sort_values('Fecha').reset_index(drop=True)
        racha_actual = 0
        ultimo_resultado = None
        for resultado in reversed(df_completadas['Resultado']):
            if ultimo_resultado is None:
                ultimo_resultado = resultado
                racha_actual = 1
            elif resultado == ultimo_resultado:
                racha_actual += 1
            else:
                break
        
        return {
            'total_trades': total_trades,
            'ganadoras': ganadoras,
            'perdedoras': perdedoras,
            'win_rate': win_rate,
            'pnl_total': pnl_total,
            'pnl_promedio': pnl_promedio,
            'profit_factor': profit_factor,
            'sharpe_ratio': sharpe_ratio,
            'racha_actual': racha_actual,
            'tipo_racha': ultimo_resultado if ultimo_resultado else 'N/A',
            'mayor_ganancia': df_completadas['PnL'].max(),
            'mayor_perdida': df_completadas['PnL'].min()
        }
    except Exception as e:
        log_consola(f"❌ Error analizando performance: {e}")
        return None

def obtener_resumen_performance():
    """Obtiene un resumen formateado de la performance."""
    metrics = analizar_performance_tiempo_real()
    if metrics is None:
        return "❌ No hay datos suficientes para analizar performance."
    
    mensaje = "📊 **Análisis de Performance en Tiempo Real**\n\n"
    mensaje += f"📈 **Estadísticas Generales:**\n"
    mensaje += f"• Total de operaciones: {metrics['total_trades']}\n"
    mensaje += f"• Ganadoras: {metrics['ganadoras']} | Perdedoras: {metrics['perdedoras']}\n"
    mensaje += f"• Win Rate: {metrics['win_rate']:.2f}%\n\n"
    mensaje += f"💰 **Rentabilidad:**\n"
    mensaje += f"• PnL Total: {metrics['pnl_total']:.4f} USDT\n"
    mensaje += f"• PnL Promedio: {metrics['pnl_promedio']:.4f} USDT\n"
    mensaje += f"• Profit Factor: {metrics['profit_factor']:.2f}\n"
    mensaje += f"• Sharpe Ratio: {metrics['sharpe_ratio']:.2f}\n\n"
    mensaje += f"📊 **Extremos:**\n"
    mensaje += f"• Mayor Ganancia: {metrics['mayor_ganancia']:.4f} USDT\n"
    mensaje += f"• Mayor Pérdida: {metrics['mayor_perdida']:.4f} USDT\n\n"
    mensaje += f"🔥 **Racha Actual:**\n"
    mensaje += f"• {metrics['racha_actual']} operaciones {metrics['tipo_racha']}"
    
    return mensaje

def backtest_estrategia(symbol, intervalo, fecha_inicio, fecha_fin=None, limite_velas=500):
    """Ejecuta backtesting de la estrategia usando datos históricos."""
    try:
        log_consola(f"🔄 Iniciando backtest para {symbol} en {intervalo}...")
        
        if fecha_fin:
            klines = api_call_with_retry(client.futures_historical_klines,
                symbol=symbol,
                interval=intervalo,
                start_str=fecha_inicio.strftime('%Y-%m-%d') if isinstance(fecha_inicio, datetime) else str(fecha_inicio),
                end_str=fecha_fin.strftime('%Y-%m-%d') if isinstance(fecha_fin, datetime) else str(fecha_fin),
                limit=limite_velas
            )
        else:
            klines = api_call_with_retry(client.futures_klines,
                symbol=symbol,
                interval=intervalo,
                limit=limite_velas
            )
        
        if not klines:
            return {"error": "No se pudieron obtener datos históricos"}
        
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume',
                                           'close_time', 'quote_asset_volume', 'number_of_trades',
                                           'taker_buy_base', 'taker_buy_quote', 'ignore'])
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        
        posicion_abierta = None
        precio_entrada = None
        tipo_posicion = None
        tp_precio = None
        sl_precio = None
        operaciones = []
        capital_inicial = 1000
        capital_actual = capital_inicial
        
        for i in range(len(df)):
            if i < max(bb_length, atr_length, ma_trend_length) + 1:
                continue
            
            df_slice = df.iloc[:i+1].copy()
            senal = calcular_senal(df_slice)
            precio_actual = float(df_slice['close'].iloc[-1])
            
            if posicion_abierta:
                if tipo_posicion == 'long':
                    if precio_actual >= tp_precio:
                        pnl = (tp_precio - precio_entrada) * posicion_abierta
                        capital_actual += pnl
                        operaciones.append({'tipo': 'long', 'entrada': precio_entrada, 'salida': tp_precio, 'resultado': 'TP', 'pnl': pnl})
                        posicion_abierta = None
                    elif precio_actual <= sl_precio:
                        pnl = (sl_precio - precio_entrada) * posicion_abierta
                        capital_actual += pnl
                        operaciones.append({'tipo': 'long', 'entrada': precio_entrada, 'salida': sl_precio, 'resultado': 'SL', 'pnl': pnl})
                        posicion_abierta = None
                else:
                    if precio_actual <= tp_precio:
                        pnl = (precio_entrada - tp_precio) * posicion_abierta
                        capital_actual += pnl
                        operaciones.append({'tipo': 'short', 'entrada': precio_entrada, 'salida': tp_precio, 'resultado': 'TP', 'pnl': pnl})
                        posicion_abierta = None
                    elif precio_actual >= sl_precio:
                        pnl = (precio_entrada - sl_precio) * posicion_abierta
                        capital_actual += pnl
                        operaciones.append({'tipo': 'short', 'entrada': precio_entrada, 'salida': sl_precio, 'resultado': 'SL', 'pnl': pnl})
                        posicion_abierta = None
            
            if not posicion_abierta and senal in ['long', 'short']:
                atr = calcular_atr(df_slice)
                if atr and atr <= umbral_volatilidad:
                    precio_entrada = precio_actual
                    tipo_posicion = senal
                    if senal == 'long':
                        tp_precio = precio_entrada + atr * tp_multiplier
                        sl_precio = precio_entrada - atr * sl_multiplier
                    else:
                        tp_precio = precio_entrada - atr * tp_multiplier
                        sl_precio = precio_entrada + atr * sl_multiplier
                    riesgo_usdt = capital_actual * riesgo_pct
                    distancia_sl = abs(precio_entrada - sl_precio)
                    posicion_abierta = riesgo_usdt / distancia_sl if distancia_sl > 0 else 0
        
        total_operaciones = len(operaciones)
        ganadoras = len([o for o in operaciones if o['resultado'] == 'TP'])
        perdedoras = len([o for o in operaciones if o['resultado'] == 'SL'])
        win_rate = (ganadoras / total_operaciones * 100) if total_operaciones > 0 else 0
        pnl_total = sum(o['pnl'] for o in operaciones)
        roi = ((capital_actual - capital_inicial) / capital_inicial) * 100
        
        return {
            'capital_inicial': capital_inicial,
            'capital_final': capital_actual,
            'roi': roi,
            'total_operaciones': total_operaciones,
            'ganadoras': ganadoras,
            'perdedoras': perdedoras,
            'win_rate': win_rate,
            'pnl_total': pnl_total,
            'operaciones': operaciones
        }
        
    except Exception as e:
        log_consola(f"❌ Error en backtest: {e}")
        return {"error": str(e)}

# ============ FUNCIÓN PRINCIPAL DEL BOT ============
def ejecutar_bot_trading():
    """Función principal del bot de trading que se ejecuta en un hilo separado"""
    global bot_activo

    ultima_posicion_cerrada = True
    datos_ultima_operacion = {}
    hubo_posicion_abierta = False
    tiempo_ultima_apertura = None
    ultimo_tp = None
    ultimo_sl = None
    perdidas_consecutivas = 0  # Al inicio de ejecutar_bot_trading
    ultimo_health_check = time.time()
    ultimo_trailing_check = time.time()

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

            # --- 1. PROCESAR CIERRE SI HAY UNO PENDIENTE ---
            tiempo_actual = time.time()
            # Verificar que realmente no hay posición abierta (doble verificación)
            pos_abierta_verificada = 0.0
            try:
                info_pos_verificacion = api_call_with_retry(client.futures_position_information, symbol=symbol)
                if info_pos_verificacion:
                    pos_abierta_verificada = float(info_pos_verificacion[0]['positionAmt'])
            except:
                pos_abierta_verificada = pos_abierta  # Usar el valor anterior si falla la verificación
            
            if (pos_abierta == 0 and pos_abierta_verificada == 0 and 
                not ultima_posicion_cerrada and 
                datos_ultima_operacion and 
                hubo_posicion_abierta and
                tiempo_ultima_apertura and
                (tiempo_actual - tiempo_ultima_apertura) > 10):

                log_consola("🔍 Detectando cierre de posición...")
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
                        cantidad = datos_ultima_operacion["cantidad_real"]
                        
                        if pnl > 0:
                            resultado = "TP"
                            mensaje = f"🎉 **Take Profit alcanzado en {symbol}**\n"
                            mensaje += f"💰 Ganancia: {pnl:.4f} USDT\n"
                            mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                            mensaje += f"📊 Precio salida: {precio_ejecucion:.4f}\n"
                            mensaje += f"📦 Cantidad: {cantidad}\n"
                            mensaje += f"🎯 TP objetivo: {tp:.4f}"
                            enviar_telegram(mensaje)
                        elif pnl < 0:
                            resultado = "SL"
                            mensaje = f"⚠️ **Stop Loss alcanzado en {symbol}**\n"
                            mensaje += f"📉 Pérdida: {pnl:.4f} USDT\n"
                            mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                            mensaje += f"📊 Precio salida: {precio_ejecucion:.4f}\n"
                            mensaje += f"📦 Cantidad: {cantidad}\n"
                            mensaje += f"🛑 SL objetivo: {sl:.4f}"
                            enviar_telegram(mensaje)
                        else:
                            resultado = "NEUTRAL"
                            mensaje = f"🔔 **Posición cerrada en {symbol}**\n"
                            mensaje += f"📊 PnL: {pnl:.4f} USDT\n"
                            mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                            mensaje += f"📊 Precio salida: {precio_ejecucion:.4f}"
                            enviar_telegram(mensaje)
                        log_consola(f"📊 Detalles del cierre: Precio entrada={precio_entrada:.4f}, Precio ejecución={precio_ejecucion:.4f}, {resultado}, PnL={pnl:.4f}")
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
                    tp = datos_ultima_operacion["tp"]
                    sl = datos_ultima_operacion["sl"]
                    
                    if senal_original == 'long':
                        pnl = (precio_actual - precio_entrada) * cantidad
                    else:
                        pnl = (precio_entrada - precio_actual) * cantidad
                    precio_ejecucion = precio_actual
                    
                    if pnl > 0:
                        resultado = "TP"
                        mensaje = f"🎉 **Take Profit alcanzado en {symbol}** (aproximado)\n"
                        mensaje += f"💰 Ganancia aproximada: {pnl:.4f} USDT\n"
                        mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                        mensaje += f"📊 Precio actual: {precio_ejecucion:.4f}"
                        enviar_telegram(mensaje)
                    elif pnl < 0:
                        resultado = "SL"
                        mensaje = f"⚠️ **Stop Loss alcanzado en {symbol}** (aproximado)\n"
                        mensaje += f"📉 Pérdida aproximada: {pnl:.4f} USDT\n"
                        mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                        mensaje += f"📊 Precio actual: {precio_ejecucion:.4f}"
                        enviar_telegram(mensaje)
                    else:
                        resultado = "NEUTRAL"
                        mensaje = f"🔔 **Posición cerrada en {symbol}** (aproximado)\n"
                        mensaje += f"📊 PnL aproximado: {pnl:.4f} USDT\n"
                        mensaje += f"📊 Precio entrada: {precio_entrada:.4f}\n"
                        mensaje += f"📊 Precio actual: {precio_ejecucion:.4f}"
                        enviar_telegram(mensaje)
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
                # Calcular ATR para validar volatilidad y calcular TP/SL
                atr = calcular_atr(df)
                if atr is None:
                    log_consola("⚠️ No se pudo calcular ATR, esperando más datos...")
                    time.sleep(60)
                    continue
                if atr > umbral_volatilidad:
                    log_consola(f"Mercado demasiado volátil (ATR={atr:.6f} > {umbral_volatilidad}), no se opera.")
                    time.sleep(60)
                    continue

                balance = api_call_with_retry(client.futures_account_balance)
                saldo_usdt = next((float(b['balance']) for b in balance if b['asset'] == 'USDT'), 0)

                precio_actual = float(df['close'].iloc[-1])

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
                    # Notificar inmediatamente que se ejecutó la orden
                    mensaje_orden = f"✅ **Orden {senal.upper()} ejecutada**\n"
                    mensaje_orden += f"📊 Símbolo: {symbol}\n"
                    mensaje_orden += f"💰 Precio entrada: {precio_entrada:.4f}\n"
                    mensaje_orden += f"📦 Cantidad: {cantidad_real}\n"
                    mensaje_orden += f"🎯 Take Profit: {tp:.4f}\n"
                    mensaje_orden += f"🛑 Stop Loss: {sl:.4f}"
                    enviar_telegram(mensaje_orden)
                    log_consola(f"✅ Orden {senal.upper()} ejecutada a {precio_entrada:.4f}")
                    
                    # Esperar un momento para que Binance registre la posición
                    time.sleep(2)
                    
                    # Validar que los precios TP/SL estén en la dirección correcta
                    if senal == 'long':
                        if tp <= precio_entrada:
                            log_consola(f"⚠️ TP ({tp:.4f}) debe ser mayor que precio entrada ({precio_entrada:.4f}) para LONG. Ajustando...")
                            tp = precio_entrada * 1.001  # Ajustar TP ligeramente por encima
                        if sl >= precio_entrada:
                            log_consola(f"⚠️ SL ({sl:.4f}) debe ser menor que precio entrada ({precio_entrada:.4f}) para LONG. Ajustando...")
                            sl = precio_entrada * 0.999  # Ajustar SL ligeramente por debajo
                    else:  # short
                        if tp >= precio_entrada:
                            log_consola(f"⚠️ TP ({tp:.4f}) debe ser menor que precio entrada ({precio_entrada:.4f}) para SHORT. Ajustando...")
                            tp = precio_entrada * 0.999  # Ajustar TP ligeramente por debajo
                        if sl <= precio_entrada:
                            log_consola(f"⚠️ SL ({sl:.4f}) debe ser mayor que precio entrada ({precio_entrada:.4f}) para SHORT. Ajustando...")
                            sl = precio_entrada * 1.001  # Ajustar SL ligeramente por encima
                    
                    # Redondear nuevamente después de los ajustes
                    cantidad_decimales, precio_decimales = obtener_precisiones(symbol)
                    tp = round(tp, precio_decimales)
                    sl = round(sl, precio_decimales)
                    
                    # Crear orden OCO para TP/SL
                    side_oco = 'SELL' if senal == 'long' else 'BUY'
                    oco_order = crear_orden_oco(symbol, side_oco, cantidad_real, tp, sl)
                    
                    if oco_order is None:
                        # Si falla crear OCO, intentar crear órdenes TP/SL separadas
                        log_consola("⚠️ Falló crear OCO, intentando crear órdenes TP/SL separadas...")
                        ordenes_creadas = crear_ordenes_tp_sl_separadas(symbol, side_oco, cantidad_real, tp, sl)
                        
                        if not ordenes_creadas:
                            # Si también fallan las órdenes separadas, notificar pero NO cerrar la posición
                            log_consola("❌ Error: No se pudieron crear órdenes TP/SL. La posición queda abierta sin protección.")
                            enviar_telegram(f"⚠️ **ADVERTENCIA**: No se pudieron crear órdenes TP/SL para {symbol}.\nLa posición está abierta sin protección. Por favor, revisa manualmente.")
                            # Continuar con la posición abierta - el usuario puede cerrarla manualmente
                        else:
                            log_consola("✅ Órdenes TP/SL separadas creadas correctamente.")
                    else:
                        log_consola(f"✅ Orden OCO creada correctamente.")

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

                    log_consola(f"🎯 Take Profit: {tp:.4f} | 🛑 Stop Loss: {sl:.4f}")
                else:
                    log_consola(f"❌ No se pudo ejecutar la orden {senal.upper()}.")
                    enviar_telegram(f"❌ Error: No se pudo ejecutar la orden {senal.upper()} para {symbol}.")
            
            # Health Check periódico
            tiempo_actual = time.time()
            if tiempo_actual - ultimo_health_check >= health_check_interval:
                ok, mensaje = verificar_estado_posicion(symbol)
                if not ok:
                    log_consola(f"⚠️ Health Check falló: {mensaje}")
                ultimo_health_check = tiempo_actual
            
            # Trailing Stop (si está activo y hay posición abierta)
            if usar_trailing_stop and datos_ultima_operacion and pos_abierta != 0:
                if tiempo_actual - ultimo_trailing_check >= 60:  # Verificar cada minuto
                    aplicar_trailing_stop(symbol, datos_ultima_operacion)
                    ultimo_trailing_check = tiempo_actual

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
    
    # Iniciar el bot de control de Telegram en un thread separado
    telegram_thread = threading.Thread(target=bot_telegram_control, daemon=True)
    telegram_thread.start()
    
    # Mantener el programa principal vivo
    try:
        while True:
            time.sleep(60)  # Verificar cada minuto si los threads están vivos
            if not telegram_thread.is_alive():
                log_consola("🚨 Thread de Telegram murió. Reiniciando...")
                telegram_thread = threading.Thread(target=bot_telegram_control, daemon=True)
                telegram_thread.start()
    except KeyboardInterrupt:
        log_consola("🛑 Programa detenido por usuario")
        bot_activo = False
        time.sleep(2)  # Dar tiempo a que el thread termine
