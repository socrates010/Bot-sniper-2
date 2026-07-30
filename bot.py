import os
import re
import csv
import time
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from flask import Flask
import ccxt
import pandas as pd
import pandas_ta as ta
import requests

# =====================================================================
# LOGGING
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sniper")

# =====================================================================
# WEB (KEEP-ALIVE PARA O RENDER)
# =====================================================================
app = Flask(__name__)


@app.route('/')
def home():
    return "Bot Sniper Boladão (Hyperliquid) rodando 24/7!"


def iniciar_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)


def watchdog_thread():
    log.info(f"Watchdog iniciado (limite: {WATCHDOG_LIMITE_MINUTOS} min sem atividade).")
    ja_alertou = False
    while True:
        time.sleep(60)
        parado_ha = (time.time() - ultima_atividade) / 60
        if parado_ha > WATCHDOG_LIMITE_MINUTOS:
            if not ja_alertou:
                enviar_mensagem_telegram(
                    f"🚨 *POSSÍVEL TRAVAMENTO*\nO loop principal está sem atividade há "
                    f"`{parado_ha:.1f}` minutos (limite: `{WATCHDOG_LIMITE_MINUTOS}` min).\n"
                    f"O bot pode estar preso numa chamada de rede. Considere reiniciar o serviço."
                )
                ja_alertou = True
        else:
            ja_alertou = False


# =====================================================================
# CONFIGURAÇÕES (VARIÁVEIS DE AMBIENTE)
# =====================================================================
# --- Autenticação Hyperliquid ---
# ATENÇÃO: modelo de auth é diferente da Binance.
#   WALLET_ADDRESS = endereço público da SUA carteira principal (0x...)
#   PRIVATE_KEY    = chave privada de uma "API Wallet" (agent wallet) gerada
#                    dentro do próprio Hyperliquid (menu "More" > API).
# A API Wallet consegue operar em seu nome mas NÃO consegue sacar fundos —
# nunca use a chave privada da sua carteira principal aqui.
WALLET_ADDRESS = os.getenv('WALLET_ADDRESS')
PRIVATE_KEY = os.getenv('PRIVATE_KEY')
USE_TESTNET = os.getenv('USE_TESTNET', 'false').lower() == 'true'

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

NOTIFICAR_ERROS_NO_TELEGRAM = os.getenv('NOTIFICAR_ERROS_NO_TELEGRAM', 'true').lower() == 'true'

# Moeda de liquidação das perpétuas na Hyperliquid é USDC (não USDT).
QUOTE = 'USDC'

# Formato de símbolo na Hyperliquid (via ccxt) é "BASE/USDC:USDC", ex: "BTC/USDC:USDC".
SYMBOLS = [s.strip() for s in os.getenv(
    'SYMBOLS', 'BTC/USDC:USDC,ETH/USDC:USDC,SOL/USDC:USDC'
).split(',') if s.strip()]

# --- Estratégias ativas e prioridade por turno ---
# 'bb_breakout'     -> Bollinger Bands + volume, giro mais lento
# 'momentum_scalp'  -> EMA cross + RSI + ATR, giro rápido e mais alavancada
# A ordem da lista = prioridade: se as duas derem sinal no mesmo par no mesmo
# ciclo, a primeira da lista abre a posição.
#
# No período da tarde (horário local, ver TIMEZONE_OPERACIONAL), o mercado
# tende a perder amplitude/volume -> priorizamos o momentum_scalp (que lucra
# com micro-movimentos) e só caímos pro bb_breakout se o scalp não sinalizar.
TIMEZONE_OPERACIONAL = os.getenv('TIMEZONE_OPERACIONAL', 'America/Sao_Paulo')
TURNO_TARDE_INICIO_HORA = int(os.getenv('TURNO_TARDE_INICIO_HORA', 12))  # 12h
TURNO_TARDE_FIM_HORA = int(os.getenv('TURNO_TARDE_FIM_HORA', 18))        # 18h (exclusivo)

ESTRATEGIAS_ORDEM_PADRAO = [s.strip() for s in os.getenv(
    'ESTRATEGIAS_ORDEM_PADRAO', 'bb_breakout,momentum_scalp'
).split(',') if s.strip()]

ESTRATEGIAS_ORDEM_TARDE = [s.strip() for s in os.getenv(
    'ESTRATEGIAS_ORDEM_TARDE', 'momentum_scalp,bb_breakout'
).split(',') if s.strip()]


def obter_ordem_estrategias_atual() -> list:
    """Retorna a lista de estratégias em ordem de prioridade, considerando
    se o horário local atual cai dentro do turno da tarde configurado."""
    agora_local = datetime.now(ZoneInfo(TIMEZONE_OPERACIONAL))
    if TURNO_TARDE_INICIO_HORA <= agora_local.hour < TURNO_TARDE_FIM_HORA:
        return ESTRATEGIAS_ORDEM_TARDE
    return ESTRATEGIAS_ORDEM_PADRAO


# --- Parâmetros da estratégia Sniper (BB breakout) ---
TIMEFRAME = os.getenv('TIMEFRAME', '5m')
BB_LENGTH = int(os.getenv('BB_LENGTH', 20))
BB_STD = float(os.getenv('BB_STD', 1.5))
# Antes: exigia volume > média(20) * multiplicador. Isso ficava alto demais
# à tarde, quando o volume médio do dia todo (que inclui a manhã, mais forte)
# não reflete o volume "normal" daquele momento. Agora comparamos com o
# volume da vela FECHADA imediatamente anterior — régua mais realista pra
# qualquer horário. O multiplicador também foi reduzido (1.5 -> 1.2) porque
# comparar candle-a-candle já é naturalmente mais ruidoso que comparar com
# uma média de 20 períodos.
VOLUME_MULTIPLICADOR = float(os.getenv('VOLUME_MULTIPLICADOR', 1.2))
LIMIT_CANDLES = max(100, BB_LENGTH * 3)

FILTRO_TENDENCIA = os.getenv('FILTRO_TENDENCIA', 'true').lower() == 'true'
# EMA100 -> EMA34: filtro de tendência mais reativo, reage mais rápido a
# mudanças de direção de curto/médio prazo (trade-off: mais sujeito a whipsaw
# em mercado lateral do que a EMA100).
EMA_TENDENCIA = int(os.getenv('EMA_TENDENCIA', 34))

LEVERAGE = int(os.getenv('LEVERAGE', 3))
PCT_TP = float(os.getenv('PCT_TP', 0.015))   # 1.5%
PCT_SL = float(os.getenv('PCT_SL', 0.01))    # 1.0%

# --- Parâmetros da estratégia Momentum Scalp (giro rápido, mais alavancada) ---
# Ideia: opera em timeframe curto, entra na confirmação de cruzamento de médias
# com momentum (RSI) a favor, e usa ATR para dimensionar TP/SL de forma
# adaptativa à volatilidade do momento (em vez de % fixo).
# Timeframe 3m -> 2m e EMA9/21 -> EMA5/13: no 2m, médias mais lentas demoram
# demais para cruzar quando o mercado perde amplitude (típico de tarde) —
# médias mais curtas cruzam com mais frequência, gerando mais gatilhos.
SCALP_TIMEFRAME = os.getenv('SCALP_TIMEFRAME', '2m')
SCALP_EMA_RAPIDA = int(os.getenv('SCALP_EMA_RAPIDA', 5))
SCALP_EMA_LENTA = int(os.getenv('SCALP_EMA_LENTA', 13))
SCALP_RSI_LENGTH = int(os.getenv('SCALP_RSI_LENGTH', 14))
# Faixa de RSI ampliada (35-65) em vez de duas faixas estreitas e opostas
# (era 50-75 no LONG / 25-50 no SHORT). Ganha-se frequência de sinal, perde-se
# a proteção contra comprar/vender em zona de RSI mais neutra — é a troca
# consciente pedida (giro rápido > seletividade).
SCALP_RSI_LONG_MIN = float(os.getenv('SCALP_RSI_LONG_MIN', 35))   # RSI mínimo p/ LONG
SCALP_RSI_LONG_MAX = float(os.getenv('SCALP_RSI_LONG_MAX', 100))  # sem teto (era 75)
SCALP_RSI_SHORT_MAX = float(os.getenv('SCALP_RSI_SHORT_MAX', 65))  # RSI máximo p/ SHORT
SCALP_RSI_SHORT_MIN = float(os.getenv('SCALP_RSI_SHORT_MIN', 0))   # sem piso (era 25)
SCALP_ATR_LENGTH = int(os.getenv('SCALP_ATR_LENGTH', 14))
# Multiplicadores de ATR menores -> TP/SL mais apertados -> posições fecham
# mais rápido -> libera margem pra próxima entrada mais cedo (giro rápido).
SCALP_ATR_MULT_TP = float(os.getenv('SCALP_ATR_MULT_TP', 1.0))
SCALP_ATR_MULT_SL = float(os.getenv('SCALP_ATR_MULT_SL', 0.8))
SCALP_PCT_SL_MINIMO = float(os.getenv('SCALP_PCT_SL_MINIMO', 0.003))  # piso de 0.3%
SCALP_PCT_TP_MAXIMO = float(os.getenv('SCALP_PCT_TP_MAXIMO', 0.05))   # teto de 5%
SCALP_LEVERAGE = int(os.getenv('SCALP_LEVERAGE', 5))
SCALP_LIMIT_CANDLES = max(100, max(SCALP_EMA_LENTA, SCALP_RSI_LENGTH, SCALP_ATR_LENGTH) * 3)

# --- Filtro de liquidez / market cap ---
# Objetivo: evitar operar (com alavancagem) tokens pequenos/pouco líquidos,
# onde um único player consegue mover o preço e "caçar" stops com mais
# facilidade. O market cap por token em tempo real é um dado do plano PAGO
# da API do DefiLlama (US$300/mês) — o plano gratuito só libera preço/TVL/fees.
# Como o próprio DefiLlama usa o CoinGecko como fonte para a maioria dos
# tokens, usamos a API pública e gratuita do CoinGecko para o mesmo dado.
FILTRO_MARKET_CAP_ATIVO = os.getenv('FILTRO_MARKET_CAP_ATIVO', 'true').lower() == 'true'
MARKET_CAP_MINIMO_USD = float(os.getenv('MARKET_CAP_MINIMO_USD', 500_000_000))  # 500M USD
MARKET_CAP_CACHE_MINUTOS = float(os.getenv('MARKET_CAP_CACHE_MINUTOS', 30))

# Mapeamento símbolo (base, sem /USDC:USDC) -> id do CoinGecko. Cobre os pares
# mais comuns; se você operar um par fora dessa lista, adicione aqui ou via
# COINGECKO_IDS_EXTRA=BASE1:id1,BASE2:id2 (variável de ambiente).
COINGECKO_IDS = {
    'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'AVAX': 'avalanche-2',
    'APT': 'aptos', 'ARB': 'arbitrum', 'OP': 'optimism', 'DOGE': 'dogecoin',
    'XRP': 'ripple', 'BNB': 'binancecoin', 'SUI': 'sui', 'LINK': 'chainlink',
    'ADA': 'cardano', 'LTC': 'litecoin', 'DOT': 'polkadot', 'NEAR': 'near',
    'HYPE': 'hyperliquid', 'WLD': 'worldcoin-wld', 'TIA': 'celestia',
    'INJ': 'injective-protocol', 'SEI': 'sei-network',
}
for _par in os.getenv('COINGECKO_IDS_EXTRA', '').split(','):
    if ':' in _par:
        _base, _id = _par.split(':', 1)
        COINGECKO_IDS[_base.strip().upper()] = _id.strip()

# Trava de segurança: nenhuma estratégia pode configurar alavancagem acima disso,
# mesmo que a variável de ambiente correspondente esteja mal configurada.
MAX_LEVERAGE_PERMITIDO = int(os.getenv('MAX_LEVERAGE_PERMITIDO', 10))

# --- Risk management (compartilhado entre as estratégias) ---
RISCO_POR_TRADE_PCT = float(os.getenv('RISCO_POR_TRADE_PCT', 0.01))
NOTIONAL_MAXIMO_USDT = float(os.getenv('NOTIONAL_MAXIMO_USDT', 100))
MAX_POSICOES_SIMULTANEAS = int(os.getenv('MAX_POSICOES_SIMULTANEAS', 3))
MAX_DRAWDOWN_DIARIO_PCT = float(os.getenv('MAX_DRAWDOWN_DIARIO_PCT', 0.05))
FECHAR_POSICOES_NO_KILLSWITCH = os.getenv('FECHAR_POSICOES_NO_KILLSWITCH', 'false').lower() == 'true'

CICLO_SEGUNDOS = int(os.getenv('CICLO_SEGUNDOS', 45))

TRADES_LOG_PATH = os.getenv('TRADES_LOG_PATH', 'trades_log.csv')

# =====================================================================
# INICIALIZAÇÃO DA HYPERLIQUID (via ccxt)
# =====================================================================
if not WALLET_ADDRESS or not PRIVATE_KEY:
    raise RuntimeError(
        "WALLET_ADDRESS e PRIVATE_KEY são obrigatórios. "
        "WALLET_ADDRESS = endereço público da sua carteira principal; "
        "PRIVATE_KEY = chave privada de uma API Wallet gerada no Hyperliquid."
    )

exchange = ccxt.hyperliquid({
    'walletAddress': WALLET_ADDRESS,
    'privateKey': PRIVATE_KEY,
    'enableRateLimit': True,
    'timeout': 15000,
    'options': {'defaultType': 'swap'},
})

if USE_TESTNET:
    try:
        exchange.set_sandbox_mode(True)
        log.warning("Rodando em TESTNET da Hyperliquid (USE_TESTNET=true).")
    except Exception as e:
        log.warning(f"Não foi possível ativar sandbox mode: {e}. "
                    f"Verifique se a versão do ccxt instalada suporta set_sandbox_mode para hyperliquid.")

# IMPORTANTE: exchange.market(symbol) (usado no diagnóstico de viabilidade e
# no cálculo do preço de referência) é uma consulta LOCAL ao dicionário de
# mercados do ccxt — ele não carrega nada sozinho. Sem chamar load_markets()
# uma vez aqui, toda chamada a exchange.market() falha com "markets not loaded".
try:
    exchange.load_markets()
    log.info(f"Mercados da Hyperliquid carregados ({len(exchange.markets)} pares disponíveis).")
except Exception as e:
    log.error(f"Falha ao carregar mercados da Hyperliquid no startup: {e}")

# --- Monitoramento de saúde do loop principal ---
HEARTBEAT_HORAS = float(os.getenv('HEARTBEAT_HORAS', 6))
WATCHDOG_LIMITE_MINUTOS = float(os.getenv('WATCHDOG_LIMITE_MINUTOS', 10))
ultima_atividade = time.time()


# =====================================================================
# NOTIFICAÇÕES
# =====================================================================
def enviar_mensagem_telegram(mensagem: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram não configurado (TELEGRAM_TOKEN/TELEGRAM_CHAT_ID ausentes).")
        return
    for chat_id in str(TELEGRAM_CHAT_ID).split(','):
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={
                "chat_id": chat_id,
                "text": mensagem,
                "parse_mode": "Markdown",
            }, timeout=5)
        except Exception as e:
            log.warning(f"Falha ao notificar Telegram ({chat_id}): {e}")


def enviar_arquivo_telegram(caminho: str, legenda: str = ""):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not os.path.exists(caminho):
        return
    for chat_id in str(TELEGRAM_CHAT_ID).split(','):
        chat_id = chat_id.strip()
        if not chat_id:
            continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
            with open(caminho, 'rb') as f:
                requests.post(url, data={"chat_id": chat_id, "caption": legenda},
                               files={"document": f}, timeout=15)
        except Exception as e:
            log.warning(f"Falha ao enviar arquivo ao Telegram ({chat_id}): {e}")


class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        try:
            mensagem = self.format(record)
            enviar_mensagem_telegram(f"⚠️ *[{record.levelname}]*\n`{mensagem}`")
        except Exception:
            pass


if NOTIFICAR_ERROS_NO_TELEGRAM:
    _tg_handler = TelegramLogHandler(level=logging.ERROR)
    _tg_handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(_tg_handler)


# =====================================================================
# LOG DE TRADES (CSV)
# =====================================================================
TRADE_LOG_CAMPOS = [
    'timestamp', 'symbol', 'estrategia', 'tipo', 'entrada', 'tp', 'sl',
    'quantidade', 'notional_usdt', 'leverage', 'pnl_usdt',
]


def registrar_trade_csv(**kwargs):
    novo_arquivo = not os.path.exists(TRADES_LOG_PATH)
    linha = {campo: kwargs.get(campo, '') for campo in TRADE_LOG_CAMPOS}
    with open(TRADES_LOG_PATH, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_CAMPOS)
        if novo_arquivo:
            writer.writeheader()
        writer.writerow(linha)


def resumo_trades_csv() -> dict:
    if not os.path.exists(TRADES_LOG_PATH):
        return {'total_saidas': 0, 'vitorias': 0, 'derrotas': 0, 'pnl_total': 0.0}

    total_saidas = vitorias = derrotas = 0
    pnl_total = 0.0
    with open(TRADES_LOG_PATH, newline='') as f:
        for linha in csv.DictReader(f):
            if linha.get('tipo') != 'SAIDA':
                continue
            pnl_str = linha.get('pnl_usdt') or ''
            if pnl_str == '':
                continue
            pnl = float(pnl_str)
            total_saidas += 1
            pnl_total += pnl
            if pnl >= 0:
                vitorias += 1
            else:
                derrotas += 1

    return {'total_saidas': total_saidas, 'vitorias': vitorias, 'derrotas': derrotas, 'pnl_total': pnl_total}


# =====================================================================
# RISK MANAGER
# =====================================================================
@dataclass
class RiskManager:
    saldo_inicial_dia: float = None
    data_referencia: str = None
    kill_switch_ativo: bool = False

    def _hoje(self) -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def atualizar_referencia_diaria(self, saldo_atual: float):
        hoje = self._hoje()
        if self.data_referencia != hoje:
            self.data_referencia = hoje
            self.saldo_inicial_dia = saldo_atual
            self.kill_switch_ativo = False
            log.info(f"Novo dia de referência de risco. Saldo inicial: {saldo_atual:.2f} {QUOTE}")

    def checar_kill_switch(self, saldo_atual: float) -> bool:
        self.atualizar_referencia_diaria(saldo_atual)

        if self.saldo_inicial_dia and self.saldo_inicial_dia > 0:
            drawdown_pct = (self.saldo_inicial_dia - saldo_atual) / self.saldo_inicial_dia
            if drawdown_pct >= MAX_DRAWDOWN_DIARIO_PCT and not self.kill_switch_ativo:
                self.kill_switch_ativo = True
                msg = (f"🛑 *KILL SWITCH ATIVADO*\nDrawdown diário: `{drawdown_pct*100:.2f}%` "
                       f"(limite: `{MAX_DRAWDOWN_DIARIO_PCT*100:.1f}%`)\n"
                       f"Novas entradas suspensas até o próximo dia UTC.")
                log.warning(msg.replace('*', '').replace('`', ''))
                enviar_mensagem_telegram(msg)

        return self.kill_switch_ativo

    def calcular_tamanho_posicao(self, saldo_disponivel: float, pct_sl: float) -> float:
        pct_sl = max(pct_sl, 0.0005)  # nunca deixa dividir por um SL ~0
        valor_risco = saldo_disponivel * RISCO_POR_TRADE_PCT
        notional = valor_risco / pct_sl
        notional = min(notional, NOTIONAL_MAXIMO_USDT)
        return notional


risk_manager = RiskManager()


# =====================================================================
# DISJUNTOR DE RATE LIMIT
# =====================================================================
# A Hyperliquid não retorna um timestamp exato de "banido até" como a Binance,
# então aqui usamos uma pausa fixa de segurança sempre que o ccxt sinalizar
# rate limit / erro de infraestrutura (evita martelar a API).
pausa_ate = 0.0
PAUSA_RATE_LIMIT_SEGUNDOS = int(os.getenv('PAUSA_RATE_LIMIT_SEGUNDOS', 90))


def eh_erro_rate_limit(e) -> bool:
    return isinstance(e, (ccxt.RateLimitExceeded, ccxt.DDoSProtection, ccxt.ExchangeNotAvailable))


def registrar_pausa_rate_limit(e) -> float:
    global pausa_ate
    pausa_ate = max(pausa_ate, time.time() + PAUSA_RATE_LIMIT_SEGUNDOS)
    return max(0, pausa_ate - time.time())


def em_pausa() -> bool:
    return time.time() < pausa_ate


# =====================================================================
# HELPERS DE MERCADO
# =====================================================================
def obter_preco_referencia(symbol: str) -> float:
    """Na Hyperliquid, ordens a mercado (via ccxt) exigem um parâmetro de preço,
    usado como proteção de slippage (o preço mid é o padrão recomendado)."""
    market = exchange.market(symbol)
    mid = (market.get('info') or {}).get('midPx')
    if mid:
        return float(mid)
    ticker = exchange.fetch_ticker(symbol)
    return float(ticker['last'])


def obter_saldo_disponivel_usdt() -> float:
    saldo = exchange.fetch_balance()
    info_quote = saldo.get(QUOTE, {})
    disponivel = info_quote.get('free') or info_quote.get('total') or 0
    return float(disponivel)


def configurar_alavancagem_isolada(symbol: str, leverage: int):
    leverage = min(leverage, MAX_LEVERAGE_PERMITIDO)
    try:
        exchange.set_margin_mode('isolated', symbol, params={'leverage': leverage})
    except Exception as e:
        log.warning(f"[{symbol}] Aviso ao configurar margem/alavancagem ({leverage}x): {e}")
    return leverage


# =====================================================================
# DADOS E INDICADORES
# =====================================================================
def buscar_dados(symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    bars = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df


def _coluna_bb(df: pd.DataFrame, prefixo: str) -> str:
    candidatas = [c for c in df.columns if c.startswith(prefixo)]
    if not candidatas:
        raise KeyError(f"Coluna com prefixo '{prefixo}' não encontrada. Colunas: {list(df.columns)}")
    return candidatas[0]


# =====================================================================
# ESTRATÉGIA 1: SNIPER (Bollinger Bands breakout + volume + filtro de tendência)
# =====================================================================
def estrategia_bb_breakout(symbol: str):
    """Retorna dict com sinal/preço/tp/sl/leverage, ou None se não há sinal."""
    df = buscar_dados(symbol, TIMEFRAME, LIMIT_CANDLES)

    df.ta.bbands(close='close', length=BB_LENGTH, std=BB_STD, append=True)
    if FILTRO_TENDENCIA:
        df['ema_tendencia'] = ta.ema(df['close'], length=EMA_TENDENCIA)

    if len(df) < max(BB_LENGTH, EMA_TENDENCIA if FILTRO_TENDENCIA else 0) + 3:
        return None

    candle = df.iloc[-2]        # último candle FECHADO (gatilho = fechamento, não pavio)
    candle_anterior = df.iloc[-3]  # candle imediatamente anterior, usado como referência de volume
    close = candle['close']
    volume = candle['volume']
    volume_anterior = candle_anterior['volume']

    col_lower = _coluna_bb(df, 'BBL_')
    col_upper = _coluna_bb(df, 'BBU_')
    bb_lower = candle[col_lower]
    bb_upper = candle[col_upper]

    if pd.isna(volume_anterior) or pd.isna(bb_lower) or pd.isna(bb_upper):
        return None

    # Antes: volume > média(20). Agora: volume > vela anterior * multiplicador.
    # Fica mais justo ao longo do dia inteiro (a média de 20 velas carrega o
    # volume mais forte da manhã e deixa a régua alta demais à tarde).
    volume_confirmado = volume > (volume_anterior * VOLUME_MULTIPLICADOR)

    # Gatilho pelo FECHAMENTO da vela acima/abaixo da banda (não pelo pavio/
    # máxima) — usar candle['close'] em vez de candle['high']/['low'] já
    # garante isso.
    sinal = None
    if close > bb_upper and volume_confirmado:
        sinal = 'LONG'
    elif close < bb_lower and volume_confirmado:
        sinal = 'SHORT'

    if sinal and FILTRO_TENDENCIA:
        ema = candle.get('ema_tendencia')
        if pd.notna(ema):
            if sinal == 'LONG' and close < ema:
                sinal = None
            elif sinal == 'SHORT' and close > ema:
                sinal = None

    log.info(f"[{symbol}][bb_breakout] Preço: {close:.4f} | BB Inf: {bb_lower:.4f} | BB Sup: {bb_upper:.4f} | "
              f"Vol: {volume:.0f} (vela anterior: {volume_anterior:.0f}) | Sinal: {sinal or '-'}")

    if not sinal:
        return None

    return {
        'nome': 'bb_breakout',
        'sinal': sinal,
        'preco': close,
        'pct_tp': PCT_TP,
        'pct_sl': PCT_SL,
        'leverage': LEVERAGE,
    }


# =====================================================================
# ESTRATÉGIA 2 (NOVA): MOMENTUM SCALP — giro rápido, mais alavancado
# =====================================================================
# Lógica: opera em timeframe curto (ex: 3m). Entra quando a EMA rápida cruza
# a EMA lenta (mudança de momentum de curto prazo) E o RSI confirma força na
# mesma direção, sem estar em zona extrema (evita comprar topo/vender fundo).
# TP e SL são calculados a partir do ATR (volatilidade real do momento), não
# de um % fixo — em mercado mais volátil, TP/SL abrem mais; em mercado parado,
# fecham mais. Isso tende a gerar giro mais rápido de posições, adequado a
# quem busca ciclos curtos com mais alavancagem — o que também significa
# STOPS SENDO ACIONADOS COM MAIS FREQUÊNCIA. Não é uma estratégia "sem risco".
def estrategia_momentum_scalp(symbol: str):
    df = buscar_dados(symbol, SCALP_TIMEFRAME, SCALP_LIMIT_CANDLES)

    df['ema_rapida'] = ta.ema(df['close'], length=SCALP_EMA_RAPIDA)
    df['ema_lenta'] = ta.ema(df['close'], length=SCALP_EMA_LENTA)
    df['rsi'] = ta.rsi(df['close'], length=SCALP_RSI_LENGTH)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=SCALP_ATR_LENGTH)

    minimo_necessario = max(SCALP_EMA_LENTA, SCALP_RSI_LENGTH, SCALP_ATR_LENGTH) + 3
    if len(df) < minimo_necessario:
        return None

    atual = df.iloc[-2]   # último candle fechado
    anterior = df.iloc[-3]  # candle anterior, para detectar o cruzamento

    campos = [atual['ema_rapida'], atual['ema_lenta'], atual['rsi'], atual['atr'],
              anterior['ema_rapida'], anterior['ema_lenta']]
    if any(pd.isna(v) for v in campos):
        return None

    close = atual['close']
    atr = atual['atr']
    rsi = atual['rsi']

    cruzou_para_cima = anterior['ema_rapida'] <= anterior['ema_lenta'] and atual['ema_rapida'] > atual['ema_lenta']
    cruzou_para_baixo = anterior['ema_rapida'] >= anterior['ema_lenta'] and atual['ema_rapida'] < atual['ema_lenta']

    sinal = None
    if cruzou_para_cima and (SCALP_RSI_LONG_MIN <= rsi <= SCALP_RSI_LONG_MAX):
        sinal = 'LONG'
    elif cruzou_para_baixo and (SCALP_RSI_SHORT_MIN <= rsi <= SCALP_RSI_SHORT_MAX):
        sinal = 'SHORT'

    log.info(f"[{symbol}][momentum_scalp] Preço: {close:.4f} | EMA{SCALP_EMA_RAPIDA}: {atual['ema_rapida']:.4f} | "
              f"EMA{SCALP_EMA_LENTA}: {atual['ema_lenta']:.4f} | RSI: {rsi:.1f} | ATR: {atr:.4f} | Sinal: {sinal or '-'}")

    if not sinal:
        return None

    pct_tp = min(max((atr * SCALP_ATR_MULT_TP) / close, SCALP_PCT_SL_MINIMO), SCALP_PCT_TP_MAXIMO)
    pct_sl = min(max((atr * SCALP_ATR_MULT_SL) / close, SCALP_PCT_SL_MINIMO), SCALP_PCT_TP_MAXIMO)

    return {
        'nome': 'momentum_scalp',
        'sinal': sinal,
        'preco': close,
        'pct_tp': pct_tp,
        'pct_sl': pct_sl,
        'leverage': SCALP_LEVERAGE,
    }


ESTRATEGIAS_DISPONIVEIS = {
    'bb_breakout': estrategia_bb_breakout,
    'momentum_scalp': estrategia_momentum_scalp,
}


def avaliar_estrategias(symbol: str):
    """Roda, em ordem de prioridade, cada estratégia ativa para o símbolo e
    retorna o primeiro sinal encontrado. A ordem muda sozinha conforme o
    turno (ver obter_ordem_estrategias_atual / TURNO_TARDE_*)."""
    ordem_atual = obter_ordem_estrategias_atual()
    for nome in ordem_atual:
        func = ESTRATEGIAS_DISPONIVEIS.get(nome)
        if not func:
            log.warning(f"Estratégia '{nome}' desconhecida na ordem de prioridade atual, ignorando.")
            continue
        try:
            resultado = func(symbol)
        except Exception as e:
            log.warning(f"[{symbol}] Erro ao avaliar estratégia '{nome}': {e}")
            resultado = None
        if resultado:
            return resultado
    return None


# =====================================================================
# DIAGNÓSTICO DE VIABILIDADE POR PAR
# =====================================================================
def diagnosticar_viabilidade_pares(saldo_disponivel: float):
    linhas_aviso = []

    for symbol in SYMBOLS:
        try:
            market = exchange.market(symbol)
            preco = obter_preco_referencia(symbol)

            min_amount = (market.get('limits', {}).get('amount', {}) or {}).get('min') or 0
            min_cost = (market.get('limits', {}).get('cost', {}) or {}).get('min') or 0
            custo_lote_minimo = min_amount * preco if (min_amount and preco) else 0
            minimo_efetivo = max(custo_lote_minimo, min_cost)

            notional_planejado = risk_manager.calcular_tamanho_posicao(saldo_disponivel, PCT_SL)

            log.info(f"[{symbol}] Diagnóstico: lote mínimo ≈ {minimo_efetivo:.2f} {QUOTE} | "
                     f"notional planejado ≈ {notional_planejado:.2f} {QUOTE} | saldo: {saldo_disponivel:.2f} {QUOTE}")

            if minimo_efetivo > notional_planejado or minimo_efetivo > saldo_disponivel:
                linhas_aviso.append(
                    f"⚠️ `{symbol}`: lote mínimo ≈ `{minimo_efetivo:.2f}` {QUOTE}, mas o notional "
                    f"planejado é ≈ `{notional_planejado:.2f}` {QUOTE}. Entradas provavelmente serão puladas."
                )
        except Exception as e:
            log.warning(f"[{symbol}] Não foi possível diagnosticar viabilidade: {e}")

    if linhas_aviso:
        enviar_mensagem_telegram(
            "🔎 *Diagnóstico de viabilidade dos pares*\n\n" + "\n".join(linhas_aviso) +
            "\n\nConsidere reduzir a lista para moedas de menor preço unitário ou aumentar o saldo."
        )
    else:
        log.info("Diagnóstico de viabilidade: todos os pares parecem operáveis com o saldo atual.")
        enviar_mensagem_telegram(
            f"🔎 *Diagnóstico de viabilidade dos pares*\n\n"
            f"✅ Todos os {len(SYMBOLS)} pares (`{', '.join(SYMBOLS)}`) parecem operáveis "
            f"com o saldo atual de `{saldo_disponivel:.2f}` {QUOTE}."
        )


# =====================================================================
# POSIÇÕES E ORDENS
# =====================================================================
def obter_posicoes_map() -> dict:
    mapa = {s: False for s in SYMBOLS}
    try:
        positions = exchange.fetch_positions(SYMBOLS)
        for p in positions:
            s = p.get('symbol')
            contratos = p.get('contracts') or 0
            if contratos and float(contratos) != 0 and s in mapa:
                mapa[s] = True
    except Exception as e:
        if eh_erro_rate_limit(e):
            espera = registrar_pausa_rate_limit(e)
            log.warning(f"Rate limit/instabilidade detectada ao buscar posições. Pausando por ~{espera:.0f}s.")
        else:
            log.error(f"Erro ao consultar posições: {e}")
        mapa = {s: True for s in SYMBOLS}
    return mapa


def obter_ordens_abertas_map() -> dict:
    mapa = {s: [] for s in SYMBOLS}
    for s in SYMBOLS:
        try:
            mapa[s] = exchange.fetch_open_orders(s)
        except Exception as e:
            if eh_erro_rate_limit(e):
                espera = registrar_pausa_rate_limit(e)
                log.warning(f"Rate limit/instabilidade ao buscar ordens abertas de {s}. Pausando por ~{espera:.0f}s.")
                break
            else:
                log.warning(f"[{s}] Erro ao buscar ordens abertas: {e}")
    return mapa


def limpar_ordens_orfas(symbol: str, esta_aberta: bool, ordens_do_par: list):
    if not ordens_do_par or esta_aberta:
        return
    try:
        exchange.cancel_all_orders(symbol)
        log.info(f"[{symbol}] Ordens órfãs canceladas ({len(ordens_do_par)}).")
    except Exception as e:
        if eh_erro_rate_limit(e):
            registrar_pausa_rate_limit(e)
        else:
            log.warning(f"[{symbol}] Erro ao limpar ordens órfãs: {e}")


def obter_pnl_realizado_recente(symbol: str, minutos: int = 30):
    """Soma o PnL realizado dos fills recentes. Na Hyperliquid o campo nativo
    (via ccxt `info`) costuma vir como 'closedPnl'; mantemos fallback para
    outras chaves por segurança de versão do ccxt."""
    try:
        desde = int((datetime.now(timezone.utc).timestamp() - minutos * 60) * 1000)
        trades = exchange.fetch_my_trades(symbol, since=desde, limit=50)
        pnl_total = 0.0
        encontrou = False
        for t in trades:
            info = t.get('info') or {}
            pnl_bruto = info.get('closedPnl', info.get('realizedPnl'))
            if pnl_bruto is not None:
                pnl_total += float(pnl_bruto)
                encontrou = True
        return pnl_total if encontrou else None
    except Exception as e:
        if eh_erro_rate_limit(e):
            registrar_pausa_rate_limit(e)
        else:
            log.warning(f"[{symbol}] Erro ao buscar PnL realizado: {e}")
        return None


def executar_ordem_com_tp_sl(symbol: str, tipo_entrada: str, preco_entrada: float,
                              notional_usdt: float, pct_tp: float, pct_sl: float,
                              leverage: int, nome_estrategia: str):
    try:
        exchange.cancel_all_orders(symbol)

        preco_ref = obter_preco_referencia(symbol)
        quantidade_bruta = notional_usdt / preco_ref
        quantidade = float(exchange.amount_to_precision(symbol, quantidade_bruta))

        market = exchange.market(symbol)
        min_amount = (market.get('limits', {}).get('amount', {}) or {}).get('min')
        min_cost = (market.get('limits', {}).get('cost', {}) or {}).get('min')

        if min_amount and quantidade < min_amount:
            log.warning(f"[{symbol}] Quantidade {quantidade} abaixo do mínimo {min_amount}. Entrada abortada.")
            return
        if min_cost and (quantidade * preco_ref) < min_cost:
            log.warning(f"[{symbol}] Notional abaixo do mínimo da exchange ({min_cost}). Entrada abortada.")
            return

        leverage_aplicada = configurar_alavancagem_isolada(symbol, leverage)

        notional_real = quantidade * preco_ref
        margem_necessaria = notional_real / leverage_aplicada
        saldo_disponivel = obter_saldo_disponivel_usdt()
        margem_com_folga = margem_necessaria * 1.05  # folga maior: DEX cobra taxa+funding e o preço de ref pode variar

        if margem_com_folga > saldo_disponivel:
            log.warning(
                f"[{symbol}] Margem necessária (~{margem_necessaria:.2f} {QUOTE}) excede o saldo "
                f"disponível ({saldo_disponivel:.2f} {QUOTE}). Entrada abortada."
            )
            return

        if tipo_entrada == 'LONG':
            side_entrada, side_saida = 'buy', 'sell'
            preco_tp = preco_entrada * (1 + pct_tp)
            preco_sl = preco_entrada * (1 - pct_sl)
        else:
            side_entrada, side_saida = 'sell', 'buy'
            preco_tp = preco_entrada * (1 - pct_tp)
            preco_sl = preco_entrada * (1 + pct_sl)

        preco_tp = float(exchange.price_to_precision(symbol, preco_tp))
        preco_sl = float(exchange.price_to_precision(symbol, preco_sl))

        # Ordem a mercado na Hyperliquid EXIGE um preço de referência (proteção
        # de slippage, tolerância padrão ~5%). Usamos o mid price atual.
        exchange.create_order(
            symbol=symbol, type='market', side=side_entrada, amount=quantidade, price=preco_ref,
        )
        log.info(f"[{symbol}] ✅ [{nome_estrategia}] Ordem {tipo_entrada} executada | qty={quantidade} | "
                 f"notional≈{notional_usdt:.2f} {QUOTE} | leverage={leverage_aplicada}x")

        # TP e SL como ordens de redução (reduceOnly) com gatilho de preço.
        # Cada uma é independente; quando uma dispara, a outra deve ser
        # cancelada no próximo ciclo por limpar_ordens_orfas().
        exchange.create_order(
            symbol=symbol, type='market', side=side_saida, amount=quantidade, price=preco_ref,
            params={'takeProfitPrice': preco_tp, 'reduceOnly': True},
        )
        exchange.create_order(
            symbol=symbol, type='market', side=side_saida, amount=quantidade, price=preco_ref,
            params={'stopLossPrice': preco_sl, 'reduceOnly': True},
        )

        registrar_trade_csv(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol=symbol, estrategia=nome_estrategia, tipo='ENTRADA', entrada=preco_entrada,
            tp=preco_tp, sl=preco_sl, quantidade=quantidade, notional_usdt=notional_usdt,
            leverage=leverage_aplicada,
        )

        enviar_mensagem_telegram(
            f"🚀 *NOVA OPERAÇÃO ({tipo_entrada})* — `{nome_estrategia}`\n\n"
            f"Par: `{symbol}`\nEntrada: `{preco_entrada:.4f}`\n"
            f"🎯 TP: `{preco_tp:.4f}` | 🛑 SL: `{preco_sl:.4f}`\n"
            f"Notional: `{notional_usdt:.2f}` {QUOTE} | Alavancagem: `{leverage_aplicada}x`"
        )

    except Exception as e:
        if eh_erro_rate_limit(e):
            registrar_pausa_rate_limit(e)
        log.error(f"[{symbol}] Erro ao executar ordens: {e}")


# =====================================================================
# CICLO PRINCIPAL
# =====================================================================
estado_posicoes = {}


def processar_par(symbol: str, aberta_agora: bool, ordens_do_par: list, total_posicoes_abertas: int):
    global ultima_atividade
    ultima_atividade = time.time()

    if em_pausa():
        return

    limpar_ordens_orfas(symbol, aberta_agora, ordens_do_par)

    estava_aberta = estado_posicoes.get(symbol, False)

    if estava_aberta and not aberta_agora:
        pnl = obter_pnl_realizado_recente(symbol)
        if pnl is not None:
            emoji = "✅" if pnl >= 0 else "🔴"
            enviar_mensagem_telegram(
                f"{emoji} *Posição encerrada em* `{symbol}`\nPnL realizado: `{pnl:+.2f}` {QUOTE}"
            )
        else:
            enviar_mensagem_telegram(f"ℹ️ Posição encerrada em `{symbol}` (PnL não pôde ser confirmado).")
        registrar_trade_csv(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol=symbol, tipo='SAIDA', pnl_usdt=pnl if pnl is not None else '',
        )

    estado_posicoes[symbol] = aberta_agora

    if aberta_agora:
        return

    if total_posicoes_abertas >= MAX_POSICOES_SIMULTANEAS:
        return

    log.info(f"[{symbol}] Buscando candles e avaliando estratégias ({', '.join(obter_ordem_estrategias_atual())})...")
    resultado = avaliar_estrategias(symbol)
    if not resultado:
        return

    saldo = obter_saldo_disponivel_usdt()
    if risk_manager.checar_kill_switch(saldo):
        return

    notional = risk_manager.calcular_tamanho_posicao(saldo, resultado['pct_sl'])
    if notional <= 0:
        log.warning(f"[{symbol}] Notional calculado inválido, entrada abortada.")
        return

    executar_ordem_com_tp_sl(
        symbol, resultado['sinal'], resultado['preco'], notional,
        resultado['pct_tp'], resultado['pct_sl'], resultado['leverage'], resultado['nome'],
    )


def enviar_resumo_diario():
    stats = resumo_trades_csv()
    total = stats['total_saidas']
    win_rate = (stats['vitorias'] / total * 100) if total else 0.0

    msg = (
        f"📊 *Resumo do dia (UTC)*\n\n"
        f"Trades fechados: `{total}`\n"
        f"Vitórias: `{stats['vitorias']}` | Derrotas: `{stats['derrotas']}`\n"
        f"Win rate: `{win_rate:.1f}%`\n"
        f"PnL total: `{stats['pnl_total']:+.2f}` {QUOTE}"
    )
    enviar_mensagem_telegram(msg)
    enviar_arquivo_telegram(TRADES_LOG_PATH, "Histórico completo de trades (CSV)")


def executar_bot():
    log.info("=" * 60)
    log.info(f"BOT SNIPER BOLADÃO (HYPERLIQUID) INICIADO | Pares: {', '.join(SYMBOLS)}")
    log.info(f"Ordem de prioridade padrão: {', '.join(ESTRATEGIAS_ORDEM_PADRAO)}")
    log.info(f"Ordem de prioridade no turno da tarde ({TURNO_TARDE_INICIO_HORA}h-{TURNO_TARDE_FIM_HORA}h, {TIMEZONE_OPERACIONAL}): "
             f"{', '.join(ESTRATEGIAS_ORDEM_TARDE)}")
    log.info("=" * 60)

    try:
        saldo_inicial = obter_saldo_disponivel_usdt()
        diagnosticar_viabilidade_pares(saldo_inicial)
    except Exception as e:
        log.warning(f"Erro ao rodar diagnóstico de viabilidade dos pares: {e}")

    enviar_mensagem_telegram(
        f"🤖 *Bot Sniper Boladão (Hyperliquid) Inicializado!*\n"
        f"Monitorando: `{', '.join(SYMBOLS)}`\n"
        f"Estratégias (ordem atual): `{', '.join(obter_ordem_estrategias_atual())}`\n"
        f"(padrão: `{', '.join(ESTRATEGIAS_ORDEM_PADRAO)}` | tarde {TURNO_TARDE_INICIO_HORA}h-{TURNO_TARDE_FIM_HORA}h: "
        f"`{', '.join(ESTRATEGIAS_ORDEM_TARDE)}`)\n"
        f"Máx. posições simultâneas: `{MAX_POSICOES_SIMULTANEAS}`\n"
        f"Risco por trade: `{RISCO_POR_TRADE_PCT*100:.1f}%` | Drawdown máx. diário: `{MAX_DRAWDOWN_DIARIO_PCT*100:.1f}%`\n"
        f"{'⚠️ MODO TESTNET' if USE_TESTNET else ''}"
    )

    erros_consecutivos = 0
    ultima_data_resumo = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    ultimo_heartbeat = time.time()
    alertou_rate_limit = False

    while True:
        try:
            if em_pausa():
                restante = pausa_ate - time.time()
                if not alertou_rate_limit:
                    enviar_mensagem_telegram(
                        f"🚫 *Instabilidade/rate limit detectado na Hyperliquid.*\nPausando TODAS as chamadas "
                        f"à API por `{restante/60:.1f}` min."
                    )
                    alertou_rate_limit = True
                    log.warning(f"Rate limit ativo. Pausa total até recuperar (~{restante:.0f}s restantes).")
                time.sleep(min(30, max(1, restante)))
                continue
            elif alertou_rate_limit:
                enviar_mensagem_telegram("✅ Pausa por rate limit terminou. Retomando operação normal.")
                alertou_rate_limit = False

            hoje = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if hoje != ultima_data_resumo:
                enviar_resumo_diario()
                ultima_data_resumo = hoje

            if (time.time() - ultimo_heartbeat) >= HEARTBEAT_HORAS * 3600:
                try:
                    saldo_atual = obter_saldo_disponivel_usdt()
                    mapa_pos_hb = obter_posicoes_map()
                    abertas = sum(1 for v in mapa_pos_hb.values() if v)
                    enviar_mensagem_telegram(
                        f"💓 *Heartbeat* — bot ativo e monitorando.\n"
                        f"Pares: `{', '.join(SYMBOLS)}`\n"
                        f"Posições abertas: `{abertas}` | Saldo: `{saldo_atual:.2f}` {QUOTE}"
                    )
                except Exception as e:
                    log.warning(f"Erro ao montar heartbeat: {e}")
                ultimo_heartbeat = time.time()

            mapa_posicoes = obter_posicoes_map()
            mapa_ordens = obter_ordens_abertas_map()
            total_abertas = sum(1 for v in mapa_posicoes.values() if v)

            for symbol in SYMBOLS:
                processar_par(symbol, mapa_posicoes[symbol], mapa_ordens[symbol], total_abertas)
            erros_consecutivos = 0
        except ccxt.AuthenticationError as e:
            log.warning(f"Erro de autenticação — verifique WALLET_ADDRESS/PRIVATE_KEY: {e}")
            enviar_mensagem_telegram(f"🛑 *Erro de autenticação na Hyperliquid.* Bot pausado por 5 min.\n`{e}`")
            time.sleep(300)
            continue
        except Exception as e:
            if eh_erro_rate_limit(e):
                registrar_pausa_rate_limit(e)
                log.warning(f"Rate limit/instabilidade detectada no ciclo principal: {e}")
            else:
                erros_consecutivos += 1
                if erros_consecutivos >= 5:
                    log.error(f"{erros_consecutivos} erros consecutivos no ciclo principal: {e}")
                else:
                    log.warning(f"Erro no ciclo principal: {e}")

        time.sleep(CICLO_SEGUNDOS)


# =====================================================================
# EXECUÇÃO PRINCIPAL
# =====================================================================
if __name__ == '__main__':
    threading.Thread(target=iniciar_web, daemon=True).start()
    threading.Thread(target=watchdog_thread, daemon=True).start()
    executar_bot()
