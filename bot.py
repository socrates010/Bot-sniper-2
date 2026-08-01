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
# Mercados HIP-3 (deploy de terceiros, ex: ações tokenizadas como SKHYNIX) podem
# usar um formato com prefixo de dex (ex: "xyz:SKHYNIX/USDC:USDC") — o bot tenta
# resolver automaticamente no startup (ver resolver_e_validar_symbols) e avisa
# no log/Telegram qual símbolo exato foi encontrado ou se precisa de ajuste manual.
#
# ATENÇÃO SKHYNIX: é uma ação tokenizada (SK Hynix) via mercado HIP-3 de
# terceiros (Trade.xyz), com oráculo próprio e cross margin — teve um evento
# de ~US$57M em liquidações em 28/jul/2026 por falha de oráculo pré-mercado.
# É estruturalmente mais arriscado que os pares cripto major abaixo.
SYMBOLS = [s.strip() for s in os.getenv(
    'SYMBOLS', 'BTC/USDC:USDC,ETH/USDC:USDC,SOL/USDC:USDC,SKHYNIX/USDC:USDC'
).split(',') if s.strip()]

# Cópia da lista original de SYMBOLS: são os pares SEMPRE operados, independente
# do que a seleção dinâmica de oportunidades (mais abaixo) decidir. A seleção
# dinâmica só ADICIONA pares extras a este núcleo fixo, nunca remove um destes.
SIMBOLOS_FIXOS = list(SYMBOLS)

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


# ATENÇÃO: a Hyperliquid só aceita intervalos específicos de candle:
# 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d, 1w, 1M. Qualquer outro
# valor (ex: "2m") faz a API retornar 422 (erro de deserialização, não de
# rede/conta) — o bot valida isso já no startup para falhar rápido e claro.
_INTERVALOS_VALIDOS_HYPERLIQUID = {
    '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '8h', '12h', '1d', '3d', '1w', '1M',
}


def _validar_timeframe(nome_var: str, valor: str) -> str:
    if valor not in _INTERVALOS_VALIDOS_HYPERLIQUID:
        raise RuntimeError(
            f"{nome_var}='{valor}' não é um intervalo válido na Hyperliquid. "
            f"Use um destes: {', '.join(sorted(_INTERVALOS_VALIDOS_HYPERLIQUID))}"
        )
    return valor


# --- Parâmetros da estratégia Sniper (BB breakout) ---
TIMEFRAME = _validar_timeframe('TIMEFRAME', os.getenv('TIMEFRAME', '5m'))
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
# Timeframe 3m -> 2m (pedido original) e EMA9/21 -> EMA5/13: no timeframe
# mais curto, médias mais lentas demoram demais para cruzar quando o mercado
# perde amplitude (típico de tarde) — médias mais curtas cruzam com mais
# frequência, gerando mais gatilhos. IMPORTANTE: "2m" não existe como
# intervalo na Hyperliquid (ver validação acima) — usamos "1m", o intervalo
# válido mais rápido disponível, para manter a intenção original (girar mais
# rápido que 3m). Se ficar ruidoso demais, ajuste via env var SCALP_TIMEFRAME.
SCALP_TIMEFRAME = _validar_timeframe('SCALP_TIMEFRAME', os.getenv('SCALP_TIMEFRAME', '1m'))
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

# --- Seleção dinâmica de símbolos por "melhor oportunidade" ---
# Desligada por padrão (SELECAO_DINAMICA_ATIVA=false) para não mudar o
# comportamento de quem já está rodando com a lista fixa. Quando ligada, a
# cada INTERVALO_SELECAO_DINAMICA_MINUTOS o bot re-avalia um "universo" de
# símbolos candidatos (UNIVERSO_DINAMICO_SIMBOLOS) por volume 24h ou variação
# 24h, aplica o mesmo piso de market cap acima, e adiciona os melhores
# QTD_SIMBOLOS_DINAMICOS colocados à lista fixa (SIMBOLOS_FIXOS) — nunca
# remove um símbolo fixo, e nunca remove um símbolo com posição aberta.
SELECAO_DINAMICA_ATIVA = os.getenv('SELECAO_DINAMICA_ATIVA', 'false').lower() == 'true'
UNIVERSO_DINAMICO_SIMBOLOS = [s.strip() for s in os.getenv(
    'UNIVERSO_DINAMICO_SIMBOLOS',
    'AVAX/USDC:USDC,ARB/USDC:USDC,OP/USDC:USDC,DOGE/USDC:USDC,XRP/USDC:USDC,'
    'LINK/USDC:USDC,SUI/USDC:USDC,APT/USDC:USDC,NEAR/USDC:USDC,TIA/USDC:USDC,'
    'INJ/USDC:USDC,SEI/USDC:USDC,HYPE/USDC:USDC'
).split(',') if s.strip()]
QTD_SIMBOLOS_DINAMICOS = int(os.getenv('QTD_SIMBOLOS_DINAMICOS', 2))
# 'volume_24h'   -> prioriza os candidatos com maior volume 24h (liquidez/interesse)
# 'variacao_24h' -> prioriza os candidatos com maior |variação| de preço nas últimas 24h (momentum)
CRITERIO_SELECAO_DINAMICA = os.getenv('CRITERIO_SELECAO_DINAMICA', 'volume_24h')
INTERVALO_SELECAO_DINAMICA_MINUTOS = float(os.getenv('INTERVALO_SELECAO_DINAMICA_MINUTOS', 60))

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


def resolver_e_validar_symbols(symbols_desejados: list) -> list:
    """Valida cada símbolo contra os mercados realmente carregados da Hyperliquid.
    Mercados HIP-3 (deploy de terceiros, ex: ações tokenizadas como SKHYNIX) às
    vezes usam um formato com prefixo de dex (ex: "xyz:SKHYNIX/USDC:USDC") que
    não bate exatamente com o que a gente escreveria "no chute" — em vez de
    travar o bot inteiro por causa de 1 símbolo, tentamos achar por aproximação
    do nome base e avisamos qual símbolo exato foi usado (ou ignoramos esse
    par pontualmente, sem derrubar os outros)."""
    symbols_validos = []
    for symbol in symbols_desejados:
        if symbol in exchange.markets:
            symbols_validos.append(symbol)
            continue

        base_procurada = symbol.split('/')[0].split(':')[-1].upper()
        candidatos = [
            m for m in exchange.markets
            if base_procurada == m.split('/')[0].split(':')[-1].upper()
        ]

        if len(candidatos) == 1:
            log.warning(f"Símbolo '{symbol}' não encontrado exatamente nos mercados da Hyperliquid. "
                        f"Usando '{candidatos[0]}' (achado por aproximação do nome base '{base_procurada}').")
            symbols_validos.append(candidatos[0])
        elif len(candidatos) > 1:
            log.error(f"Símbolo '{symbol}' não encontrado exatamente e há {len(candidatos)} candidatos "
                      f"ambíguos: {candidatos[:10]}. Configure o símbolo EXATO na variável SYMBOLS. "
                      f"Este par foi IGNORADO até a configuração ser corrigida.")
            enviar_mensagem_telegram(
                f"⚠️ Símbolo `{symbol}` ambíguo ({len(candidatos)} candidatos). "
                f"Configure o exato em SYMBOLS. Ignorado por enquanto."
            )
        else:
            log.error(f"Símbolo '{symbol}' não encontrado nos mercados da Hyperliquid (nem por "
                      f"aproximação). Este par foi IGNORADO. Confira o nome exato no app da Hyperliquid.")
            enviar_mensagem_telegram(
                f"⚠️ Símbolo `{symbol}` não encontrado na Hyperliquid. Ignorado por enquanto — "
                f"confira o nome exato do mercado (pode ter prefixo de dex, ex: `xyz:{symbol}`)."
            )

    return symbols_validos


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

# Só agora (com enviar_mensagem_telegram já disponível) rodamos a validação
# dos símbolos configurados contra os mercados carregados da Hyperliquid.
if exchange.markets:
    SYMBOLS[:] = resolver_e_validar_symbols(SYMBOLS)
    SIMBOLOS_FIXOS[:] = [s for s in SIMBOLOS_FIXOS if s in SYMBOLS] or list(SYMBOLS)
    log.info(f"Símbolos ativos após validação: {', '.join(SYMBOLS) if SYMBOLS else '(nenhum válido!)'}")
else:
    log.error("Mercados não carregados — símbolos não puderam ser validados. "
              "Usando a lista bruta de SYMBOLS por enquanto; erros de símbolo aparecerão nas chamadas seguintes.")


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


_ja_logou_balance_bruto = False


def obter_saldo_disponivel_usdt() -> float:
    global _ja_logou_balance_bruto
    saldo = exchange.fetch_balance()
    info_quote = saldo.get(QUOTE, {})
    disponivel = info_quote.get('free') or info_quote.get('total') or 0

    if float(disponivel) <= 0 and not _ja_logou_balance_bruto:
        # Log único (evita spam a cada ciclo): ajuda a diferenciar "saldo
        # realmente zerado" de "bug de nome de campo no retorno da API".
        _ja_logou_balance_bruto = True
        log.warning(f"Saldo em {QUOTE} lido como 0. Retorno bruto de fetch_balance() (debug único): "
                    f"chaves disponíveis={list(saldo.keys())} | conteúdo de '{QUOTE}'={info_quote}")

    return float(disponivel)


def configurar_alavancagem_isolada(symbol: str, leverage: int):
    leverage = min(leverage, MAX_LEVERAGE_PERMITIDO)
    try:
        exchange.set_margin_mode('isolated', symbol, params={'leverage': leverage})
    except Exception as e:
        log.warning(f"[{symbol}] Aviso ao configurar margem/alavancagem ({leverage}x): {e}")
    return leverage


# --- Filtro de liquidez / market cap (via CoinGecko, ver bloco de config) ---
_mcap_cache = {}  # base (str) -> (valor_usd: float | None, timestamp: float)


def obter_market_cap(base: str):
    """Retorna o market cap em USD do token (via CoinGecko, com cache), ou
    None se não for possível determinar (símbolo sem mapeamento, erro de rede,
    etc.). Retornar None é tratado como 'não bloquear a entrada' (fail-open) —
    a ideia é ajudar a evitar tokens pequenos, não travar o bot inteiro se o
    CoinGecko estiver fora do ar ou faltar mapeamento pra um símbolo novo."""
    base = base.upper()

    cache_hit = _mcap_cache.get(base)
    if cache_hit and (time.time() - cache_hit[1]) < MARKET_CAP_CACHE_MINUTOS * 60:
        return cache_hit[0]

    coingecko_id = COINGECKO_IDS.get(base)
    if not coingecko_id:
        log.warning(f"[market cap] '{base}' sem id do CoinGecko mapeado (COINGECKO_IDS/"
                    f"COINGECKO_IDS_EXTRA). Filtro de liquidez ignorado para este símbolo.")
        _mcap_cache[base] = (None, time.time())
        return None

    try:
        resp = requests.get(
            'https://api.coingecko.com/api/v3/simple/price',
            params={'ids': coingecko_id, 'vs_currencies': 'usd', 'include_market_cap': 'true'},
            timeout=8,
        )
        resp.raise_for_status()
        dados = resp.json()
        mcap = (dados.get(coingecko_id) or {}).get('usd_market_cap')
        valor = float(mcap) if mcap is not None else None
        _mcap_cache[base] = (valor, time.time())
        return valor
    except Exception as e:
        log.warning(f"[market cap] Erro ao consultar CoinGecko para '{base}' ({coingecko_id}): {e}")
        # Em erro de rede, reaproveita o valor antigo do cache se houver,
        # em vez de bloquear a entrada por uma falha temporária da API externa.
        return cache_hit[0] if cache_hit else None


def passa_no_filtro_market_cap(symbol: str) -> bool:
    if not FILTRO_MARKET_CAP_ATIVO:
        return True
    base = symbol.split('/')[0].split(':')[-1]
    mcap = obter_market_cap(base)
    if mcap is None:
        return True  # fail-open: sem dado, não bloqueia
    if mcap < MARKET_CAP_MINIMO_USD:
        log.info(f"[{symbol}] Bloqueado pelo filtro de market cap: "
                 f"${mcap:,.0f} < piso de ${MARKET_CAP_MINIMO_USD:,.0f}.")
        return False
    return True


# --- Seleção dinâmica de símbolos por "melhor oportunidade" (ver config) ---
def selecionar_melhores_oportunidades() -> list:
    """Avalia o UNIVERSO_DINAMICO_SIMBOLOS por volume ou variação 24h, aplica
    o filtro de market cap, e retorna até QTD_SIMBOLOS_DINAMICOS símbolos —
    os melhores colocados no critério escolhido. Nunca inclui um símbolo que
    já esteja em SIMBOLOS_FIXOS (esses já são operados de qualquer forma)."""
    candidatos = []
    for symbol in UNIVERSO_DINAMICO_SIMBOLOS:
        if symbol in SIMBOLOS_FIXOS:
            continue
        if symbol not in exchange.markets:
            continue  # símbolo do universo não existe/mudou de nome na Hyperliquid
        try:
            ticker = exchange.fetch_ticker(symbol)
            volume_24h = float(ticker.get('quoteVolume') or 0)
            variacao_24h = abs(float(ticker.get('percentage') or 0))
            candidatos.append((symbol, volume_24h, variacao_24h))
        except Exception as e:
            log.warning(f"[seleção dinâmica] Erro ao avaliar {symbol}: {e}")

    if not candidatos:
        return []

    if CRITERIO_SELECAO_DINAMICA == 'variacao_24h':
        candidatos.sort(key=lambda c: c[2], reverse=True)
    else:
        candidatos.sort(key=lambda c: c[1], reverse=True)

    aprovados = []
    for symbol, volume_24h, variacao_24h in candidatos:
        if not passa_no_filtro_market_cap(symbol):
            continue
        aprovados.append(symbol)
        log.info(f"[seleção dinâmica] Candidato aprovado: {symbol} "
                 f"(volume24h≈${volume_24h:,.0f}, variação24h≈{variacao_24h:.2f}%)")
        if len(aprovados) >= QTD_SIMBOLOS_DINAMICOS:
            break

    return aprovados


def atualizar_selecao_dinamica():
    """Atualiza SYMBOLS in-place: mantém SIMBOLOS_FIXOS + posições abertas +
    os melhores candidatos do momento. Muta a lista existente (SYMBOLS[:] = ...)
    em vez de reatribuir, porque várias funções guardam referência a esse
    mesmo objeto de lista."""
    if not SELECAO_DINAMICA_ATIVA:
        return

    try:
        simbolos_com_posicao_aberta = [s for s, aberta in obter_posicoes_map().items() if aberta]
    except Exception as e:
        log.warning(f"[seleção dinâmica] Erro ao checar posições abertas antes de atualizar: {e}")
        simbolos_com_posicao_aberta = []

    melhores = selecionar_melhores_oportunidades()

    novos_symbols = list(SIMBOLOS_FIXOS)
    for s in simbolos_com_posicao_aberta + melhores:
        if s not in novos_symbols:
            novos_symbols.append(s)

    if set(novos_symbols) != set(SYMBOLS):
        log.info(f"[seleção dinâmica] Lista de símbolos atualizada: {', '.join(novos_symbols)}")
        enviar_mensagem_telegram(
            f"🔄 *Seleção dinâmica atualizou os pares operados:*\n`{', '.join(novos_symbols)}`\n"
            f"(fixos: `{', '.join(SIMBOLOS_FIXOS)}`)"
        )
    SYMBOLS[:] = novos_symbols


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


def _cancelar_lista_ordens(symbol: str, ordens: list):
    """Cancela uma lista de ordens já buscada. A Hyperliquid (via ccxt) não
    implementa cancelAllOrders() ('is not supported yet') — por isso
    cancelamos manualmente: tenta em lote via cancel_orders(ids, symbol) e,
    se isso falhar por qualquer motivo, cai para cancelar uma a uma."""
    ids = [o['id'] for o in ordens if o.get('id')]
    if not ids:
        return

    try:
        exchange.cancel_orders(ids, symbol)
        return
    except Exception as e:
        if eh_erro_rate_limit(e):
            registrar_pausa_rate_limit(e)
            return
        log.warning(f"[{symbol}] cancel_orders em lote falhou ({e}), cancelando ordem a ordem...")

    for order_id in ids:
        try:
            exchange.cancel_order(order_id, symbol)
        except Exception as e2:
            if eh_erro_rate_limit(e2):
                registrar_pausa_rate_limit(e2)
                return
            log.warning(f"[{symbol}] Erro ao cancelar ordem {order_id}: {e2}")


def cancelar_todas_ordens(symbol: str):
    """Busca as ordens abertas do símbolo e cancela todas (ver _cancelar_lista_ordens)."""
    try:
        ordens = exchange.fetch_open_orders(symbol)
    except Exception as e:
        if eh_erro_rate_limit(e):
            registrar_pausa_rate_limit(e)
        else:
            log.warning(f"[{symbol}] Erro ao buscar ordens abertas para cancelar: {e}")
        return
    _cancelar_lista_ordens(symbol, ordens)


def limpar_ordens_orfas(symbol: str, esta_aberta: bool, ordens_do_par: list):
    if not ordens_do_par or esta_aberta:
        return
    try:
        _cancelar_lista_ordens(symbol, ordens_do_par)
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
        cancelar_todas_ordens(symbol)

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

    if not passa_no_filtro_market_cap(symbol):
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
        log.warning(
            f"[{symbol}] Notional calculado inválido ({notional:.4f}), entrada abortada. "
            f"Saldo lido na conta Perps: {saldo:.4f} {QUOTE} | pct_sl: {resultado['pct_sl']*100:.3f}%. "
            f"Se o saldo estiver em 0, confira se o USDC está na carteira PERPS da Hyperliquid "
            f"(não na Spot) e vinculado ao WALLET_ADDRESS configurado."
        )
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
    log.info(f"Símbolos fixos: {', '.join(SIMBOLOS_FIXOS)} | Seleção dinâmica: "
             f"{'ATIVA' if SELECAO_DINAMICA_ATIVA else 'desativada'}")
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
        f"Seleção dinâmica de pares: `{'ativa' if SELECAO_DINAMICA_ATIVA else 'desativada'}`\n"
        f"Máx. posições simultâneas: `{MAX_POSICOES_SIMULTANEAS}`\n"
        f"Risco por trade: `{RISCO_POR_TRADE_PCT*100:.1f}%` | Drawdown máx. diário: `{MAX_DRAWDOWN_DIARIO_PCT*100:.1f}%`\n"
        f"{'⚠️ MODO TESTNET' if USE_TESTNET else ''}"
    )

    erros_consecutivos = 0
    ultima_data_resumo = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    ultimo_heartbeat = time.time()
    ultima_selecao_dinamica = 0.0  # força uma primeira atualização já no início, se ativa
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

            if SELECAO_DINAMICA_ATIVA and (time.time() - ultima_selecao_dinamica) >= INTERVALO_SELECAO_DINAMICA_MINUTOS * 60:
                try:
                    atualizar_selecao_dinamica()
                except Exception as e:
                    log.warning(f"Erro ao atualizar seleção dinâmica de símbolos: {e}")
                ultima_selecao_dinamica = time.time()

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
