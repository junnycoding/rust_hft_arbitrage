import aiohttp
import asyncio
import time
import hmac
import hashlib
import uuid
from typing import Dict, Any

# ==============================================================================
# --- 사용자 설정 영역 ---
# ==============================================================================

class TradingConfig:
    # Bybit 설정
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""

    # Binance 설정
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # --- 트레이딩 파라미터 ---
    target_symbol: str = 'HIFIUSDT'
    size: str = "9000"
    max_entries: int = 1
    entry_gap: int = 985
    target_gap: int = 20

    @property
    def target_price(self) -> int:
        """목표가는 진입가 + 타겟갭으로 계산됩니다."""
        return self.entry_gap + self.target_gap

# ==============================================================================
# --- 웹소켓 매니저 클래스 ---
# ==============================================================================

class BybitWebsocketManager:
    """Bybit 거래용 Private WebSocket 연결 및 주문을 관리합니다."""
    def __init__(self, session: aiohttp.ClientSession, api_key: str, api_secret: str, ready_event: asyncio.Event):
        self._session = session
        self._api_key = api_key
        self._api_secret = api_secret
        self._ws = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self.ws_url = "wss://stream.bybit.com/v5/trade"
        self._ready_event = ready_event
        self._heartbeat_task = None

    async def connect(self):
        while True:
            print("Bybit Trade WebSocket 연결 시도...")
            self._ready_event.clear()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                self._ws = await self._session.ws_connect(self.ws_url, timeout=timeout)
                print("✅ Bybit Trade WebSocket 연결 성공. 인증 시작...")
                await self._authenticate()
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                await self._receiver_loop()
            except Exception as e:
                print(f"❌ Bybit Trade WebSocket 연결 또는 수신 중 오류: {e}")
            print("Bybit Trade WebSocket 5초 후 재연결...")
            if self._ws and not self._ws.closed:
                await self._ws.close()
            await asyncio.sleep(5)

    async def _heartbeat_loop(self):
        while self._ws and not self._ws.closed:
            try:
                await asyncio.sleep(20)
                await self._ws.send_json({"op": "ping"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"하트비트 전송 오류: {e}")
                break

    async def _authenticate(self):
        expires = int((time.time() + 10) * 1000)
        signature = hmac.new(self._api_secret.encode('utf-8'), f"GET/realtime{expires}".encode('utf-8'), hashlib.sha256).hexdigest()
        await self._ws.send_json({"op": "auth", "args": [self._api_key, expires, signature]})

    async def _receiver_loop(self):
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                if data.get("op") == "pong":
                    continue
                if data.get("op") == "auth":
                    if data.get("retCode") == 0:
                        print("✅ Bybit 인증 완료. 주문 가능 상태.")
                        self._ready_event.set()
                    else:
                        raise ConnectionError(f"Bybit Auth Failed: {data.get('retMsg')}")
                req_id = data.get("reqId")
                if req_id and req_id in self._pending_requests:
                    self._pending_requests.pop(req_id).set_result(data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise ConnectionError("Bybit WebSocket connection closed.")

    async def place_order(self, **kwargs) -> Dict[str, Any]:
        if not self._ready_event.is_set():
            return {"error": "Bybit manager not ready."}
        req_id = f"order-{uuid.uuid4().hex[:8]}"
        kwargs.update({"category": "linear"})
        payload = {
            "reqId": req_id,
            "header": {"X-BAPI-TIMESTAMP": str(int(time.time() * 1000)), "X-BAPI-RECV-WINDOW": "5000"},
            "op": "order.create", "args": [kwargs]
        }
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future
        await self._ws.send_json(payload)
        try:
            return await asyncio.wait_for(future, timeout=10)
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            return {"error": "Order response timed out"}

class BinanceWebsocketManager:
    """Binance 거래용 Private WebSocket 연결 및 주문을 관리합니다."""
    def __init__(self, session: aiohttp.ClientSession, api_key: str, api_secret: str, ready_event: asyncio.Event):
        self._session = session
        self._api_key = api_key
        self._api_secret = api_secret
        self._ws = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self.ws_url = "wss://ws-fapi.binance.com/ws-fapi/v1"
        self._ready_event = ready_event

    async def connect(self):
        while True:
            print("Binance Private WebSocket 연결 시도...")
            self._ready_event.clear()
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                self._ws = await self._session.ws_connect(self.ws_url, timeout=timeout)
                print("✅ Binance Private WebSocket 연결 성공.")
                self._ready_event.set()
                await self._receiver_loop()
            except Exception as e:
                print(f"❌ Binance Private WebSocket 연결 또는 수신 중 오류: {e}")
            print("Binance Private WebSocket 5초 후 재연결...")
            if self._ws and not self._ws.closed:
                await self._ws.close()
            await asyncio.sleep(5)

    async def _receiver_loop(self):
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                req_id = data.get("id")
                if req_id and req_id in self._pending_requests:
                    self._pending_requests.pop(req_id).set_result(data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise ConnectionError("Binance WebSocket connection closed.")

    async def place_order(self, **kwargs) -> Dict[str, Any]:
        if not self._ready_event.is_set():
            return {"error": "Binance manager not ready."}
        req_id = f"order-{uuid.uuid4().hex[:8]}"
        params = {'apiKey': self._api_key, 'timestamp': str(int(time.time() * 1000))}
        params.update(kwargs)
        if 'reduceOnly' in params and isinstance(params['reduceOnly'], bool):
            params['reduceOnly'] = 'true' if params['reduceOnly'] else 'false'
        sorted_keys = sorted(params.keys())
        query_string = "&".join([f"{key}={params[key]}" for key in sorted_keys])
        params['signature'] = hmac.new(self._api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
        payload = {"id": req_id, "method": "order.place", "params": params}
        future = asyncio.get_running_loop().create_future()
        self._pending_requests[req_id] = future
        await self._ws.send_json(payload)
        try:
            return await asyncio.wait_for(future, timeout=10)
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            return {"error": "Order response timed out"}

# ==============================================================================
# --- 메인 트레이딩 봇 클래스 ---
# ==============================================================================

class ArbitrageBot:
    def __init__(self, config: TradingConfig):
        self.config = config
        
        # 봇의 상태 (State) 관리
        self.entry_count = 0
        self.position_status = "NONE" # "NONE", "BYBIT_LONG", "BINANCE_LONG"

        # 마켓 데이터
        self.bybit_data: Dict[str, Dict[str, float]] = {}
        self.binance_best_ask: Dict[str, float] = {}
        self.binance_best_bid: Dict[str, float] = {}

        # 웹소켓 매니저
        self.bybit_ws_manager: BybitWebsocketManager = None
        self.binance_ws_manager: BinanceWebsocketManager = None
        self.bybit_ready = asyncio.Event()
        self.binance_ready = asyncio.Event()

    def _bybit_processor(self, data: Dict[str, Any]):
        if 'data' in data and data.get('type') in ('snapshot', 'delta'):
            symbol = data['data']['s']
            if symbol in self.bybit_data:
                if data['data']['a']: self.bybit_data[symbol]['a'] = float(data['data']['a'][0][0])
                if data['data']['b']: self.bybit_data[symbol]['b'] = float(data['data']['b'][0][0])

    def _binance_processor(self, data: Dict[str, Any]):
        if 'stream' in data and data['stream'].endswith('@bookTicker'):
            ticker_data = data['data']
            symbol = ticker_data['s']
            if symbol in self.binance_best_ask:
                self.binance_best_ask[symbol] = float(ticker_data['a'])
                self.binance_best_bid[symbol] = float(ticker_data['b'])
    
    async def _data_stream_task(self, name, url, processor, subscription=None):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        print(f"✅ {name} Public 웹소켓 연결 성공.")
                        if subscription:
                            await ws.send_json(subscription)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                processor(msg.json())
            except Exception as e:
                print(f"❌ {name} Public 웹소켓 오류: {e}. 5초 후 재연결...")
                await asyncio.sleep(5)

    async def _initialize_market_data(self, session: aiohttp.ClientSession) -> bool:
        print("초기 데이터 로딩 시작...")
        try:
            bybit_url = "https://api.bybit.com/v5/market/tickers?category=linear"
            async with session.get(bybit_url) as response:
                response.raise_for_status()
                data = await response.json()
                bybit_ticker = [i['symbol'] for i in data['result']['list'] if i['symbol'].endswith('USDT')]
                self.bybit_data.update({symbol: {'a': 0, 'b': 0} for symbol in bybit_ticker})

            binance_url = 'https://fapi.binance.com/fapi/v1/ticker/bookTicker'
            async with session.get(binance_url) as response:
                response.raise_for_status()
                data = await response.json()
                usdt_tickers = [i for i in data if i['symbol'].endswith('USDT')]
                for i in usdt_tickers:
                    self.binance_best_ask[i['symbol']] = float(i['askPrice'])
                    self.binance_best_bid[i['symbol']] = float(i['bidPrice'])
            print("초기 데이터 로딩 완료...")
            return True
        except Exception as e:
            print(f"🔥 초기 데이터 로딩 중 오류 발생: {e}")
            return False

    async def _run_trading_loop(self):
        print("메인 로직 시작: Bybit와 Binance가 준비될 때까지 대기 중...")
        await self.bybit_ready.wait()
        await self.binance_ready.wait()
        print("✅ Bybit & Binance 모두 준비 완료! 거래를 시작합니다.")

        sym = self.config.target_symbol
        
        while True:
            await asyncio.sleep(0.001)

            # 데이터 유효성 검사
            if not all([
                sym in self.bybit_data, self.bybit_data.get(sym, {}).get('a', 0) > 0, self.bybit_data.get(sym, {}).get('b', 0) > 0,
                sym in self.binance_best_bid, self.binance_best_bid.get(sym, 0) > 0,
                sym in self.binance_best_ask, self.binance_best_ask.get(sym, 0) > 0
            ]):
                continue

            # 갭 계산
            forward_entry_kimp = round(self.bybit_data[sym]['a'] / self.binance_best_bid[sym] * 1000, 2)
            forward_close_kimp = round(self.bybit_data[sym]['b'] / self.binance_best_ask[sym] * 1000, 2)
            reverse_entry_kimp = round(self.binance_best_ask[sym] / self.bybit_data[sym]['b'] * 1000, 2)
            reverse_close_kimp = round(self.binance_best_bid[sym] / self.bybit_data[sym]['a'] * 1000, 2)

            # --- 포지션 상태에 따른 로직 분기 ---
            
            # 1. 포지션이 없는 경우
            if self.position_status == "NONE":
                if (forward_entry_kimp < self.config.entry_gap) and (self.entry_count < self.config.max_entries):
                    print(f"⬇️ [정방향 진입] Bybit Long / Binance Short 시도 - 갭: {forward_entry_kimp}")
                    tasks = [
                        self.bybit_ws_manager.place_order(symbol=sym, side="Buy", orderType="Market", qty=self.config.size),
                        self.binance_ws_manager.place_order(symbol=sym, side='SELL', type='MARKET', quantity=self.config.size)
                    ]
                    results = await asyncio.gather(*tasks)
                    bybit_result, binance_result = results
                    
                    if bybit_result.get('retCode') == 0 and binance_result.get('error') is None:
                        print("✅ 양방향 진입 주문 성공!")
                        self.entry_count += 1
                        self.position_status = "BYBIT_LONG"
                    else:
                        print("❌ 진입 주문 실패! 포지션 확인 필요!")
                        print(f"Bybit: {bybit_result}\nBinance: {binance_result}")

                elif (reverse_entry_kimp < self.config.entry_gap) and (self.entry_count < self.config.max_entries):
                    print(f"⬇️ [역방향 진입] Binance Long / Bybit Short 시도 - 갭: {reverse_entry_kimp}")
                    tasks = [
                        self.binance_ws_manager.place_order(symbol=sym, side='BUY', type='MARKET', quantity=self.config.size),
                        self.bybit_ws_manager.place_order(symbol=sym, side="Sell", orderType="Market", qty=self.config.size)
                    ]
                    results = await asyncio.gather(*tasks)
                    binance_result, bybit_result = results

                    if bybit_result.get('retCode') == 0 and binance_result.get('error') is None:
                        print("✅ 양방향 진입 주문 성공!")
                        self.entry_count += 1
                        self.position_status = "BINANCE_LONG"
                    else:
                        print("❌ 진입 주문 실패! 포지션 확인 필요!")
                        print(f"Bybit: {bybit_result}\nBinance: {binance_result}")

            # 2. Bybit Long / Binance Short 포지션
            elif self.position_status == "BYBIT_LONG":
                if (forward_entry_kimp < self.config.entry_gap) and (self.entry_count < self.config.max_entries):
                    print(f"⬇️ [정방향 추가 진입] 시도 - 갭: {forward_entry_kimp}")
                    tasks = [
                        self.bybit_ws_manager.place_order(symbol=sym, side="Buy", orderType="Market", qty=self.config.size),
                        self.binance_ws_manager.place_order(symbol=sym, side='SELL', type='MARKET', quantity=self.config.size)
                    ]
                    results = await asyncio.gather(*tasks)
                    if results[0].get('retCode') == 0 and results[1].get('error') is None:
                        print("✅ 추가 진입 성공!")
                        self.entry_count += 1
                    else:
                        print("❌ 추가 진입 실패!", results)


                elif (forward_close_kimp > self.config.target_price) and (self.entry_count >= 1):
                    print(f"⬆️ [정방향 종료] 시도 - 갭: {forward_close_kimp}")
                    close_qty = str(int(self.config.size) * self.entry_count)
                    tasks = [
                        self.bybit_ws_manager.place_order(symbol=sym, side="Sell", orderType="Market", qty=close_qty, reduceOnly=True),
                        self.binance_ws_manager.place_order(symbol=sym, side='BUY', type='MARKET', quantity=close_qty, reduceOnly=True)
                    ]
                    results = await asyncio.gather(*tasks)
                    if results[0].get('retCode') == 0 and results[1].get('error') is None:
                        print("✅ 양방향 종료 주문 성공!")
                        self.entry_count = 0
                        self.position_status = "NONE"
                    else:
                        print("❌ 종료 주문 실패! 포지션 확인 필요!")
                        print(f"Bybit: {results[0]}\nBinance: {results[1]}")

            # 3. Binance Long / Bybit Short 포지션
            elif self.position_status == "BINANCE_LONG":
                if (reverse_entry_kimp < self.config.entry_gap) and (self.entry_count < self.config.max_entries):
                    print(f"⬇️ [역방향 추가 진입] 시도 - 갭: {reverse_entry_kimp}")
                    tasks = [
                        self.binance_ws_manager.place_order(symbol=sym, side='BUY', type='MARKET', quantity=self.config.size),
                        self.bybit_ws_manager.place_order(symbol=sym, side="Sell", orderType="Market", qty=self.config.size)
                    ]
                    results = await asyncio.gather(*tasks)
                    if results[0].get('error') is None and results[1].get('retCode') == 0:
                        print("✅ 추가 진입 성공!")
                        self.entry_count += 1
                    else:
                        print("❌ 추가 진입 실패!", results)
                    
                elif (reverse_close_kimp > self.config.target_price) and (self.entry_count >= 1):
                    print(f"⬆️ [역방향 종료] 시도 - 갭: {reverse_close_kimp}")
                    close_qty = str(int(self.config.size) * self.entry_count)
                    tasks = [
                        self.binance_ws_manager.place_order(symbol=sym, side='SELL', type='MARKET', quantity=close_qty, reduceOnly=True),
                        self.bybit_ws_manager.place_order(symbol=sym, side="Buy", orderType="Market", qty=close_qty, reduceOnly=True)
                    ]
                    results = await asyncio.gather(*tasks)
                    if results[0].get('error') is None and results[1].get('retCode') == 0:
                        print("✅ 양방향 종료 주문 성공!")
                        self.entry_count = 0
                        self.position_status = "NONE"
                    else:
                        print("❌ 종료 주문 실패! 포지션 확인 필요!")
                        print(f"Binance: {results[0]}\nBybit: {results[1]}")

    async def run(self):
        async with aiohttp.ClientSession() as session:
            if not await self._initialize_market_data(session):
                return

            self.bybit_ws_manager = BybitWebsocketManager(session, self.config.BYBIT_API_KEY, self.config.BYBIT_API_SECRET, self.bybit_ready)
            self.binance_ws_manager = BinanceWebsocketManager(session, self.config.BINANCE_API_KEY, self.config.BINANCE_API_SECRET, self.binance_ready)

            # Public 데이터 스트림 URL 및 구독 정보 설정
            symbol_lower = self.config.target_symbol.lower()
            binance_public_url = f"wss://fstream.binance.com/ws/{symbol_lower}@bookTicker"
            bybit_subscription = {"op": "subscribe", "args": [f"orderbook.1.{self.config.target_symbol}"]}
            
            await asyncio.gather(
                self.bybit_ws_manager.connect(),
                self.binance_ws_manager.connect(),
                self._data_stream_task('Bybit', 'wss://stream.bybit.com/v5/public/linear', self._bybit_processor, bybit_subscription),
                self._data_stream_task('Binance', binance_public_url, self._binance_processor),
                self._run_trading_loop()
            )

if __name__ == "__main__":
    config = TradingConfig()
    bot = ArbitrageBot(config)
    asyncio.run(bot.run())