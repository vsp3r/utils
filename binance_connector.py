import websockets
from websockets.exceptions import ConnectionClosedError
import asyncio
import orjson
import time
import requests
from async_logger import AsynchronousLogger
RED = '\033[91m'
GREEN = '\033[92m'
RESET = '\033[0m'  # Reset the color

class BinanceConnector:
    def __init__(self, symbols, queues):
        self.symbols = symbols
        self.queue = queues
        self.running = True
        
        self.api_url = "https://fapi.binance.com"
        self.ws_url = 'wss://fstream.binance.com/ws'
        self.exchange = 1
    
    def start(self):
        """Starts Binance websocket. In charge of doing its job, and reporting errors"""
        self.logger = AsynchronousLogger('bin_connector.log')
        self.logger.log("timestamp,coin,exch_to_server,msg_to_process")
        asyncio.run(self.run())

    async def run(self):
        await self.connect()

    async def connect(self):
        await self.approve_symbols()
        async with websockets.connect(self.ws_url) as ws:
            self.ws = ws
            await asyncio.gather(*(self.subscribe(ws, coin.lower() + 'usdt')
                                for coin in self.symbols))
            while self.running:
                message = await ws.recv()
                air_time = time.time() * 1_000_000
                ts = time.perf_counter_ns()
                asyncio.create_task(self.process_data(message, air_time, ts))

    async def subscribe(self, ws, coin):
        subscription_msg = {
            "method":"SUBSCRIBE",
            "params":[
                coin+"@depth5@0ms"
                # coin+"@aggTrade",
                # coin+"@markPrice@1s",
                # coin+"bookTicker"
            ],
            "id":1
        }
        await ws.send(orjson.dumps(subscription_msg).decode('utf-8'))
        await asyncio.sleep(0.2)

    async def process_data(self, message, air_time, ts):
        t2 = time.perf_counter_ns()
        data = orjson.loads(message)
        try:
            # Check if the keys exist in the data
            if 'e' in data and 's' in data:
                coin = data['s'][:-4]
                self.queue.put_nowait(('binance', coin, data, ts))
                exch_ts = data['E'] * 1000
                print(RED + f'[BIN {coin[:4]}]: Exch to server: {air_time - exch_ts}us ({(air_time-exch_ts)/1000}ms). Msg to process: {(t2-ts)/1000}us' + RESET)
                self.logger.log(f"{exch_ts},{coin},{air_time - exch_ts},{(t2-ts)/1000}")
            else:
                pass

        except Exception as e:
            print(f"(BINANCE) {self.symbols} Error processing message: {e}\nMessage: {message}")

    async def shutdown(self):
        try:
            await asyncio.gather(*(self.unsubscribe(self.ws, coin)
                                    for coin in self.symbols))
            await self.ws.close()
            print('SUCCESSFULLY CLOSED BINANCE WS')
        except ConnectionClosedError:
            pass
        
    async def unsubscribe(self, ws, coin):
        unsub = {
            "method":"UNSUBSCRIBE",
            "params":[
                coin+"@depth5@0ms"
            ],
            "id":1
        }
        await ws.send(orjson.dumps(unsub).decode('utf-8'))
        await asyncio.sleep(0.2)


    async def approve_symbols(self):
        info_url = self.api_url + '/fapi/v1/exchangeInfo'
        response = requests.get(info_url)
        data = response.json()

        symbols = [item['symbol'][:-4] for item in data['symbols'] if 'USDT' in item['symbol']]
        pre_len = len(self.symbols)
        self.symbols = [coin for coin in self.symbols if coin in symbols]
        if len(self.symbols) != pre_len:
            raise Exception(f"BinanceConn: available {symbols} and {self.symbols} do not align")
        print(f'init binance w/ {self.symbols} total length" {len(self.symbols)}')



