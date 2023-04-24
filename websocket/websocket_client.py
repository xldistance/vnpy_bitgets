import json
from typing import Union
from multiprocessing.dummy.connection import Client
import gzip
import sys
import traceback
from datetime import datetime
from types import coroutine
from threading import Thread
from asyncio import (
    get_running_loop,
    new_event_loop,
    set_event_loop,
    run_coroutine_threadsafe,
    AbstractEventLoop
)
from pathlib import Path
from aiohttp import ClientSession, ClientWebSocketResponse,client_exceptions,TCPConnector,ClientTimeout

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine, LogEngine
from vnpy.trader.utility import save_connection_status

#------------------------------------------------------------------------------------------------- 
class WebsocketClient:
    """
    针对各类Websocket API的异步客户端
    * 重载unpack_data方法来实现数据解包逻辑
    * 重载on_connected方法来实现连接成功回调处理
    * 重载on_disconnected方法来实现连接断开回调处理
    * 重载on_packet方法来实现数据推送回调处理
    * 重载on_error方法来实现异常捕捉回调处理
    """
    #------------------------------------------------------------------------------------------------- 
    def __init__(self):
        """
        """
        self._active: bool = False
        self._host: str = ""
        self._session: ClientSession = None
        self._ws: ClientWebSocketResponse = None
        self._loop: AbstractEventLoop = None

        self._proxy: str = ""
        self._ping_interval: int = 20  # 秒
        self._header: dict = {}

        self._last_sent_text: str = ""
        self._last_received_text: str = ""
        self.event_engine = EventEngine()
        self.main_engine = MainEngine(self.event_engine)
        self.write_log = self.main_engine.write_log
    #------------------------------------------------------------------------------------------------- 
    def init(
        self,
        host: str,
        proxy_host: str = "",
        proxy_port: int = 0,
        proxy_type: str = "socks5",
        ping_interval: int = 20,
        header: dict = None,
        gateway_name: str = ""
    ):
        """
        初始化客户端
        """
        self._host = host
        self._ping_interval = ping_interval
        self.gateway_name = gateway_name
        if header:
            self._header = header

        if proxy_host and proxy_port:
            self._proxy = f"http://{proxy_host}:{proxy_port}"
        assert self.gateway_name,"请到交易接口websocketapi connect函数里面的self.init函数中添加gateway_name参数"
    #------------------------------------------------------------------------------------------------- 
    def start(self):
        """
        启动客户端
        连接成功后会自动调用on_connected回调函数，请等待on_connected被调用后，再发送数据包。
        """
        self._active = True

        try:
            self._loop = get_running_loop()
        except RuntimeError:
            self._loop = new_event_loop()
        start_event_loop(self._loop)

        run_coroutine_threadsafe(self._run(), self._loop)
    #------------------------------------------------------------------------------------------------- 
    def stop(self):
        """
        停止客户端
        """
        self._active = False

        if self._ws:
            coro: coroutine= self._ws.close()
            run_coroutine_threadsafe(coro, self._loop)

        if self._loop and self._loop.is_running():
            self._loop.stop()
    #------------------------------------------------------------------------------------------------- 
    def join(self):
        """
        等待后台线程退出。
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def send_packet(self, packet: Union[dict,str]):
        """
        发送数据包字典到服务器。
        如果需要发送非json数据，请重载实现本函数。
        """
        if self._ws:
            if packet in ["Pong","ping"]:
                text = packet
            else:
                text: str = json.dumps(packet)
            self._record_last_sent_text(text)
            coro: coroutine = self._ws.send_str(text)
            run_coroutine_threadsafe(coro, self._loop)
    #------------------------------------------------------------------------------------------------- 
    def unpack_data(self, data: str):
        """
        对字符串数据进行json格式解包
        如果需要使用json以外的解包格式，请重载实现本函数。
        """
        data = json.loads(data)
        return data
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self):
        """
        连接成功回调
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_disconnected(self):
        """
        连接断开回调
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet: dict):
        """
        收到数据回调
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_error(self, exception_type: type, exception_value: Exception, tracebacks):
        """
        触发异常回调
        """
        self.write_log(self.exception_detail(exception_type, exception_value, tracebacks))
    #------------------------------------------------------------------------------------------------- 
    def exception_detail(
        self, exception_type: type, exception_value: Exception, tracebacks
    ):
        """
        异常信息格式化
        """
        text = "[{}]: Unhandled WebSocket Error:{}\n".format(
            datetime.now().isoformat(), exception_type
        )
        text += "LastSentText:\n{}\n".format(self._last_sent_text)
        text += "LastReceivedText:\n{}\n".format(self._last_received_text)
        text += "Exception trace: \n"
        text += "".join(
            traceback.format_exception(exception_type, exception_value, tracebacks)
        )
        return text
    #------------------------------------------------------------------------------------------------- 
    async def _run(self):
        """
        在事件循环中运行的主协程
        """
        # 限制超时300秒，连接池数量300
        timeout = ClientTimeout(total = 300)
        connector = TCPConnector(limit=300, verify_ssl=False)
        self._session: ClientSession = ClientSession(trust_env=True,timeout = timeout,connector = connector)
        while self._active:
            # 捕捉运行过程中异常
            try:
                # 发起Websocket连接
                self._ws = await self._session.ws_connect(
                    self._host,
                    proxy=self._proxy,
                    heartbeat= self._ping_interval,
                    verify_ssl = False,
                )
                # 调用连接成功回调
                self.on_connected()

                # 持续处理收到的数据
                async for msg in self._ws:
                    text: Union[str,bytes] = msg.data
                    if text == "pong":
                        self.on_packet(text)
                        continue
                    # 解压gzip数据
                    if isinstance(text,bytes):
                        text = gzip.decompress(text).decode("UTF-8")
                        if text == "Ping":
                            self.on_packet(text)
                            continue
                    self._record_last_received_text(text)
                    try:
                        data: dict = self.unpack_data(text)
                        self.on_packet(data)
                    except Exception as err:
                        self.write_log(f"交易接口：{self.gateway_name}，websocket api解析数据错误信息：{err}，收到数据为：{text}")
                        #更新websocket api连接错误状态
                        save_connection_status(self.gateway_name,False)
                        self.stop()
                # 移除Websocket连接对象
                self._ws = None

                # 调用连接断开回调
                self.on_disconnected()
            except (client_exceptions.WSServerHandshakeError):
                self.write_log(f"交易接口：{self.gateway_name}，websocket api触发WSServerHandshakeError，最近收到数据：{self._last_received_text}")
                #更新websocket api连接错误状态
                save_connection_status(self.gateway_name,False)
                self.stop()
            # 处理捕捉到的异常
            except Exception as err:
                exception_type, exception_value, tracebacks = sys.exc_info()
                self.on_error(exception_type, exception_value, tracebacks)
                #更新websocket api连接错误状态
                save_connection_status(self.gateway_name,False)
                self.stop()
    #------------------------------------------------------------------------------------------------- 
    def _record_last_sent_text(self, text: str):
        """
        记录最近发出的数据字符串
        """
        self._last_sent_text = text[:1000]
    #------------------------------------------------------------------------------------------------- 
    def _record_last_received_text(self, text: str):
        """
        记录最近收到的数据字符串
        """
        self._last_received_text = text[:1000]

#------------------------------------------------------------------------------------------------- 
def start_event_loop(loop: AbstractEventLoop) -> AbstractEventLoop:
    """
    启动事件循环
    """
    # 如果事件循环未运行，则创建后台线程来运行
    if not loop.is_running():
        thread = Thread(target=run_event_loop, args=(loop,))
        thread.daemon = True
        thread.start()
#------------------------------------------------------------------------------------------------- 
def run_event_loop(loop: AbstractEventLoop) -> None:
    """
    运行事件循环
    """
    set_event_loop(loop)
    loop.run_forever()
#------------------------------------------------------------------------------------------------- 