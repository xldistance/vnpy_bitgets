import re
import urllib
from urllib.parse import urlencode
import base64
import json
import zlib
import hashlib
import hmac
import sys
from pathlib import Path
import csv
from copy import copy
from datetime import datetime, timedelta
from time import time
from threading import Lock
from typing import Sequence
from typing import Dict, List, Any
from peewee import chunked

from vnpy.event import Event
from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    BarData,
    AccountData,
    PositionData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.setting import bitget_account  #导入账户字典
from vnpy.trader.utility import (get_folder_path,load_json, save_json,get_local_datetime,extract_vt_symbol,TZ_INFO,GetFilePath)
from vnpy.trader.database import database_manager

REST_HOST = "https://api.bitget.com"
WEBSOCKET_DATA_HOST = "wss://ws.bitget.com/mix/v1/stream"               # Market Data
WEBSOCKET_TRADE_HOST = "wss://ws.bitget.com/mix/v1/stream"    # Account and Order

STATUS_BITGETS2VT: Dict[int, Status] = {
    "init": Status.NOTTRADED,
    "new": Status.NOTTRADED,
    "partially_filled": Status.PARTTRADED,
    "partial-fill": Status.PARTTRADED,
    "canceled": Status.CANCELLED,
    "cancelled": Status.CANCELLED,
    "filled": Status.ALLTRADED,
    "full-fill": Status.ALLTRADED,
}

ORDERTYPE_VT2BITGETS: Dict[OrderType, Any] = {
    OrderType.LIMIT: "limit",
    OrderType.MARKET: "market"
}
ORDERTYPE_BITGETS2VT: Dict[Any, OrderType] = {v: k for k, v in ORDERTYPE_VT2BITGETS.items()}

DIRECTION_VT2BITGETS: Dict[Direction, str] = {
    Direction.LONG: "buy_single",
    Direction.SHORT: "sell_single",
}
DIRECTION_BITGETS2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BITGETS.items()}

HOLDSIDE_BITGETS2VT: Dict[str,Direction] = {
    "long":Direction.LONG,
    "short":Direction.SHORT
}

INTERVAL_VT2BITGETS: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1H",
    Interval.DAILY: "1D"
}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class BitGetSGateway(BaseGateway):
    """
    * bitget接口
    * 单向持仓模式
    """
    #default_setting由vnpy.trader.ui.widget调用
    default_setting: Dict[str, Any] = {
        "API Key": "",
        "Secret Key": "",
        "会话数": 3,
        "代理地址": "",
        "代理端口": "",
    }

    exchanges = [Exchange.BITGETS]        #由main_engine add_gateway调用
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, event_engine):
        """
        """
        super(BitGetSGateway,self).__init__(event_engine, "BITGETS")
        self.orders: Dict[str, OrderData] = {}
        self.rest_api = BitGetSRestApi(self)
        self.trade_ws_api = BitGetSTradeWebsocketApi(self)
        self.market_ws_api = BitGetSDataWebsocketApi(self)
        #所有合约列表
        self.recording_list = GetFilePath.recording_list
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if extract_vt_symbol(vt_symbol)[2] == self.gateway_name  and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
        #查询历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if extract_vt_symbol(vt_symbol)[2] == self.gateway_name and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
        self.leverage_contracts = copy(self.recording_list)
    #------------------------------------------------------------------------------------------------- 
    def connect(self,log_account:dict = {}):
        """
        """
        if not log_account:
            log_account = bitget_account
        key = log_account["API Key"]
        secret = log_account["Secret Key"]
        session_number = log_account["会话数"]
        passphrase = log_account["Passphrase"]
        proxy_host = log_account["代理地址"]
        proxy_port = log_account["代理端口"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret,passphrase,session_number,
                              proxy_host, proxy_port)
        self.trade_ws_api.connect(key, secret,passphrase,proxy_host, proxy_port)
        self.market_ws_api.connect(key, secret,passphrase,proxy_host, proxy_port)

        self.init_query()
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        self.market_ws_api.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        return self.rest_api.send_order(req)
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        self.rest_api.cancel_order(req)
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> Request:
        """
        查询账户
        """
        self.rest_api.query_account()
    #------------------------------------------------------------------------------------------------- 
    def query_order(self,symbol:str):
        """
        查询活动委托单
        """
        self.rest_api.query_order(symbol)
    #-------------------------------------------------------------------------------------------------  
    def query_position(self, symbol: str):
        """
        查询持仓
        """
        pass
    #-------------------------------------------------------------------------------------------------   
    def query_history(self,event:Event):
        """
        查询合约历史数据
        """
        if len(self.history_contracts) > 0:
            symbol,exchange,gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol = symbol,
                exchange = exchange,
                interval = Interval.MINUTE,
                start = datetime.now(TZ_INFO) - timedelta(days = 1),
                gateway_name = self.gateway_name
            )
            self.rest_api.query_history(req)
    #---------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        收到委托单推送，BaseGateway推送数据
        """
        self.orders[order.vt_orderid] = copy(order)
        super().on_order(order)
    #---------------------------------------------------------------------------------------
    def get_order(self, vt_orderid: str) -> OrderData:
        """
        用vt_orderid获取委托单数据
        """
        return self.orders.get(vt_orderid, None)
    #------------------------------------------------------------------------------------------------- 
    def close(self) -> None:
        """
        关闭接口
        """
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()
    #------------------------------------------------------------------------------------------------- 
    def process_timer_event(self, event: Event):
        """
        处理定时任务
        """
        if self.query_contracts:
            vt_symbol = self.query_contracts.pop(0)
            symbol,exchange,gateway_name = extract_vt_symbol(vt_symbol)
            self.query_order(symbol)
            self.query_contracts.append(vt_symbol)
        self.query_account()
        if self.leverage_contracts:
            symbol = extract_vt_symbol(self.leverage_contracts.pop(0))[0]
            self.rest_api.set_leverage(symbol)
    #------------------------------------------------------------------------------------------------- 
    def init_query(self):
        """
        初始化定时查询
        """
        self.event_engine.register(EVENT_TIMER, self.query_history)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
#------------------------------------------------------------------------------------------------- 
class BitGetSRestApi(RestClient):
    """
    BITGET REST API
    """
    def __init__(self, gateway: BitGetSGateway):
        """
        """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.host: str = ""
        self.key: str = ""
        self.secret: str = ""

        self.order_count: int = 10000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.account_date = None   #账户日期
        self.accounts_info:Dict[str,dict] = {}
        self.all_contracts:List[str] = []          #所有vt_symbol合约列表
        self.product_types = ["umcbl","cmcbl","dmcbl"]   # USDT,USDC,币本位合约
        self.product_coin_map = {
            "UMCBL": "USDT",
            "CMCBL": "USDC",
        }
    #------------------------------------------------------------------------------------------------- 
    def sign(self, request) -> Request:
        """
        生成签名
        """
        timestamp = str(int(time() * 1000))
        path = request.path
        method = request.method
        data = request.data
        params = request.params
        if method == "GET":
            path += "?" + urlencode(params)
        if method == "POST":
            body = request.data = json.dumps(data)
        else:
            body = ""
        
        message = timestamp + method + path + body
        signature = create_signature(self.secret,message)

        if not request.headers:
            request.headers = {}
            request.headers["ACCESS-KEY"] = self.key
            request.headers["ACCESS-SIGN"] = signature
            request.headers["ACCESS-TIMESTAMP"] = timestamp
            request.headers["ACCESS-PASSPHRASE"] = self.passphrase
            request.headers["Content-Type"] = "application/json"
        return request
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        session_number: int,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        连接REST服务
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.connect_time = int(datetime.now(TZ_INFO).strftime("%y%m%d%H%M%S"))

        self.init(REST_HOST, proxy_host, proxy_port,gateway_name=self.gateway_name)
        self.start(session_number)

        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")

        self.query_contract()
    #------------------------------------------------------------------------------------------------- 
    def get_margin_coin(self,symbol:str):
        """
        获取保证金币种
        """
        product_type = symbol.split("_")[1]
        margin_coin = self.product_coin_map.get(product_type,None)
        if not margin_coin:
            margin_coin = symbol.split("USD")[0]
        return margin_coin
    #------------------------------------------------------------------------------------------------- 
    def set_leverage(self,symbol:str):
        """
        设置杠杆
        """
        
        data = {
            "symbol":symbol,
            "marginCoin":self.get_margin_coin(symbol),
            "leverage":20,
        }
        self.add_request(
            method="POST",
            path="/api/mix/v1/account/setLeverage",
            callback=self.on_leverage,
            data = data
        )
    #------------------------------------------------------------------------------------------------- 
    def on_leverage(self,data:dict,request: Request):
        pass
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> Request:
        """
        查询账户数据
        """
        for product in self.product_types:
            params = {"productType":product}
            self.add_request(
                method="GET",
                path="/api/mix/v1/account/accounts",
                callback=self.on_query_account,
                params= params
            )
    #------------------------------------------------------------------------------------------------- 
    def query_order(self,symbol:str):
        """
        查询合约活动委托单
        """

        params = {"symbol": symbol}
        self.add_request(
            method="GET",
            path="/api/mix/v1/order/current",
            callback=self.on_query_order,
            params=params,
            extra=symbol
        )
    #------------------------------------------------------------------------------------------------- 
    def query_contract(self) -> Request:
        """
        获取合约信息
        """
        for product in self.product_types:
            params = {"productType":product}    
            self.add_request(
                method="GET",
                path="/api/mix/v1/market/contracts",
                params = params,
                callback=self.on_query_contract,
            )
    #------------------------------------------------------------------------------------------------- 
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history = []
        count = 200
        start = req.start
        time_delta = TIMEDELTA_MAP[req.interval]
        time_consuming_start = time()

        while True:
            end = start + time_delta * count

            # 查询K线参数
            params = {
                "symbol": req.symbol,
                "granularity": INTERVAL_VT2BITGETS[req.interval],
                "startTime": str(int(start.timestamp() * 1000)),
                "endTime": str(int(end.timestamp() * 1000)),
                "limit":str(count)
            }
            resp = self.request(
                "GET",
                "/api/mix/v1/market/candles",
                params=params
            )
            # resp为空或收到错误代码则跳出循环
            if not resp:
                msg = f"获取历史数据失败，状态码：{resp}"
                self.gateway.write_log(msg)
                break
            elif resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                rawdata = resp.json()
                buf = []
                for data in rawdata:
                    dt = get_local_datetime(int(data[0]))

                    bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=float(data[5]),
                        open_price=float(data[1]),
                        high_price=float(data[2]),
                        low_price=float(data[3]),
                        close_price=float(data[4]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)
                start = bar.datetime
                #下载够数据量跳出循环
                if len(buf) < count:
                    break
        if not history:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            return
        for bar_data in chunked(history, 10000):               #分批保存数据
            try:
                database_manager.save_bar_data(bar_data,False)      #保存数据到数据库  
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return    
        time_consuming_end =time()        
        query_time = round(time_consuming_end - time_consuming_start,3)
        if not history:
            msg = f"未查询到合约：{req.vt_symbol}历史数据，请核实行情连接"
        else:
            msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime} ，结束时间： {history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def new_local_orderid(self) -> str:
        """
        生成local_orderid
        """
        with self.order_count_lock:
            self.order_count += 1
            local_orderid = str(self.connect_time+self.order_count)
            return local_orderid
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        local_orderid =req.symbol + "-" + self.new_local_orderid()
        order = req.create_order_data(
            local_orderid,
            self.gateway_name
        )
        order.datetime = datetime.now(TZ_INFO)

        data = {
            "symbol": req.symbol,
            "marginCoin":self.get_margin_coin(req.symbol),
            "clientOid": local_orderid,
            "price": str(req.price),
            "size": str(req.volume),
            "side": DIRECTION_VT2BITGETS.get(req.direction),
            "orderType": ORDERTYPE_VT2BITGETS.get(req.type),
            "timeInForceValue": "normal"
        }

        if req.offset == Offset.CLOSE:
            data["reduceOnly"] = True

        self.add_request(
            method="POST",
            path="/api/mix/v1/order/placeOrder",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        self.gateway.on_order(order)
        return order.vt_orderid
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        order: OrderData = self.gateway.get_order(req.vt_orderid)

        data = {
            "symbol": req.symbol,
            "marginCoin":self.get_margin_coin(req.symbol),
            "clientOid":req.orderid
        }
        self.add_request(
            method="POST",
            path="/api/mix/v1/order/cancel-order",
            callback=self.on_cancel_order,
            on_failed=self.on_cancel_order_failed,
            data=data,
            extra=order
        )
    #------------------------------------------------------------------------------------------------- 
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        收到账户数据回报
        """
        if self.check_error(data, "查询账户"):
            return
        for account_data in data["data"]:
            account = AccountData(
                accountid=account_data["marginCoin"] + "_" + self.gateway_name,
                balance= float(account_data["equity"]),
                available= float(account_data["available"]),
                position_profit = float(account_data["unrealizedPL"]),
                frozen=float(account_data["locked"]),
                datetime= datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            if account.balance:
                self.gateway.on_account(account)
                #保存账户资金信息
                self.accounts_info[account.accountid] = account.__dict__
        if  not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = GetFilePath().ctp_account_path.replace("ctp_account_1",self.gateway.account_file_name)
        for account_data in accounts_info:
            if not Path(account_path).exists(): # 如果文件不存在，需要写header
                with open(account_path, 'w',newline="") as f1:          #newline=""不自动换行
                    w1 = csv.DictWriter(f1, account_data.keys())
                    w1.writeheader()
                    w1.writerow(account_data)
            else: # 文件存在，不需要写header
                if self.account_date and self.account_date != account_date:        #一天写入一次账户信息         
                    with open(account_path,'a',newline="") as f1:                               #a二进制追加形式写入
                        w1 = csv.DictWriter(f1, account_data.keys())
                        w1.writerow(account_data)
        self.account_date = account_date
    #------------------------------------------------------------------------------------------------- 
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        收到委托回报
        """
        if self.check_error(data, "查询活动委托"):
            return
        data = data["data"]
        if not data:
            return
        for order_data in data:
            order_datetime =  get_local_datetime(int(order_data["cTime"]))

            order = OrderData(
                orderid=order_data["clientOid"],
                symbol=order_data["symbol"],
                exchange=Exchange.BITGETS,
                price=order_data["price"],
                volume=order_data["size"],
                type=ORDERTYPE_BITGETS2VT[order_data["orderType"]],
                direction=DIRECTION_BITGETS2VT[order_data["side"]],
                traded=float(order_data["filledQty"]),
                status=STATUS_BITGETS2VT[order_data["state"]],
                datetime= order_datetime,
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"]:
                order.offset = Offset.CLOSE

            self.gateway.on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def on_query_contract(self, data: dict, request: Request) -> None:
        """
        收到合约参数回报
        """
        if self.check_error(data, "查询合约"):
            return
        for contract_data in data["data"]:
            price_place = contract_data["pricePlace"]
            contract = ContractData(
                symbol=contract_data["symbol"],
                exchange=Exchange.BITGETS,
                name=contract_data["symbol"],
                price_tick=float(contract_data["priceEndStep"]) * float(f"1e-{price_place}"),
                size=20,    # 合约杠杆
                min_volume=float(contract_data["minTradeNum"]),
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
            if contract.vt_symbol not in self.all_contracts:
                self.all_contracts.append(contract.vt_symbol)
        product_type = contract_data["supportMarginCoins"][0]
        self.gateway.on_all_contracts(self.all_contracts)  
        self.gateway.write_log(f"交易接口：{self.gateway_name}，{product_type}合约信息查询成功")
    #------------------------------------------------------------------------------------------------- 
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        """
        order = request.extra

        if self.check_error(data, "委托"):
            order.status = Status.REJECTED
            self.gateway.on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_failed(self, status_code, request: Request) -> None:
        """
        收到委托失败回报
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        """
        self.check_error(data, "撤单")
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order_failed(
        self,
        status_code,
        request: Request
    ) -> None:
        """
        收到撤单失败回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """
        Callback to handler request exception.
        """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )
    #------------------------------------------------------------------------------------------------- 
    def check_error(self, data: dict, func: str = "") -> bool:
        """
        """
        if data["msg"] == "success":
            return False

        error_code = data["code"]
        error_msg = data["msg"]
        self.gateway.write_log(f"{func}请求出错，代码：{error_code}，信息：{error_msg}")
        return True

#------------------------------------------------------------------------------------------------- 
class BitGetSWebsocketApiBase(WebsocketClient):
    """
    """

    def __init__(self, gateway):
        """
        """
        super(BitGetSWebsocketApiBase, self).__init__()

        self.gateway: BitGetSGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""
        self.count = 0
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        url: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.init(url, proxy_host, proxy_port,gateway_name = self.gateway_name)
        self.start()
        self.gateway.event_engine.register(EVENT_TIMER,self.send_ping)
    #------------------------------------------------------------------------------------------------- 
    def send_ping(self,event):
        self.count +=1
        if self.count < 20:
            return
        self.count = 0
        self.send_packet("ping")
    #------------------------------------------------------------------------------------------------- 
    def login(self) -> int:
        """
        """
        timestamp = str(int(time()))
        message = timestamp + "GET" + "/user/verify"
        signature = create_signature(self.secret,message)
        params = {
            "op":"login",
            "args":[
                {
                    "apiKey":self.key,
                    "passphrase":self.passphrase,
                    "timestamp":timestamp,
                    "sign":signature
                }
            ]
        }
        return self.send_packet(params)
    #------------------------------------------------------------------------------------------------- 
    def on_login(self, packet) -> None:
        """
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_data(self, packet):
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet) -> None:
        """
        """
        if packet == "pong":
            return
        if "event" in packet:
            if packet["event"] == "login" and packet["code"] == 0:
                self.on_login()
            elif packet["event"] == "error":
                self.on_error_msg(packet)
        else:
            self.on_data(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_error_msg(self, packet) -> None:
        """
        """
        msg = packet["msg"]
        self.gateway.write_log(f"交易接口：{self.gateway_name} WebSocket API收到错误回报，回报信息：{msg}")
#------------------------------------------------------------------------------------------------- 
class BitGetSDataWebsocketApi(BitGetSWebsocketApiBase):
    """
    """

    def __init__(self, gateway):
        """
        """
        super().__init__(gateway)

        self.ticks = {}
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase:str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        super().connect(
            key,
            secret,
            passphrase,
            WEBSOCKET_DATA_HOST,
            proxy_host,
            proxy_port
        )
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接成功")

        for symbol in list(self.ticks.keys()):
            self.subscribe_data(symbol)
    #-------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws行情断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接断开")
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        tick = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=Exchange.BITGETS,
            datetime=datetime.now(TZ_INFO),
            gateway_name=self.gateway_name,
        )
        inst_id = tick.symbol.split("_")[0]
        self.ticks[inst_id] = tick

        self.subscribe_data(tick.symbol)
    #------------------------------------------------------------------------------------------------- 
    def subscribe_data(self, symbol: str) -> None:
        """
        订阅市场深度主题
        """
        # 订阅tick
        symbol = symbol.split("_")[0]
        req = {
            "op":"subscribe",
            "args":[
                {
                    "instType":"MC",
                    "channel":"ticker",
                    "instId":symbol
                }
            ]
        }
        self.send_packet(req)

        # 订阅行情深度
        req = {
            "op":"subscribe",
            "args":[
                {
                    "instType":"MC",
                    "channel":"books5",
                    "instId":symbol
                }
            ]
        }
        self.send_packet(req)
        #订阅最新成交
        req = {
            "op":"subscribe",
            "args":[
                {
                    "instType":"MC",
                    "channel":"tradeNew",
                    "instId":symbol
                }
            ]
        }
        self.send_packet(req)        
    #------------------------------------------------------------------------------------------------- 
    def on_data(self, packet) -> None:
        """
        """
        channel = packet["arg"]["channel"]    #全量推送snapshot，增量推送update
        if "action" in packet:
            if channel == "ticker":
                self.on_tick(packet["data"])
            elif channel == "books5":
                self.on_depth(packet)
            elif channel == "tradeNew":
                self.on_public_trade(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_tick(self, data: dict) -> None:
        """
        收到tick数据推送
        """
        for tick_data in data:
            symbol = tick_data["symbolId"]
            inst_id = symbol.split("_")[0]
            tick = self.ticks[inst_id]
            tick.datetime = get_local_datetime(tick_data["systemTime"])
            tick.open_price = float(tick_data["openUtc"])
            tick.high_price = float(tick_data["high24h"])
            tick.low_price = float(tick_data["low24h"])
            tick.last_price = float(tick_data["last"])
            tick.open_interest = float(tick_data["holding"])

            tick.volume = float(tick_data["baseVolume"])    #本币成交量
            tick.bid_price_1 = float(tick_data["bestBid"])
            tick.bid_volume_1 = float(tick_data["bidSz"])
            tick.ask_price_1 = float(tick_data["bestAsk"])
            tick.ask_volume_1 = float(tick_data["askSz"])
            self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_depth(self, packet: dict) -> None:
        """
        行情深度推送
        """
        data = packet["data"][0]
        inst_id = packet["arg"]["instId"]
        tick = self.ticks[inst_id]

        tick.datetime = get_local_datetime(int(data["ts"]))

        bids = data["bids"]
        asks = data["asks"]
        for index in range(len(bids)):
            price, volume = bids[index]
            tick.__setattr__("bid_price_" + str(index + 1), float(price))
            tick.__setattr__("bid_volume_" + str(index + 1), float(volume))

        for index in range(len(asks)):
            price, volume = asks[index]
            tick.__setattr__("ask_price_" + str(index + 1), float(price))
            tick.__setattr__("ask_volume_" + str(index + 1), float(volume))

        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    #------------------------------------------------------------------------------------------------- 
    def on_public_trade(self,packet):
        """
        收到公开成交回报
        """
        data = packet["data"][0]
        inst_id = packet["arg"]["instId"]
        tick = self.ticks[inst_id]
        tick.last_price = float(data["p"])
        tick.datetime = get_local_datetime(data["ts"])
#------------------------------------------------------------------------------------------------- 
class BitGetSTradeWebsocketApi(BitGetSWebsocketApiBase):
    """
    """
    def __init__(self, gateway):
        """
        """
        self.trade_count = 0
        super().__init__(gateway)
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        key: str,
        secret: str,
        passphrase:str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """
        """
        super().connect(
            key,
            secret,
            passphrase,
            WEBSOCKET_TRADE_HOST,
            proxy_host,
            proxy_port
        )
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self) -> int:
        """
        订阅私有频道
        """
        inst_types = ["UMCBL","CMCBL","DMCBL"]  #产品类型 UMCBL:专业合约私有频道,DMCBL:混合合约私有频道(币本位合约),CMCBL:USDC专业合约
        # 订阅持仓
        for inst_type in inst_types:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": inst_type,
                    "channel": "positions",
                    "instId": "default"
                }]
            }
            self.send_packet(req)
        # 订阅委托
        for inst_type in inst_types:
            req = {
                "op": "subscribe",
                "args": [{
                    "instType": inst_type,
                    "channel": "orders",
                    "instId": "default"
                }]
            }
            self.send_packet(req)
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接成功")
        self.login()
    #-------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws交易断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接断开")
    #------------------------------------------------------------------------------------------------- 
    def on_login(self) -> None:
        """
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API登录成功")
        self.subscribe()
    #------------------------------------------------------------------------------------------------- 
    def on_data(self, packet) -> None:
        """
        """
        channel = packet["arg"]["channel"]
        data = packet["data"]
        if "action" in packet:
            if channel == "positions":
                self.on_position(data)
            elif channel == "orders":
                self.on_order(data)
    #------------------------------------------------------------------------------------------------- 
    def on_order(self, raw: dict) -> None:
        """
        收到委托回报
        """
        for data in raw:
            order_datetime = get_local_datetime(data["cTime"])
            orderid = data["clOrdId"]
            order = OrderData(
                symbol=data["instId"],
                exchange=Exchange.BITGETS,
                orderid=orderid,
                type=ORDERTYPE_BITGETS2VT[data["ordType"]],
                direction=DIRECTION_BITGETS2VT[data["tS"]],
                price=float(data["px"]),
                volume=float(data["sz"]),
                traded=float(data["accFillSz"]),
                status=STATUS_BITGETS2VT[data["status"]],
                datetime = order_datetime,
                gateway_name=self.gateway_name
            )
            self.gateway.on_order(order)

            # 推送成交事件
            if not order.traded:
                return
            self.trade_count += 1
            trade = TradeData(
                symbol=order.symbol,
                exchange=Exchange.BITGETS,
                orderid=order.orderid,
                tradeid=str(self.trade_count),
                direction=order.direction,
                offset=order.offset,
                price=float(data["fillPx"]),
                volume=float(data["fillSz"]),
                datetime= get_local_datetime(int(data["fillTime"])),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    #------------------------------------------------------------------------------------------------- 
    def on_position(self,data:dict):
        """
        收到持仓回报
        """
        for pos_data in data:
            position = PositionData(
                symbol = pos_data["instId"],
                exchange = Exchange.BITGETS,
                direction = HOLDSIDE_BITGETS2VT[pos_data["holdSide"]],
                volume = float(pos_data["available"]),
                price = float(pos_data["averageOpenPrice"]),
                pnl = float(pos_data["upl"]),
                gateway_name = self.gateway_name
            )
            self.gateway.on_position(position)
#------------------------------------------------------------------------------------------------- 
def create_signature(secret,message):
    mac = hmac.new(bytes(secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256').digest()
    sign_str =  base64.b64encode(mac).decode()
    return sign_str
