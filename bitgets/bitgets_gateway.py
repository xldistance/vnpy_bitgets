import base64
import csv
import hashlib
import hmac
import json
import re
import sys
import urllib
import zlib
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from time import sleep, time
from typing import Any, Dict, List, Union
from urllib.parse import urlencode

from peewee import chunked
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import bitget_account  # 导入账户字典
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    extract_vt_symbol,
    get_folder_path,
    get_local_datetime,
    is_target_contract,
    load_json,
    save_json,
    remain_digit
)

REST_HOST = "https://api.bitget.com"
WEBSOCKET_DATA_HOST = "wss://ws.bitget.com/v2/ws/public"  # ws公共频道
WEBSOCKET_TRADE_HOST = "wss://ws.bitget.com/v2/ws/private"  # ws私有频道

STATUS_BITGETS2VT: Dict[int, Status] = {
    "live": Status.NOTTRADED,
    "partially_filled": Status.PARTTRADED,
    "filled": Status.ALLTRADED,
    "canceled": Status.CANCELLED,
}

ORDERTYPE_VT2BITGETS: Dict[OrderType, Any] = {OrderType.LIMIT: "limit", OrderType.MARKET: "market"}
ORDERTYPE_BITGETS2VT: Dict[Any, OrderType] = {v: k for k, v in ORDERTYPE_VT2BITGETS.items()}

DIRECTION_VT2BITGETS: Dict[Direction, str] = {
    Direction.LONG: "buy",
    Direction.SHORT: "sell",
}
DIRECTION_BITGETS2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BITGETS.items()}

HOLDSIDE_BITGETS2VT: Dict[str, Direction] = {"long": Direction.LONG, "short": Direction.SHORT}
OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}

INTERVAL_VT2BITGETS: Dict[Interval, str] = {Interval.MINUTE: "1m", Interval.HOUR: "1H", Interval.DAILY: "1D"}

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
    # default_setting由vnpy.trader.ui.widget调用
    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "会话数": 3,
        "host": "",
        "port": "",
    }

    exchanges = [Exchange.BITGETS]  # 由main_engine add_gateway调用
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine):
        """ """
        super(BitGetSGateway, self).__init__(event_engine, "BITGETS")
        self.orders: Dict[str, OrderData] = {}
        self.rest_api = BitGetSRestApi(self)
        self.trade_ws_api = BitGetSTradeWebsocketApi(self)
        self.market_ws_api = BitGetSDataWebsocketApi(self)
        self.count = 0  # 轮询计时:秒
        # 所有合约列表
        self.recording_list = GetFilePath.recording_list
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 查询历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if is_target_contract(vt_symbol, self.gateway_name)]
        self.leverage_contracts = copy(self.recording_list)
        # 下载历史数据状态
        self.history_status: bool = True
        # 订阅逐笔成交数据状态
        self.book_trade_status: bool = False
    # ----------------------------------------------------------------------------------------------------
    def connect(self, log_account: dict = {}):
        """ """
        if not log_account:
            log_account = bitget_account
        key = log_account["key"]
        secret = log_account["secret"]
        passphrase = log_account["passphrase"]
        proxy_host = log_account["host"]
        proxy_port = log_account["port"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(key, secret, passphrase, proxy_host, proxy_port)
        self.trade_ws_api.connect(key, secret, passphrase, proxy_host, proxy_port)
        self.market_ws_api.connect(key, secret, passphrase, proxy_host, proxy_port)

        self.init_query()
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        self.market_ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> Request:
        """
        查询账户
        """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_order(self, symbol: str):
        """
        查询活动委托单
        """
        self.rest_api.query_order(symbol)
    # ----------------------------------------------------------------------------------------------------
    def query_position(self, symbol: str):
        """
        查询持仓
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event: Event):
        """
        查询合约历史数据
        """
        if len(self.history_contracts) > 0:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
    # -------------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        收到委托单推送，BaseGateway推送数据
        """
        self.orders[order.vt_orderid] = copy(order)
        super().on_order(order)
    # -------------------------------------------------------------------------------------------------------
    def get_order(self, vt_orderid: str) -> OrderData:
        """
        用vt_orderid获取委托单数据
        """
        return self.orders.get(vt_orderid, None)
    # ----------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭接口
        """
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event: Event):
        """
        处理定时任务
        """
        if self.query_contracts:
            vt_symbol = self.query_contracts.pop(0)
            symbol, exchange, gateway_name = extract_vt_symbol(vt_symbol)
            self.rest_api.query_order(symbol)
            self.query_contracts.append(vt_symbol)
        if self.leverage_contracts:
            symbol = extract_vt_symbol(self.leverage_contracts.pop(0))[0]
            self.rest_api.set_leverage(symbol)
            self.rest_api.set_margin_mode(symbol)
        self.count += 1
        if self.count < 3:
            return
        self.count = 0
        self.query_account()
    # ----------------------------------------------------------------------------------------------------
    def init_query(self):
        """
        初始化定时查询
        """
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
# ----------------------------------------------------------------------------------------------------
class BitGetSRestApi(RestClient):
    """
    BITGET REST API
    """

    def __init__(self, gateway: BitGetSGateway):
        """ """
        super().__init__()

        self.gateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.host: str = ""
        self.key: str = ""
        self.secret: str = ""

        self.order_count: int = 10000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}
        self.product_types = ["USDT-FUTURES", "USDC-FUTURES", "COIN-FUTURES"]  # USDT,USDC,币本位合约
        self.margin_coin_map = {
            "UMCBL": "USDT",
            "CMCBL": "USDC",
        }
        self.product_type_map = {
            "USDT":"USDT-FUTURES",
            "USDC":"USDC-FUTURES"
        }
        self.delivery_date_map: Dict[str, str] = {}
        self.contract_inited: bool = False
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request) -> Request:
        """
        生成签名
        """
        timestamp = str(int(time() * 1000))
        path = request.path
        method = request.method
        body = ""

        if method == "GET":
            params = sorted(request.params.items())
            path += "?" + urlencode(params)
        elif method == "POST":
            body = json.dumps(request.data)
            request.data = body

        message = f"{timestamp}{method}{path}{body}"
        signature = create_signature(self.secret, message)

        if not request.headers:
            request.headers = {}
            request.headers["ACCESS-KEY"] = self.key
            request.headers["ACCESS-SIGN"] = signature
            request.headers["ACCESS-TIMESTAMP"] = timestamp
            request.headers["ACCESS-PASSPHRASE"] = self.passphrase
            request.headers["Content-Type"] = "application/json"
        return request
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, passphrase: str, proxy_host: str, proxy_port: int) -> None:
        """
        连接REST服务
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.connect_time = int(datetime.now(TZ_INFO).strftime("%y%m%d%H%M%S"))

        self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()

        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")

        self.query_contract()
        self.set_position_mode()
    # ----------------------------------------------------------------------------------------------------
    def get_product_margin(self, symbol: str):
        """
        获取产品类型，保证金币种
        """
        if symbol.endswith("PERP"):
            margin_coin = "USDC"
        elif symbol.endswith("USDT"):
            margin_coin = "USDT"
        else:
            margin_coin = symbol.split("USD")[0]
        productType = self.product_type_map.get(margin_coin,"COIN-FUTURES")
        return productType,margin_coin
    # ----------------------------------------------------------------------------------------------------
    def set_leverage(self, symbol: str):
        """
        设置杠杆
        """
        product_type,margin_coin = self.get_product_margin(symbol)
        data = {
            "symbol": symbol,
            "productType":product_type,
            "marginCoin": margin_coin,
            "leverage": 20,
        }
        self.add_request(method="POST", path="/api/v2/mix/account/set-leverage", callback=self.on_leverage, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self, data: dict, request: Request):
        pass
    # ----------------------------------------------------------------------------------------------------
    def set_margin_mode(self,symbol:str):
        """
        设置全仓保证金模式
        """
        product_type,margin_coin = self.get_product_margin(symbol)
        data = {
            "symbol": symbol,
            "productType":product_type,
            "marginCoin": margin_coin,
            "marginMode": "crossed",
        }
        self.add_request(method="POST", path="/api/v2/mix/account/set-margin-mode", callback=self.on_margin_mode, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_margin_mode(self,data: dict, request: Request):
        pass
    # ----------------------------------------------------------------------------------------------------
    def set_position_mode(self):
        """
        设置单向持仓模式
        """
        for type_ in self.product_types:
            data = {
                "productType":type_,
                "posMode": "one_way_mode",
            }
            self.add_request(method="POST", path="/api/v2/mix/account/set-position-mode", callback=self.on_position_mode, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_position_mode(self,data:dict,request:Request):
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> Request:
        """
        查询账户数据
        """
        for product in self.product_types:
            params = {"productType": product}
            self.add_request(method="GET", path="/api/v2/mix/account/accounts", callback=self.on_query_account, params=params)
    # ----------------------------------------------------------------------------------------------------
    def query_order(self, symbol: str):
        """
        查询合约活动委托单
        """
        product_type = self.get_product_margin(symbol)[0]
        params = {"productType": product_type}
        self.add_request(method="GET", path="/api/v2/mix/order/orders-pending", callback=self.on_query_order, params=params, extra=symbol)
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self) -> Request:
        """
        获取合约信息
        """
        for product in self.product_types:
            params = {"productType": product}
            self.add_request(
                method="GET",
                path="/api/v2/mix/market/contracts",
                params=params,
                callback=self.on_query_contract,
            )
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history = []
        count = 200
        start = req.start
        time_delta = TIMEDELTA_MAP[req.interval]
        time_consuming_start = time()
        symbol = req.symbol
        product_type = self.get_product_margin(req.symbol)[0]
        while True:
            end = start + time_delta * count

            # 构建查询参数
            params = {
                "symbol": symbol,
                "productType":product_type,
                "granularity": INTERVAL_VT2BITGETS[req.interval],
                "startTime": str(int(start.timestamp() * 1000)),
                "endTime": str(int(end.timestamp() * 1000)),
                "limit": str(count),
            }
            resp = self.request("GET", "/api/v2/mix/market/candles", params=params)

            if not resp or resp.status_code // 100 != 2:
                msg = f"获取历史数据失败，状态码：{getattr(resp, 'status_code', '未知')}, 信息：{getattr(resp, 'text', '')}"
                self.gateway.write_log(msg)
                break

            rawdata = resp.json()["data"]
            buf = [BarData(
                symbol=symbol,
                exchange=req.exchange,
                datetime=get_local_datetime(int(data[0])),
                interval=req.interval,
                volume=float(data[5]),
                open_price=float(data[1]),
                high_price=float(data[2]),
                low_price=float(data[3]),
                close_price=float(data[4]),
                gateway_name=self.gateway_name,
            ) for data in rawdata]

            history.extend(buf)
            start = buf[-1].datetime if buf else start

            # 结束条件检查
            if len(buf) < count or start >= req.end:
                break

        if history:
            try:
                database_manager.save_bar_data(history, False)
            except Exception as err:
                self.gateway.write_log(f"保存数据库出错：{err}")
            time_consuming_end = time()
            query_time = round(time_consuming_end - time_consuming_start, 3)
            msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime}，结束时间：{history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
            self.gateway.write_log(msg)
        else:
            msg = f"未查询到合约：{req.vt_symbol}历史数据，请核实行情连接"
            self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def new_local_orderid(self) -> str:
        """
        生成local_orderid
        """
        with self.order_count_lock:
            self.order_count += 1
            local_orderid = str(self.connect_time + self.order_count)
            return local_orderid
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        发送委托单
        """
        local_orderid = req.symbol + "-" + self.new_local_orderid()
        order = req.create_order_data(local_orderid, self.gateway_name)
        order.datetime = datetime.now(TZ_INFO)
        req_symbol = req.symbol
        product_type,margin_coin = self.get_product_margin(req.symbol)
        data = {
            "symbol": req_symbol,
            "productType": product_type,
            "marginCoin": margin_coin,
            "marginMode":"crossed",
            "clientOid": local_orderid,
            "price": str(req.price),
            "size": str(req.volume),
            "side": DIRECTION_VT2BITGETS.get(req.direction),
            "orderType": ORDERTYPE_VT2BITGETS.get(req.type),
            "force": "gtc",
        }
        if req.offset == Offset.CLOSE:
            data["reduceOnly"] = "YES"
        else:
            data["reduceOnly"] = "NO"

        self.add_request(
            method="POST",
            path="/api/v2/mix/order/place-order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )

        self.gateway.on_order(order)
        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> Request:
        """
        取消委托单
        """
        order = self.gateway.get_order(req.vt_orderid)
        product_type,margin_coin = self.get_product_margin(req.symbol)
        data = {"symbol": req.symbol,"productType":product_type, "marginCoin": margin_coin, "clientOid": req.orderid}
        self.add_request(
            method="POST", path="/api/v2/mix/order/cancel-order", callback=self.on_cancel_order, on_failed=self.on_cancel_order_failed, data=data, extra=order
        )
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        收到账户数据回报
        """
        if self.check_error(data, "查询账户"):
            return
        for account_data in data["data"]:
            account = AccountData(
                accountid=account_data["marginCoin"] + "_" + self.gateway_name,
                balance=float(account_data["accountEquity"]),
                available=float(account_data["available"]),
                position_profit=float(account_data["crossedUnrealizedPL"]) if account_data["crossedUnrealizedPL"] else 0,         # 全仓未实现盈亏
                frozen=float(account_data["locked"]),
                datetime=datetime.now(TZ_INFO),
                file_name=self.gateway.account_file_name,
                gateway_name=self.gateway_name,
            )
            if account.balance:
                self.gateway.on_account(account)
                # 保存账户资金信息
                self.accounts_info[account.accountid] = account.__dict__
        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)
    # ----------------------------------------------------------------------------------------------------
    def on_query_order(self, data: dict, request: Request) -> None:
        """
        收到委托回报
        """
        if self.check_error(data, "查询活动委托"):
            return
        data = data["data"]
        if not data or not data["entrustedList"]:
            return
        for order_data in data["entrustedList"]:
            order_datetime = get_local_datetime(int(order_data["cTime"]))
            order = OrderData(
                orderid=order_data["clientOid"],
                symbol=order_data["symbol"],
                exchange=Exchange.BITGETS,
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                type=ORDERTYPE_BITGETS2VT[order_data["orderType"]],
                direction=DIRECTION_BITGETS2VT[order_data["side"]],
                traded=float(order_data["baseVolume"]),
                status=STATUS_BITGETS2VT[order_data["status"]],
                datetime=order_datetime,
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"] == "YES":
                order.offset = Offset.CLOSE

            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
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
                size=20,  # 合约杠杆
                min_volume=float(contract_data["minTradeNum"]),
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            # 保存交割合约名称
            if contract.name[-1].isdigit():
                delivery_time = get_local_datetime(int(contract_data["deliveryTime"]))
                contract.name = contract_data["baseCoin"] + contract_data["quoteCoin"] + datetime.strftime(delivery_time, "%y%m%d")
                self.delivery_date_map[contract.symbol] = contract.name
                # 过滤过期交割合约推送
                contract_postfix = str(datetime.now().year)[:2] + remain_digit(contract.name)
                current_postfix = datetime.now().strftime("%Y%m%d")
                if int(contract_postfix) <= int(current_postfix):
                    continue
            self.gateway.on_contract(contract)
        margin_coin = self.get_product_margin(contract.symbol)[0]
        self.gateway.write_log(f"交易接口：{self.gateway_name}，{margin_coin}合约信息查询成功")
        self.contract_inited = True
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data: dict, request: Request) -> None:
        """ """
        order = request.extra
        if self.check_error(data, "委托"):
            order.status = Status.REJECTED
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code, request: Request) -> None:
        """
        收到委托失败回报
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        """
        self.check_error(data, "撤单")
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order_failed(self, status_code, request: Request) -> None:
        """
        收到撤单失败回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        Callback to handler request exception.
        """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(exception_type, exception_value, tb, request))
    # ----------------------------------------------------------------------------------------------------
    def check_error(self, data: dict, func: str = "") -> bool:
        """ """
        if data["msg"] == "success":
            return False

        error_code = data["code"]
        error_msg = data["msg"]
        self.gateway.write_log(f"{func}请求出错，代码：{error_code}，信息：{error_msg}")
        return True
# ----------------------------------------------------------------------------------------------------
class BitGetSWebsocketApiBase(WebsocketClient):
    """ """

    def __init__(self, gateway):
        """ """
        super(BitGetSWebsocketApiBase, self).__init__()

        self.gateway: BitGetSGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""
        self.count = 0
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, passphrase: str, url: str, proxy_host: str, proxy_port: int) -> None:
        """ """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase

        self.init(url, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
        self.gateway.event_engine.register(EVENT_TIMER, self.send_ping)
    # ----------------------------------------------------------------------------------------------------
    def send_ping(self, event):
        self.count += 1
        if self.count < 20:
            return
        self.count = 0
        self.send_packet("ping")
    # ----------------------------------------------------------------------------------------------------
    def login(self) -> int:
        """ """
        timestamp = str(int(time()))
        message = timestamp + "GET" + "/user/verify"
        signature = create_signature(self.secret, message)
        params = {"op": "login", "args": [{"apiKey": self.key, "passphrase": self.passphrase, "timestamp": timestamp, "sign": signature}]}
        return self.send_packet(params)
    # ----------------------------------------------------------------------------------------------------
    def on_login(self, packet) -> None:
        """ """
        pass
    # ----------------------------------------------------------------------------------------------------
    def on_data(self, packet):
        pass
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: Union[str, dict]) -> None:
        """ """
        if packet == "pong":
            return
        if "event" in packet:
            if packet["event"] == "login" and packet["code"] == 0:
                self.on_login()
            elif packet["event"] == "error":
                self.on_error_msg(packet)
        else:
            self.on_data(packet)
    # ----------------------------------------------------------------------------------------------------
    def on_error_msg(self, packet) -> None:
        """ """
        msg = packet["msg"]
        self.gateway.write_log(f"交易接口：{self.gateway_name} WebSocket API收到错误回报，回报信息：{msg}")


# ----------------------------------------------------------------------------------------------------
class BitGetSDataWebsocketApi(BitGetSWebsocketApiBase):
    """ """

    def __init__(self, gateway: BitGetSGateway):
        """ """
        super().__init__(gateway)
        self.ticks: Dict[str, TickData] = {}
        self.topic_map = {
            "ticker":self.on_tick,
            "books5":self.on_depth,
            "trade": self.on_public_trade
        }
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, passphrase: str, proxy_host: str, proxy_port: int) -> None:
        """ """
        super().connect(key, secret, passphrase, WEBSOCKET_DATA_HOST, proxy_host, proxy_port)
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接成功")

        for symbol in list(self.ticks):
            self.subscribe_data(symbol)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws行情断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，行情Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅合约
        """
        # 等待rest合约数据推送完成再订阅
        while not self.gateway.rest_api.contract_inited:
            sleep(1)

        tick = TickData(
            symbol=req.symbol,
            name=req.symbol,
            exchange=Exchange.BITGETS,
            datetime=datetime.now(TZ_INFO),
            gateway_name=self.gateway_name,
        )
        symbol = tick.symbol
        self.ticks[symbol] = tick
        self.subscribe_data(symbol)
    # ----------------------------------------------------------------------------------------------------
    def topic_subscribe(self, symbol:str,channel: str, product_type: str):
        """
        主题订阅
        """
        req = {"op": "subscribe", "args": [{"instType": product_type, "channel": channel, "instId": symbol}]}
        self.send_packet(req)
    # ----------------------------------------------------------------------------------------------------
    def subscribe_data(self, symbol: str) -> None:
        """
        订阅市场深度主题
        """
        # 订阅tick，行情深度
        channels = ["ticker", "books5"]
        if self.gateway.book_trade_status:
            # 订阅逐笔成交
            channels.append("trade")
        for channel in channels:
            product_type = self.gateway.rest_api.get_product_margin(symbol)[0]
            self.topic_subscribe(symbol,channel, product_type)
    # ----------------------------------------------------------------------------------------------------
    def on_data(self, packet) -> None:
        """ """
        channel = packet["arg"]["channel"]  # 全量推送snapshot，增量推送update
        if "action" in packet:
            self.topic_map[channel](packet)
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, packet: dict) -> None:
        """
        收到tick数据推送
        """
        data = packet["data"]
        for tick_data in data:
            symbol = tick_data["instId"]
            tick = self.ticks[symbol]
            tick.datetime = get_local_datetime(int(tick_data["ts"]))
            tick.open_price = float(tick_data["open24h"])
            tick.high_price = float(tick_data["high24h"])
            tick.low_price = float(tick_data["low24h"])
            tick.last_price = float(tick_data["lastPr"])
            tick.open_interest = float(tick_data["holdingAmount"])
            tick.volume = float(tick_data["baseVolume"])  #近24小时币的成交量，quoteVolume：usd成交量，baseVolume：本币成交量
            tick.bid_price_1 = float(tick_data["bidPr"])
            tick.bid_volume_1 = float(tick_data["bidSz"])
            tick.ask_price_1 = float(tick_data["askPr"])
            tick.ask_volume_1 = float(tick_data["askSz"])
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_depth(self, packet: dict) -> None:
        """
        行情深度推送
        """
        data = packet["data"][0]
        symbol = packet["arg"]["instId"]
        tick = self.ticks[symbol]

        tick.datetime = get_local_datetime(int(data["ts"]))

        # 辅助函数，用于设置tick属性
        def set_tick_attributes(side: str, orders: list):
            attr_prefix = "bid_" if side == "bids" else "ask_"
            for index, (price, volume) in enumerate(orders, start=1):
                setattr(tick, f"{attr_prefix}price_{index}", float(price))
                setattr(tick, f"{attr_prefix}volume_{index}", float(volume))

        # 处理买单和卖单
        set_tick_attributes("bids", data["bids"])
        set_tick_attributes("asks", data["asks"])

        if tick.last_price:
            self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, packet):
        """
        收到逐笔成交回报
        """
        data = packet["data"][0]
        symbol = packet["arg"]["instId"]
        tick = self.ticks[symbol]
        tick.last_price = float(data["price"])
        tick.datetime = get_local_datetime(int(data["ts"]))
# ----------------------------------------------------------------------------------------------------
class BitGetSTradeWebsocketApi(BitGetSWebsocketApiBase):
    """ """

    def __init__(self, gateway: BitGetSGateway):
        """ """
        self.trade_count = 0
        super().__init__(gateway)
    # ----------------------------------------------------------------------------------------------------
    def connect(self, key: str, secret: str, passphrase: str, proxy_host: str, proxy_port: int) -> None:
        """ """
        super().connect(key, secret, passphrase, WEBSOCKET_TRADE_HOST, proxy_host, proxy_port)
    # ----------------------------------------------------------------------------------------------------
    def subscribe_private(self) -> int:
        """
        订阅私有频道
        """
        inst_types = self.gateway.rest_api.product_types
        # 订阅持仓
        for inst_type in inst_types:
            req = {"op": "subscribe", "args": [{"instType": inst_type, "channel": "positions", "instId": "default"}]}
            self.send_packet(req)
        # 订阅委托
        for inst_type in inst_types:
            req = {"op": "subscribe", "args": [{"instType": inst_type, "channel": "orders", "instId": "default"}]}
            self.send_packet(req)
        # 订阅成交
        for inst_type in inst_types:
            req = {"op": "subscribe", "args": [{"instType": inst_type, "channel": "fill", "instId": "default"}]}
            self.send_packet(req)
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接成功")
        self.login()
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self):
        """
        ws交易断开回调
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def on_login(self) -> None:
        """ """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，交易Websocket API登录成功")
        self.subscribe_private()
    # ----------------------------------------------------------------------------------------------------
    def on_data(self, packet) -> None:
        """ """
        channel = packet["arg"]["channel"]
        data = packet["data"]
        if "action" in packet:
            if channel == "positions":
                self.on_position(data)
            elif channel == "orders":
                self.on_order(data)
            elif channel == "fill":
                self.on_trade(data)
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, data: dict) -> None:
        """
        收到委托回报
        """
        for order_data in data:
            order_datetime = get_local_datetime(int(order_data["cTime"]))
            order = OrderData(
                symbol=order_data["instId"],
                exchange=Exchange.BITGETS,
                orderid=order_data["clientOid"],
                type=ORDERTYPE_BITGETS2VT[order_data["orderType"]],
                direction=DIRECTION_BITGETS2VT[order_data["side"]],
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                traded=float(order_data["accBaseVolume"]),
                status=STATUS_BITGETS2VT[order_data["status"]],
                datetime=order_datetime,
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"] == "yes":
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
            
    def on_trade(self, data: dict) -> None:
        """
        收到成交回报
        """
        for trade_data in data:
            self.trade_count += 1
            trade = TradeData(
                symbol=trade_data["symbol"],
                exchange=Exchange.BITGETS,
                orderid=trade_data["orderId"],
                tradeid=str(self.trade_count),
                direction=DIRECTION_BITGETS2VT[trade_data["side"]],
                price=float(trade_data["price"]),
                volume=float(trade_data["baseVolume"]),
                datetime=get_local_datetime(int(trade_data["uTime"])),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_trade(trade)
    # ----------------------------------------------------------------------------------------------------
    def on_position(self, data: dict):
        """
        收到持仓回报
        """
        for pos_data in data:
            position = PositionData(
                symbol=pos_data["instId"],
                exchange=Exchange.BITGETS,
                direction=HOLDSIDE_BITGETS2VT[pos_data["holdSide"]],
                volume=float(pos_data["available"]),
                price=float(pos_data["openPriceAvg"]),
                pnl=float(pos_data["unrealizedPL"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)
# ----------------------------------------------------------------------------------------------------
def create_signature(secret: str, message: str):
    mac = hmac.new(bytes(secret, encoding="utf8"), bytes(message, encoding="utf-8"), digestmod="sha256").digest()
    sign_str = base64.b64encode(mac).decode()
    return sign_str
