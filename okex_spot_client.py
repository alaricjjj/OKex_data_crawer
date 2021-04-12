import utils
from requests import Request
from request_manager import RequestManager
import json




class okex_SpotClient():

    def __init__(self, api_key, api_seceret_key, passphrase, use_server_time=False,is_proxies = False):
        self.API_KEY = api_key
        self.API_SECRET_KEY = api_seceret_key
        self.PASSPHRASE = passphrase
        self.use_server_time = use_server_time
        self.BASE_URL = 'https://www.okex.com'
        self.is_proxies = is_proxies

    #
    # Authentication required methods
    #
    # def authentication_required(fn):
    #     """Annotation for methods that require auth."""
    #
    #     def wrapped(self, *args, **kwargs):
    #         if not (self.API_KEY):
    #             msg = "You must be authenticated to use this method"
    #             raise errors.AuthenticationError(msg)
    #         else:
    #             return fn(self, *args, **kwargs)
    #
    #     return wrapped

    '''钱包API'''

    # 获取币种列表
    def get_currencies(self):
        method = 'GET'
        path = '/api/account/v3/currencies'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取钱包账户信息
    def get_wallet(self):
        method = 'GET'
        path = '/api/account/v3/wallet'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取单一币种账户信息
    def get_wallet_symbol(self, symbol):
        method = 'GET'
        path = '/api/account/v3/wallet/{symbol}'.format(symbol=symbol)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)


    '''币币API'''

    # 币币账户信息
    # 获取币币账户资产列表(仅展示拥有资金的币对)，查询各币种的余额、冻结和可用等信息。
    def get_spot_accounts(self):
        method = 'GET'
        path = '/api/spot/v3/accounts'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取币币账户单个币种的余额、冻结和可用等信息。
    def get_spot_account_currency(self, currency):
        method = 'GET'
        path = '/api/spot/v3/accounts/{currency}'.format(currency=currency)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 账单流水查询
    def get_ledger(self, currency):
        method = 'GET'
        path = '/api/spot/v3/accounts/'+currency+'/ledger'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request, self.is_proxies)


    ## 下单
    # client_oid---	由您设置的订单ID来识别您的订单,格式是字母（区分大小写）+数字 或者 纯字母（区分大小写），1-32位字符 （不能重复）
    # type---limit或market（默认是limit）。当以market（市价）下单，order_type只能选择0（普通委托）
    # order_type---
    # 0：普通委托（order type不填或填0都是普通委托）
    # 1：只做Maker（Post only）
    # 2：全部成交或立即取消（FOK）
    # 3：立即成交并取消剩余（IOC）

    # 必填项为 side,instrument_id
    # 现价订单必填price和size
    # 市价订单必填size，变量notional=‘’

    def post_order(self, side,instrument_id,order_type=None,client_oid=None,type=None,price = None, size=None,notional=None):
        method = 'POST'
        path = '/api/spot/v3/orders'
        data = {
            'client_oid': client_oid,
            'type': type,
            'side':side,
            'instrument_id': instrument_id,
            'order_type': order_type,
            'price':price,
            'size':size,
            'notional':notional
        }
        data = json.dumps(data)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path, str(data))
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers,
            data=data
        )
        return RequestManager().send_request(my_request, self.is_proxies)

    # 批量下单
    def post_batch_orders(self, orders):
        orders = json.dumps(orders)
        method = 'POST'
        path = '/api/spot/v3/batch_orders'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path, str(orders))
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers,
            data=orders
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 撤销指定订单(需要知道order_id和instrument_id，可通过获取订单信息获得)
    def cancel_order(self, order_id, instrument_id):
        method = 'POST'
        path = '/api/spot/v3/cancel_orders/{order_id}'.format(order_id=order_id)
        data = {
            'instrument_id': instrument_id
        }
        data = json.dumps(data)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path, str(data))
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers,
            data=data
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 批量撤销订单
    def cancel_batch_orders(self, orders):
        orders = json.dumps(orders)
        method = 'POST'
        path = '/api/spot/v3/cancel_batch_orders'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path, str(orders))
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers,
            data=orders
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取订单列表
    def get_orders(self, state, instrument_id):
        method = 'GET'
        path = '/api/spot/v3/orders'
        params = {
            'state': state,
            'instrument_id': instrument_id
        }
        request_path = path + utils.parse_params_to_str(params)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, request_path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            params=params,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取所有未成交订单
    def get_pending_orders(self):
        method = 'GET'
        path = '/api/spot/v3/orders_pending'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 通过id获取订单信息
    def get_order_by_id(self, order_id, instrument_id):
        method = 'GET'
        path = '/api/spot/v3/orders/{order_id}'.format(order_id=order_id)
        params = {
            'instrument_id': instrument_id
        }
        request_path = path + utils.parse_params_to_str(params)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, request_path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            params=params,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取深度数据
    def get_depth(self, instrument_id):
        method = 'GET'
        path = '/api/spot/v3/instruments/{instrument_id}/book'.format(instrument_id=instrument_id)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取全部ticker信息
    def get_ticker(self):
        method = 'GET'
        path = '/api/spot/v3/instruments/ticker'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request, self.is_proxies)

    # 获取某个ticker信息
    def get_ticker_by_instrument_id(self, instrument_id):
        method = 'GET'
        path = '/api/spot/v3/instruments/{instrument_id}/ticker'.format(instrument_id=instrument_id)
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取k线数据
    def get_k_line(self, instrument_id,from_time = None, to_time = None, granularity='60'):
        if from_time == None and to_time == None:
            params = f'''?granularity={granularity}'''
        else:
            params = f'''?start={from_time}&end={to_time}&granularity={granularity}'''
        method = 'GET'
        path = '/api/spot/v3/instruments/{instrument_id}/candles'.format(instrument_id=instrument_id)
        path = path + params

        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    def get_history_klines(self, instrument_id,from_time = None, to_time = None, granularity='60'):
        if from_time == None and to_time == None:
            params = f'''?granularity={granularity}'''
        else:
            params = f'''?start={to_time}&end={from_time}&granularity={granularity}'''
        method = 'GET'
        path = '/api/spot/v3/instruments/{instrument_id}/history/candles'.format(instrument_id=instrument_id)
        path = path + params

        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request,self.is_proxies)

    # 获取当前账户交易手续费费率
    def get_trade_fee(self):
        method = 'GET'
        path = '/api/spot/v3/trade_fee'
        headers = utils.create_headers(self.API_KEY, self.API_SECRET_KEY, self.PASSPHRASE, method, path)
        my_request = Request(
            method=method,
            url=self.BASE_URL + path,
            headers=headers
        )
        return RequestManager().send_request(my_request, self.is_proxies)


if __name__ == '__main__':
    aa = okex_SpotClient( api_key= '',
                          api_seceret_key = '',
                          passphrase = '',
                          use_server_time=False,
                          is_proxies = True)
    # print(aa.get_k_line(instrument_id = 'BTC-USDT',granularity=3600))
    # print(aa.get_k_line(instrument_id = 'BTC-USDT',granularity=3600, from_time='2021-03-01T06:00:00.000Z',to_time='2021-03-05T08:00:00.000Z'))

    from_time = '2019-09-30T00:00:00.000Z'
    to_time =   '2019-10-05T08:00:00.000Z'

    print(aa.get_history_klines(instrument_id = 'BTC-USDT',
                                granularity = 3600,
                                from_time = from_time,
                                to_time = to_time))

    # from_time='2019-10-05T08:00:00.000Z',to_time='2019-10-01T06:00:00.000Z'