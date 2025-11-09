from prometheus_client import Gauge
from pyflink.table.udf import ScalarFunction, udf, FunctionContext

from services.pushgateway_service import PushgatewayService

class SymbolPriceUdf(ScalarFunction):

    def __init__(self):
        self.gauge = None
        self.pushgateway_service = None

    def open(self, context: FunctionContext):
        self.pushgateway_service = PushgatewayService()
        registry = self.pushgateway_service.get_registry()
        self.gauge = Gauge(
            "symbol_price",
            "Current symbol price",
            ["symbol"],
            registry=registry)

    def eval(self, symbol : str, price : float):
        self.gauge.labels(symbol=symbol).set(price)
        self.pushgateway_service.push_metric()
        return symbol
