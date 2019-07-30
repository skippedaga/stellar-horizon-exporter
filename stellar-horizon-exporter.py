from prometheus_client import start_http_server
from prometheus_client.core import SummaryMetricFamily, CounterMetricFamily, GaugeMetricFamily, REGISTRY
import argparse
import json
import logging
import sys
import time
from stellar_base.horizon import Horizon 
from collections import defaultdict
import copy
import requests

# logging setup
log = logging.getLogger('stellar-horizon-exporter')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

current_data = defaultdict(lambda: 0)
current_payment_detail = defaultdict(lambda: defaultdict(lambda: 0))
current_large_native_payment_detail = defaultdict(lambda: defaultdict(lambda: 0))

current_minute = None

class StatsCollector():

  def collect(self):

    yield SummaryMetricFamily('summary', 'This is simple summary', labels={'name': 'horizon.stellar.org'})

    log.info('current_data.items(): %s' %current_data.items())

    for k,v in current_data.items():
      yield CounterMetricFamily(k, 'stellar base metric values', value=float(v))

    log.info('current_payment_detail.items(): %s' %current_payment_detail.items())  

    for asset, asset_data in current_payment_detail.items():
      summ = CounterMetricFamily('sum_payment', 'stellar payment metric values', labels=['sum_payment'])
      summ.add_metric(asset, asset_data['sum'])
      yield summ
      yield CounterMetricFamily('nb_payment', 'stellar payment metric values', value=float(asset_data['nm'])) 
    
    metric = GaugeMetricFamily('large_native_payment_detail', 'large native stellar payment metric values', value=7)
    for from_addr, amount_by_dest in current_large_native_payment_detail.items():
        for to_addr, amount in amount_by_dest.items():
            metric.add_sample('sum_large_native_payment', value=amount, labels={'from_addr': from_addr, 'to_addr': to_addr})
    yield metric

def main_loop(server):

    horizon = Horizon(server);
    operations = horizon.operations()

    global current_minute

    try:
        for resp in operations['_embedded']['records']:
          
          cm = resp['created_at'][:-4]

          log.info('minute change %s => %s' % (current_minute, cm))
          if cm != current_minute:
             
            log.info('minute change %s => %s' % (current_minute, cm))

            current_minute = cm

            global current_data 
            global current_payment_detail 
            global current_large_native_payment_detail 

          op_type = resp['type']

          current_data['nb_operation'] += 1
          current_data['nb_operation_%s' % op_type] += 1

          if op_type == 'payment':
            current_data['total_amount_payment'] += float(resp['amount'])

            if resp['asset_type'] == 'native':
                asset = 'native'

                v = float(resp['amount'])
                if v >= 10000:

                    from_addr = resp['from']
                    to_addr = resp['to']
                    current_large_native_payment_detail[from_addr][to_addr] += v

            else:
                asset = resp['asset_code']

            current_payment_detail[asset]['nb'] += 1
            current_payment_detail[asset]['sum'] += float(resp['amount'])

    except requests.exceptions.HTTPError as e:
        log.info(str(e))
        lolog.infoo('http exception, restarting')
        return

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--port', type=int, help='The TCP port to listen on.', default=9101)
    parser.add_argument('--host', nargs='?', help='The URL exporter will connect.', default="https://horizon.stellar.org")
    args = parser.parse_args()
    log.info(args.port)
    log.info(args.host)

    start_http_server(args.port)
    REGISTRY.register(StatsCollector())
    while True:
        main_loop(args.host)
        time.sleep(10)
