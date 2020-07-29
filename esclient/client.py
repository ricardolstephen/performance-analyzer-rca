#!/usr/bin/env python3
import sys
import logging
import json
import time
from time import sleep
import requests
from objects import *


# logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
SLEEP_TIME = 0.1


if __name__ == '__main__':
    host = default_host
    if len(sys.argv) == 1:
        exit()

    if sys.argv[1] == 'help':
        print(['verify', 'get_state', 'enable_metrics', 'get_metrics'])
    elif sys.argv[1] == 'verify':
        print(host.get_pretty_cluster_config())
        # host.verify_cluster_config()
    elif sys.argv[1] == 'get_state':
        print(host.get_cluster_state())
        print(host.get_node_state())
    elif sys.argv[1] == 'enable_metrics':
        host.enable_feature('pa')
        sleep(SLEEP_TIME)
        print(host.get_cluster_state())
        print(host.get_node_state())
    elif sys.argv[1] == 'get_metrics':
        data = host.get_metrics()
        print(json.dumps(data, indent=4))
    elif sys.argv[1] == 'enable_batch':
        host.enable_feature('batch')
        sleep(SLEEP_TIME)
        print(host.get_cluster_state())
        print(host.get_node_state())
    elif sys.argv[1] == 'get_batch':
        starttime = int(time.time()*1000)
        endtime = starttime - (60000) * 3
        period = None
        maxdatapoints = None
        if len(sys.argv) > 2:
            if sys.argv[2].startswith('period='):
                period = int(sys.argv[2].replace('period=', ''))
            elif sys.argv[2].startswith('maxdatapoints='):
                maxdatapoints = int(sys.argv[2].replace('maxdatapoints=', ''))
        if len(sys.argv) > 3:
            if sys.argv[3].startswith('period='):
                period = int(sys.argv[3].replace('period='), '')
            elif sys.argv[3].startswith('maxdatapoints='):
                maxdatapoints = int(sys.argv[3].replace('maxdatapoints=', ''))
        data = host.get_batch_metrics(starttime, endtime, period=period,
                                      maxdatapoints=maxdatapoints)
        print(json.dumps(data, indent=4))
    else:
        raise Exception(f'Invalid argument {sys.argv[1]}')
