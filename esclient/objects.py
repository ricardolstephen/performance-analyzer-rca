import logging
import json
import requests


logger = logging.getLogger(__name__)


BATCH_ENABLED = True
KNOWN_FEATURES = {'pa': False, 'rca': False, 'logging': False}
if BATCH_ENABLED:
    KNOWN_FEATURES['batch'] = False
KNOWN_OTHERS = {'shards_per_collection': 0}

class ClusterConfig:

    def __init__(self, es_version="7.3.2",
                 nodes=['elasticsearch1', 'elasticsearch2']):
        self.es_version = es_version
        self.nodes = nodes


class SifiState:

    PA_ENABLED_BIT_POS = 0
    RCA_ENABLED_BIT_POS = 1
    LOGGING_ENABLED_BIT_POS = 2
    BATCH_ENABLED_BIT_POS = 3

    def __init__(self, features=None, others=None):
        if not features:
            features = KNOWN_FEATURES.copy()
        if not others:
            others = KNOWN_OTHERS.copy()
        if features.keys() != KNOWN_FEATURES.keys():
            raise ValueError(f'Invalid feature set {features.keys()}')
        for k, v in features.items():
            if not isinstance(v, type(KNOWN_FEATURES[k])):
                raise ValueError(f'Invalid feature value: {k}, {v}')
        if others.keys() != KNOWN_OTHERS.keys():
            raise ValueError(f'Invalid others set {others.keys()}')
        for k, v in others.items():
            if not isinstance(v, type(KNOWN_OTHERS[k])):
                raise ValueError(f'Invalid others value: {k}, {v}')
        self.features = features
        self.others = others

    @classmethod
    def parse_from_cluster_state(cls, state):
        features = {}
        features['pa'] = bool(state['currentPerformanceAnalyzerClusterState'] &
                              1 << cls.PA_ENABLED_BIT_POS)
        features['rca'] = bool(state['currentPerformanceAnalyzerClusterState'] &
                               1 << cls.RCA_ENABLED_BIT_POS)
        features['logging'] = bool(state['currentPerformanceAnalyzerClusterState'] &
                                   1 << cls.LOGGING_ENABLED_BIT_POS)
        if BATCH_ENABLED:
            features['batch'] = bool(state['currentPerformanceAnalyzerClusterState'] &
                                     1 << cls.BATCH_ENABLED_BIT_POS)
        others = {}
        others['shards_per_collection'] = state['shardsPerCollection']
        return SifiState(features=features, others=others)

    @staticmethod
    def parse_from_node_state(state):
        features = {}
        features['pa'] = state['performanceAnalyzerEnabled']
        features['rca'] = state['rcaEnabled']
        features['logging'] = state['loggingEnabled']
        if BATCH_ENABLED:
            features['batch'] = state['batchMetricsEnabled']
        others = {}
        others['shards_per_collection'] = state['shardsPerCollection']
        return SifiState(features=features, others=others)

    def __getitem__(self, key):
        try:
            return self.features[key]
        except KeyError:
            return self.others[key]

    def __setitem__(self, key, value):
        if key in self.features:
            self.features[key] = value
        elif key in self.others:
            self.others[key] = value
        else:
            raise ValueError(f'Invalid key {key}.')

    def __repr__(self):
        return json.dumps(dict(self.features, **self.others))

    def __eq__(self, other):
        return self.features == other.features and self.others == other.others

    def __hash__(self):
        return hash(str(self))

    def __copy__(self):
        return SifiState(features = self.features.copy(),
                         others = self.others.copy())


default_cluster_config = ClusterConfig()
default_sifi_state = SifiState()


DEFAULT_TIMEOUT = 1


class Host:

    def __init__(self, protocol='http', hostname='localhost', port=9200,
                 cluster_config=default_cluster_config):
        self.protocol = protocol
        self.hostname = hostname
        self.port = port
        self.cluster_config = cluster_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_pretty_cluster_config(self):
        data = []
        resp = requests.get(self.url(), timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data.append(json.dumps(resp.json(), indent=4))
        data.append('\n\n')
        resp = requests.get(self.url('/_cat/nodes?v'), timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data.append(resp.text)
        data.append('\n')
        resp = requests.get(self.url('/_cat/plugins?v'),
                            timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data.append(resp.text)
        return ''.join(data)

    def verify_cluster_config(self):
        resp = requests.get(self.url(), timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if payload['version']['number'] != self.cluster_config.es_version:
            raise Exception(f'ES version mismatch:\n{resp}')
        if payload['name'] not in self.cluster_config.nodes:
            raise Exception(f'Unexpected node {resp["name"]}')
        resp = requests.get(self.url('/_cat/nodes?v'), timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        payload = resp.text
        for node in self.cluster_config.nodes:
            if node not in payload:
            	raise Exception(f'Missing node {node}')
        # TODO should check that all the present nodes are valid
        resp = requests.get(self.url('/_cat/plugins?v'),
                            timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        payload = resp.text
        for node in self.cluster_config.nodes:
            if ' '.join((node, 'opendistro_performance_analyzer')) not in payload:
            	raise Exception(f'Missing pa plugin in node {node}')
        return True

    def get_cluster_state(self):
        resp = requests.get(self.url('/_opendistro/_performanceanalyzer/cluster/config'),
                            timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        state = SifiState.parse_from_cluster_state(resp.json())
        self.logger.info(state)
        return state

    def get_node_state(self):
        resp = requests.get(self.url('/_opendistro/_performanceanalyzer/config'),
                            timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        state = SifiState.parse_from_node_state(resp.json())
        self.logger.info(state)
        return state

    def enable_feature(self, feature, enable=True, node=False):
        if feature not in KNOWN_FEATURES:
            raise ValueError(f'Unknown feature {feature}.')
        if feature == 'pa':
            path_ext = ''
        else:
            path_ext = f'{feature}/'
        if node:
            path = f'/_opendistro/_performanceanalyzer/{path_ext}config'
        else:
            path = f'/_opendistro/_performanceanalyzer/{path_ext}cluster/config'
        resp = requests.post(self.url(path), json={'enabled': enable},
                             timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp

    def get_metrics(self):
        url = ''.join((self.protocol, '://', self.hostname, ':',
                       '9600',
                       '/_opendistro/_performanceanalyzer/metrics?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all'))
        resp = requests.get(url, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        self.logger.info(resp.text)
        return resp.json()

    def get_batch_metrics(self, starttime, endtime):
        url = ''.join((self.protocol, '://', self.hostname, ':',
                       '9600',
                       '/_opendistro/_performanceanalyzer/batch',
                       '?metrics=Latency,CPU_Utilization',
                       '&starttime=',
                       str(starttime),
                       '&endtime=',
                       str(endtime)))
        resp = requests.get(url, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        self.logger.info(resp.text)
        return resp.json()        

    def reset_cluster_state(self):
        self.enable_feature('pa', enable=False)

    def reset_node_state(self):
        self.enable_feature('pa', enable=False, node=True)

    def reset_state(self):
        self.reset_cluster_state()
        self.reset_node_state()

    def url(self, suffix=''):
        return ''.join((self.protocol, '://', self.hostname, ':',
                        str(self.port), suffix))

    def __repr__(self):
        return self.url()

    
default_host = Host()
