#!/usr/bin/env python3
import unittest
import copy
from time import sleep
from objects import *
from requests.exceptions import HTTPError

logging.basicConfig(level='INFO')
SLEEP_TIME = 0.1


class TestClusterStates(unittest.TestCase):

    def setUp(self):
        print()
        self.host = default_host
        self.host.verify_cluster_config()
        self.host.reset_state()
        sleep(SLEEP_TIME)
        self.assertEqual(self.host.get_cluster_state(), default_sifi_state)
        self.assertEqual(self.host.get_node_state(), default_sifi_state)
        self.other = 'batch'
        self.logger = logging.getLogger(self.__class__.__name__)

    def test_10(self):
        self.host.enable_feature('pa')
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state = copy.copy(default_sifi_state)
        expected_state['pa'] = True
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature('pa', enable=False)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_01(self):
        self.host.enable_feature(self.other)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_11(self):
        self.host.enable_feature('pa')
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state = copy.copy(default_sifi_state)
        expected_state['pa'] = True
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature(self.other)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state[self.other] = True
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature(self.other, enable=False)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state[self.other] = False
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature('pa', enable=False)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_11_disable_pa(self):
        self.host.enable_feature('pa')
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state = copy.copy(default_sifi_state)
        expected_state['pa'] = True
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature(self.other)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_state[self.other] = True
        self.assertEqual(cluster_state, expected_state)
        self.assertEqual(node_state, expected_state)
        self.host.enable_feature('pa', False)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_00_10(self):
        self.host.enable_feature('pa', node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state = copy.copy(default_sifi_state)
        expected_node_state['pa'] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature('pa', enable=False, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_00_01(self):
        # self.host.enable_feature(self.other, node=True)
        with self.assertRaises(HTTPError) as cm:
            self.host.enable_feature(self.other, node=True)
        self.logger.info(f'Got expected HTTPError: {cm.exception.response}')
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_00_11(self):
        self.host.enable_feature('pa', node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state = copy.copy(default_sifi_state)
        expected_node_state['pa'] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state[self.other] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other, enable=False, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state[self.other] = False
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature('pa', enable=False, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    def test_00_11_disable_pa(self):
        self.host.enable_feature('pa', node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state = copy.copy(default_sifi_state)
        expected_node_state['pa'] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state[self.other] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature('pa', enable=False, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    @unittest.skip('Known issue')
    def test_bug(self):
        self.host.enable_feature('pa', node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state = copy.copy(default_sifi_state)
        expected_node_state['pa'] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other, node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state[self.other] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, default_sifi_state)

    @unittest.skip('Known issue')
    def test_bug_2(self):
        self.host.enable_feature('pa', node=True)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        expected_node_state = copy.copy(default_sifi_state)
        expected_node_state['pa'] = True
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        self.host.enable_feature(self.other)
        sleep(SLEEP_TIME)
        cluster_state = self.host.get_cluster_state()
        node_state = self.host.get_node_state()
        self.assertEqual(cluster_state, default_sifi_state)
        self.assertEqual(node_state, expected_node_state)
        # Note, this test is incomplete


if __name__ == '__main__':
    unittest.main()
