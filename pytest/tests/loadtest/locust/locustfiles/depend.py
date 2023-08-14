"""
A workload with Fungible Token operations.
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import task
from common.base import NearUser
from common.depend import DependencyBomb

logger = new_logger(level=logging.WARN)
rng = random.Random()


class DependencyBomber(NearUser):

    @task
    def bomb(self):
        tx = DependencyBomb(self.account, self.contract)
        self.send_tx(tx, locust_name="dependency bomb")

    def on_start(self):
        super().on_start()
        self.contract = random.choice(self.environment.dependency_contract_accounts)
        