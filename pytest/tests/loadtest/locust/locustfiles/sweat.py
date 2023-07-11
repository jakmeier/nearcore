"""
A workload with Sweat operations.

Sweat is a slightly modified version of the standard fungible token contract.
  - The lookup map is slightly modified to make storage keys shorter
  - There is a record_batch method which can update many users' balances at once
  - The "oracles" concept was added, a list of privileged accounts that can mint tokens

This workload is similar to the FT workload with 2 major differences:
  - Single account with larger state (larger state still TODO)
  - Periodic batches that adds steps (mints new tokens)
"""

from common.sweat import SweatMintBatch
from common.ft import TransferFT
from common.base import NearUser
from locust import between, tag, task
import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger

logger = new_logger(level=logging.WARN)


class SweatUser(NearUser):
    """
    Registers itself on an FT contract in the setup phase, then just sends Sweat to
    random users.

    Also includes a task to mint and distribute tokens in batches.
    """
    wait_time = between(1, 3)  # random pause between transactions

    @task(3)
    def ft_transfer(self):
        receiver = self.sweat.random_receiver(self.account_id)
        tx = TransferFT(self.sweat.account, self.account, receiver)
        self.send_tx(tx, locust_name="Sweat transfer")

    @task(1)
    def record_single_batch(self):
        rng = random.Random()
        batch_size = min(rng.randint(100, 150),
                         len(self.sweat.registered_users))
        receivers = self.sweat.random_receivers(self.account_id, batch_size)
        tx = SweatMintBatch(
            self.sweat.account.key.account_id, self.sweat.oracle,
            [[account_id, rng.randint(1000, 3000)] for account_id in receivers])
        self.send_tx(tx, locust_name="Sweat record batch")

    @tag("storage-stress-test")
    @task
    def record_batch_of_large_batches(self):
        # ensure large enough state by creating more sweat users
        while len(self.sweat.registered_users) < 1000:
            SweatUser(self.environment).on_start()

        rng = random.Random()
        # just around 300Tgas
        batch_size = rng.randint(700, 750)
        # just around the log limit
        # batch_size = rng.randint(150, 180)
        receivers = self.sweat.random_receivers(self.account_id, batch_size)
        tx = SweatMintBatch(
            self.sweat.account.key.account_id, self.sweat.oracle,
            [[account_id, rng.randint(1000, 3000)] for account_id in receivers])
        self.send_tx(tx, locust_name="Sweat record batch (stress test)")

    def on_start(self):
        super().on_start()
        self.sweat = self.environment.sweat
        self.sweat.register_user(self)
        logger.debug(
            f"{self.account_id} ready to use Sweat contract {self.sweat.account.key.account_id}"
        )
