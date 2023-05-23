"""
TODO
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))

import account
import cluster
import common
import key
from configured_logger import new_logger
from locust import between, task, events
from common.base import Account, CreateSubAccount, Deploy, NearUser, send_transaction
from common.ft import InitFT, InitFTAccount, TransferFT
from common.social import Follow, InitSocialDB, InitSocialDbAccount

logger = new_logger(level=logging.WARN)

FT_ACCOUNT = None
SOCIAL_DB_ACCOUNT = None


class FtTransferUser(NearUser):
    """
    Registers itself on an FT contract in the setup phase, then just sends FTs to
    random users.
    """
    wait_time = between(1, 3)  # random pause between transactions
    registered_users = []

    @task
    def ft_transfer(self):
        logger.debug(f"START FT TRANSFER {self.id}")
        rng = random.Random()

        receiver = rng.choice(FtTransferUser.registered_users)
        # Sender must be != receiver but maybe there is no other registered user
        # yet, so we just send to the contract account which is registered
        # implicitly from the start
        if receiver == self.account_id:
            receiver = self.contract_account.key.account_id

        self.send_tx(
            TransferFT(self.contract_account,
                       self.account,
                       receiver,
                       how_much=1))
        logger.debug(f"FT TRANSFER {self.id} DONE")

    def on_start(self):
        super().on_start()

        self.contract_account = FT_ACCOUNT

        logger.debug(f"starting user {self.id} init")
        self.send_tx(InitFTAccount(self.contract_account, self.account))
        logger.debug(f"user {self.account_id} InitFTAccount done")
        self.send_tx(
            TransferFT(self.contract_account,
                       self.contract_account,
                       self.account_id,
                       how_much=1E8))
        logger.debug(f"user {self.account_id} TransferFT done, user ready")

        FtTransferUser.registered_users.append(self.account_id)


class SocialDbUser(NearUser):
    """
    Registers itself on near.social in the setup phase, then starts posting,
    following, and liking posts.
    """
    wait_time = between(1, 3)  # random pause between transactions
    registered_users = []

    @task
    def follow(self):
        rng = random.Random()
        users_to_follow = [rng.choice(SocialDbUser.registered_users)]
        self.send_tx(
            Follow(self.contract_account, self.account, users_to_follow))

    # @task
    # def post(self):
    #     #TODO:
    #     post = lorem_ipsum()
    #     send(tryx(post))

    # @task
    # def like(self):
    #     #TODO:
    #     post_id = posts_pool.random_with_pareto_ditro()
    #     send(tx(post))

    def on_start(self):
        super().on_start()
        self.contract_account = SOCIAL_DB_ACCOUNT
        
        self.send_tx(InitSocialDbAccount(self.contract_account, self.account))
        logger.debug(f"user {self.account_id} ready to use SocialDB on {self.contract_account.key.account_id}")
        
        SocialDbUser.registered_users.append(self.account_id)


# called once per process before user initialization
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    funding_account = NearUser.funding_account
    ft_contract_code = environment.parsed_options.fungible_token_wasm
    social_contract_code = environment.parsed_options.social_db_wasm

    # TODO: more than one FT contract
    contract_key = key.Key.from_random(f"ft.{funding_account.key.account_id}")
    ft_account = Account(contract_key)
    global FT_ACCOUNT
    FT_ACCOUNT = ft_account
    
    contract_key = key.Key.from_random(f"social.{funding_account.key.account_id}")
    social_account = Account(contract_key)
    global SOCIAL_DB_ACCOUNT
    SOCIAL_DB_ACCOUNT = social_account
    

    # Note: These setup requests are not tracked by locust because we use our own http session
    host, port = environment.host.split(":")
    node = cluster.RpcNode(host, port)
    send_transaction(
        node, CreateSubAccount(funding_account, ft_account.key,
                               balance=50000.0))
    ft_account.refresh_nonce(node)
    send_transaction(node, Deploy(ft_account, ft_contract_code, "FT"))
    send_transaction(node, InitFT(ft_account))
    logger.info("FT account ready")

    send_transaction(
        node, CreateSubAccount(funding_account, social_account.key,
                               balance=50000.0))
    social_account.refresh_nonce(node)
    send_transaction(node, Deploy(social_account, social_contract_code, "Social"))
    send_transaction(node, InitSocialDB(social_account))
    logger.info("Social DB account ready")


# Add custom CLI args here, will be available in `environment.parsed_options`
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--fungible-token-wasm",
                        type=str,
                        required=True,
                        help="Path to the compiled Fungible Token contract")
    parser.add_argument("--social-db-wasm",
                        type=str,
                        required=True,
                        help="Path to the compiled SocialDB contract, get it from https://github.com/NearSocial/social-db/tree/aa7fafaac92a7dd267993d6c210246420a561370/res")
