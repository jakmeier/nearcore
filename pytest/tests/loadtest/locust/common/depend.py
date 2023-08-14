import random
import sys
import pathlib
import typing
from locust import events

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE


class DependencyBomb(FunctionCall):

    def __init__(self, sender: Account, contract: Account):
        super().__init__(sender, contract.key.account_id, "depend_a_lot")

    def args(self) -> dict:
        return {}


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    INIT_DONE.wait()
    node = NearNodeProxy(environment)
    code_path = "./res/depend.wasm"
    funding_account = NearUser.funding_account
    parent_id = funding_account.key.account_id

    funding_account.refresh_nonce(node.node)
    
    accounts = [
        Account(key.Key.from_random(environment.account_generator.random_account_id(parent_id, '_dep')))
        for _ in range(8)
    ]
    node.prepare_accounts(accounts, funding_account, 10, "prep dep acc")
    
    for account in accounts:
        node.send_tx_retry(Deploy(account, code_path, "Dependency"), "deploy dep")
    
    environment.dependency_contract_accounts = accounts
        
        
