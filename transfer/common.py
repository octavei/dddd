import os

from substrateinterface import SubstrateInterface, Keypair, ExtrinsicReceipt
import time
import random
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
# from db.base1 import
load_dotenv()


def connect_substrate():
    urls = [url.strip() for url in os.getenv("URLS").split(",")]
    try:
        url = random.choice(urls)
        substrate = SubstrateInterface(
            url=url,
        )
        print("连接上节点: {}".format(url))
        print(f"chain: {substrate.chain}, format: {substrate.ss58_format}, token symbol: {substrate.token_symbol}")

        return substrate
    except ConnectionRefusedError:
        print("⚠️ No local Substrate node running, try running 'start_local_substrate_node.sh' first")
        time.sleep(6)
        return connect_substrate()


def sorted_txs(item: dict):
    return item["block_num"], item["extrinsic index"]


def total_amount(users_balance: dict) -> int:
    total = 0
    for v in users_balance.values():
        total += (v["available"] + v["hold"])
    return total
