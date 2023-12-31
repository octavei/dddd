# from db import haha
import time

from db.base1 import DBInterface
from transfer.transfer_txs_crawler import Crawler


if __name__ == "__main__":
    # db_interface = DBInterface(host=os.getenv("HOST"),
    #                            user=os.getenv("USER"),
    #                            pwd=os.getenv("PASSWORD"),
    #                            dbname=os.getenv("DATABASE"),
    #                            pool_size=int(os.getenv("POOL_SIZE")),
    #                            )

    # db_interface.insert_transaction_dota([
    #     ["c7CYan1Gx3DF3vGR3tiggiLGvj2G6L25jdiBArcxruugMUU", "DOTA", 0, "0xee8c74ffb60cdfc47cd72a73b1b3c3079c513e9c201c20ba2d6157bd824104b7", 1000, 1050, 50, "c7CYan1Gx3DF3vGR3tiggiLGvj2G6L25jdiBArcxruugMUU", "c7CYan1Gx3DF3vGR3tiggiLGvj2G6L25jdiBArcxruugMUU", 5],
    #     ["d2", "1", 0, "txhash1", 1000, 1050, 50, "d1", "d2", 0.5],
    #     ["d3", "2", 0, "txhash1", 1000, 1050, 50, "d1", "d2", 0.5],
    # ])
    # b = db_interface.get_balance(
    #     ["d0", "d3"],
    #     0
    # )

    # db_interface.update_balance([[
    #     "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
    #     100000000,
    #     0
    # ], [
    #     "5CiPPseXPECbkjWCa6MnjNokrgYjMqmKndv2rSnekmSK2DjL",
    #     100000000,
    #     0
    # ]], "DOTA")
    # db_interface.delete_transaction_dota()
    # db_interface.commit_batch_update_insert()
    txs = []
    print([i[0] for i in txs])
    print(int(time.time()))