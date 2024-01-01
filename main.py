from transfer.common import connect_substrate
from transfer.transfer_txs_crawler import Crawler
from transfer.transfer_checker import TransferChecker
import threading
from db.base1 import DBInterface, DBLog
from dotenv import load_dotenv
import os
from loguru import logger


def main(start_block: int):
    logger.add("file.log", level="INFO", rotation="{} day".format(os.getenv("ROTATION")), retention="{} weeks".format(os.getenv("RENTENTION")))
    db_interface = DBInterface(host=os.getenv("HOST"),
                               user=os.getenv("USER"),
                               pwd=os.getenv("PASSWORD"),
                               dbname=os.getenv("DATABASE"),
                               pool_size=int(os.getenv("POOL_SIZE")),
                               log_lv=DBLog.LV_VERBOSE)  # log_lv=DBLog.LV_VERBOSE)
    try:
        blocks = db_interface.get_block_height("DOTA")[0]

        checker_start_block_num, crawler_start_block_num = blocks[0], blocks[1]
    except Exception as e:
        checker_start_block_num = start_block
        crawler_start_block_num = start_block
        print(f"checker和crawler都还没有设置高度, 将使用默认值 {start_block}。 err: {e}")
    print(f"checker_start_block_num: {checker_start_block_num}, crawler_start_block_num: {crawler_start_block_num}")
    crawler = Crawler(db_interface, logger, crawler_start_block_num + 1)
    checker = TransferChecker(checker_start_block_num + 1, crawler, logger, db_interface)
    crawler.run(checker.check_txs)


if __name__ == "__main__":
    # db_interface = DBInterface(host=os.getenv("HOST"),
    #                            user=os.getenv("USER"),
    #                            pwd=os.getenv("PASSWORD"),
    #                            dbname=os.getenv("DATABASE"),
    #                            pool_size=int(os.getenv("POOL_SIZE")),
    #                            log_lv=DBLog.LV_VERBOSE)  # log_lv=DBLog.LV_VERBOSE)
    # print(db_interface.get_transaction_dota(327, "DOTA"))
    # # print(db_interface.get_block_height("DOTA"))
    # batch_name = "babab"
    # db_interface.delete_tables(batch_name)
    # db_interface.update_or_insert_balance(
    #     [
    #         [
    #             "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
    #             100000000,
    #             0
    #
    #     ],
    #         [
    #             "5CoqNFTYZtVYcx3hPdjTTvczUiXUBYtgskwrzz6zdeSNFCKo",
    #             200000000,
    #             0
    #         ],
    #         [
    #             "5FKmjePh2YT58zzKbuNJEM1vXe7uY3HuCugpNVmt1EWJEv7S",
    #             200000000,
    #             0
    #         ],
    #
    #     ],
    #     "DOTA"
    # )
    # db_interface.update_or_insert_checker_block_height("DOTA", 59140)
    # db_interface.update_or_insert_crawler_block_height("DOTA", 59140)
    # # db_interface.update_or_insert_checker_crawler_block_height("DOTA",399,999);
    # db_interface.commit_batch_update_insert(batch_name)
    load_dotenv()
    main(int(os.getenv("TRANSFER_START_BLOCK")))