import time

from transfer.common import connect_substrate
from transfer.transfer_txs_crawler import Crawler
from transfer.transfer_checker import TransferChecker
import threading
from db.base1 import DBInterface, DBLog
from dotenv import load_dotenv
import os
from loguru import logger
import redis
import checkbot


def main(start_block: int):
    redis_host = os.getenv("REDIS_HOST")  # Redis 服务器地址
    redis_port = int(os.getenv("REDIS_PORT"))  # Redis 服务器端口
    redis_db = int(os.getenv("REDIS_DB"))  # Redis 数据库索引
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    # redis_client.flushdb()

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
    crawler = Crawler(db_interface, redis_client, logger, crawler_start_block_num + 1)
    checker = TransferChecker(checker_start_block_num + 1, crawler, logger, db_interface)
    t1 = threading.Thread(target=crawler.run, args=(checker.check_txs, ))
    t2 = threading.Thread(target=checkbot.while_check, args=(crawler, ))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # crawler.run(checker.check_txs)


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