# from db import haha
import time
from transfer.transfer_txs_crawler import Crawler
from transfer.transfer_checker import TransferChecker
from db.base1 import DBInterface, DBLog
from db.base1 import DBInterface
from transfer.transfer_txs_crawler import Crawler
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from loguru import logger
import os
from transfer.transfer_txs_crawler import *



if __name__ == "__main__":
    redis_client.set(str(1), "hahaha")
    if redis_client.get(str(1)) is None:
        print("kkkk")
    print(redis_client.get(str(1)))
    # start_block = 0
    # load_dotenv()
    # logger.add("file.log", level="INFO", rotation="{} day".format(os.getenv("ROTATION")),
    #            retention="{} weeks".format(os.getenv("RENTENTION")))
    # db_interface = DBInterface(host=os.getenv("HOST"),
    #                            user=os.getenv("USER"),
    #                            pwd=os.getenv("PASSWORD"),
    #                            dbname=os.getenv("DATABASE"),
    #                            pool_size=int(os.getenv("POOL_SIZE")),
    #                            log_lv=DBLog.LV_VERBOSE)  # log_lv=DBLog.LV_VERBOSE)
    # # try:
    # #     blocks = db_interface.get_block_height("DOTA")[0]
    # #
    # #     checker_start_block_num, crawler_start_block_num = blocks[0], blocks[1]
    # # except Exception as e:
    # checker_start_block_num = start_block
    # crawler_start_block_num = start_block
    #     # print(f"checker和crawler都还没有设置高度, 将使用默认值 {start_block}。 err: {e}")
    # print(f"checker_start_block_num: {checker_start_block_num}, crawler_start_block_num: {crawler_start_block_num}")
    # crawler = Crawler(db_interface, logger, crawler_start_block_num + 1)
    # checker = TransferChecker(checker_start_block_num + 1, crawler, logger, db_interface)
    # # crawler.run(checker.check_txs)
    # # 创建一个 ThreadPoolExecutor，最多同时运行两个线程
    # with ThreadPoolExecutor(max_workers=5) as executor:
    #     # 提交任务到线程池
    #     future_to_task = {executor.submit(crawler.get_transfer_txs_by_block_num, i): f"Task {i}" for i in range(300, 4000)}
    #
    #     # 等待所有任务完成
    #     for future in as_completed(future_to_task):
    #         task_name = future_to_task[future]
    #         try:
    #             future.result()  # 获取任务的结果
    #         except Exception as e:
    #             print(f"Task {task_name} encountered an error: {e}")
    #         else:
    #             print(f"Task {task_name} completed successfully")
    #
    # print("All tasks are done")