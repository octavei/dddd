import time

from transfer.transfer_txs_crawler import Crawler
from transfer.common import connect_substrate, total_amount
from db.base1 import DBInterface
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException
from substrateinterface.exceptions import SubstrateRequestException
from logging import Logger


def get_user_total_amt(balance: dict) -> int:
    return balance["available"] + balance["hold"]


class TransferChecker():
    def __init__(self, start_block: int, crawler: Crawler, logger: Logger, mysql_db: DBInterface):
        self.start_block = start_block
        self.tick = "DOTA"
        self.mysql_db = mysql_db
        self.crawler = crawler
        self.logger = logger

    def update_users_balance(self, txs: list, block_num: int) -> bool:

        if len(txs) == 0:
            batch_name = "checker_block_height"
            self.mysql_db.update_or_insert_checker_block_height(batch_name, self.tick, block_num)
            if self.mysql_db.commit_batch_update_insert(batch_name):
                self.logger.info(f"#{self.start_block} checker向数据库写入数据(checker_block_height)成功")
            else:
                self.logger.error(f"#{self.start_block} checker向数据库写入数据(checker_block_height)失败!!!")
                return False

        users = []
        for tx in txs:
            users.extend([tx["from"], tx["to"]])
        users = list(set(users))
        self.logger.debug(f"users: {users}")
        users_balance = self.crawler.get_users_balances_from_mysql(tick=self.tick, users=users)
        self.logger.debug(f"从数据库获得用户资产: {users_balance}")
        before_total = total_amount(users_balance)
        self.logger.debug(f"本次更新设计到的用户总资产: {before_total}")

        users_bill = []
        for index, tx in enumerate(txs):
            self.logger.debug(f"正在处理交易: {tx}")
            if tx["status"] == 1:
                self.logger.error(f"已经处理过该笔交易: {tx}")
                return False
            if tx["status"] == 0:
                from_ = tx["from"]
                to = tx["to"]
                amt = tx["amt"]
                self.logger.debug(f"{from_} 转账到 {to}, 金额: {amt}")
                from_balance = users_balance[from_]
                to_balance = users_balance[to]
                self.logger.debug(f"from地址转账前金额 {from_balance}， to地址转账前金额: {to_balance}")
                from_hold = from_balance["hold"] - amt
                to_available = to_balance["available"] + amt
                if from_hold < 0:
                    self.logger.error(f"from地址锁仓金额不足于扣除")
                    return False
                from_balance["hold"] = from_hold
                to_balance["available"] = to_available
                self.logger.debug(f"from地址转账后金额 {from_balance}， to地址转账后金额: {to_balance}")
                users_balance[from_] = from_balance
                users_balance[to] = to_balance
                txs[index]["status"] = 1
                from_user_bill = [from_, from_, to, self.tick, 3, tx["tx_hash"], tx["block_num"], tx["extrinsic_index"], 0-amt,
                                  get_user_total_amt(from_balance) + amt, get_user_total_amt(from_balance), 0]
                to_user_bill = [to, from_, to, self.tick, 2, tx["tx_hash"], tx["block_num"], tx["extrinsic_index"], amt,
                                get_user_total_amt(to_balance) - amt, get_user_total_amt(to_balance), 0]

                users_bill.extend([from_user_bill, to_user_bill])

        after_total = total_amount(users_balance)
        if before_total != after_total:
            self.logger.error(f"入库金额对不上。 {before_total} - {after_total}")
            return False

        batch_name = "checker"
        self.mysql_db.update_or_insert_balance(batch_name, [[key, value["available"], value["hold"]] for key, value in users_balance.items()], self.tick)
        self.mysql_db.update_transaction_dota(batch_name, [[tx["tick"],tx["status"],tx["tx_hash"], tx["block_num"], tx["extrinsic_index"]] for tx in txs])
        self.mysql_db.insert_user_bill(batch_name, users_bill)
        self.mysql_db.update_or_insert_checker_block_height(batch_name, self.tick, block_num)

        if self.mysql_db.commit_batch_update_insert(batch_name):
            self.logger.info(f"#{block_num} checker向数据库写入数据(users_balance, txs_status, users_bill)成功")
        else:
            self.logger.error(f"#{block_num} checker向数据库写入数据(users_balance, txs_status, users_bill)失败")
            return False
        return True

    def is_txs_equal(self, mysql_txs: list, crawler_txs: list, now_block: int) -> bool:
        if len(mysql_txs) != len(crawler_txs):
            self.logger.error("交易数量不同。")
            return False
        for mysql_item, crawler_item in zip(mysql_txs, crawler_txs):
            self.logger.debug(f"正在处理交易: {mysql_item} {crawler_item}")
            if mysql_item["block_num"] != now_block or crawler_item["block_num"] != now_block:
                self.logger.error(f"区块高度错误{now_block} - {mysql_item} - {crawler_item}")
                return False
            if mysql_item["tx_hash"] != crawler_item["tx_hash"]:
                self.logger.error("交易hash不同")
                return False
            if mysql_item["extrinsic_index"] != crawler_item["extrinsic_index"]:
                self.logger.error("交易index不同")
                return False
            if mysql_item["from"] != crawler_item["from"]:
                self.logger.error("来源地址不同")
                return False
            if mysql_item["to"] != crawler_item["to"]:
                self.logger.error("目标地址不同")
                return False
            if mysql_item["tick"] != crawler_item["tick"]:
                self.logger.error("tick 不同")
                return False
            if mysql_item["amt"] != crawler_item["amt"]:
                return False
        return True

    def get_txs_from_mysql_by_block_num(self, num: int) -> list:
        txs = self.mysql_db.get_transaction_dota(num, self.tick)
        self.logger.debug(f"从数据库中获得区块高度为 #{num}的交易:{txs}")
        return [{"block_num": tx[0], "tx_hash": tx[1],
                                  "extrinsic_index": tx[2], "from": tx[3],
                                  "to": tx[4], "tick": tx[5], "amt": tx[6], "status": tx[7]} for tx in txs]

    def check_txs(self, crawler_block_height: int):
        if self.start_block + 6 <= crawler_block_height:
            range_ = range(int(self.start_block), int(crawler_block_height) - 6)
            for n in range_:
                self.logger.info(f"checker在检查区块高度#{n} 交易")
                # crawler去爬取区块数据
                crawler_txs = self.crawler.get_transfer_txs_by_block_num(n)
                # 从数据库中获取该区块的所有txs 然后核对与rpc爬取的是否相同
                mysql_txs = self.get_txs_from_mysql_by_block_num(n)

                if len(mysql_txs) != len(crawler_txs):
                    self.logger.error(f"区块高度#{n} 交易数量不一致")
                    exit(0)
                is_equal = self.is_txs_equal(mysql_txs, crawler_txs, n)
                if is_equal is False:
                    self.logger.error(f"区块高度#{n} 交易数据不一致，程序停止！")
                    exit(0)
                if self.update_users_balance(mysql_txs, n) is False:
                    exit(0)
                self.start_block += 1

        else:
            time.sleep(3)

    # def run(self):
    #     self.logger.info("checker启动")
    #     while True:
    #         try:
    #             try:
    #                 crawler_block_height = self.mysql_db.get_block_height(self.tick)[0][1]
    #                 print(self.start_block, crawler_block_height)
    #             except Exception as e:
    #                 print(f"crawler还没有设置高度。 err: {e}")
    #                 crawler_block_height = 0
    #                 time.sleep(12)
    #
    #             self.check_txs(self.start_block, crawler_block_height)
    #
    #         except (SubstrateRequestException, WebSocketConnectionClosedException, WebSocketTimeoutException) as e:
    #             self.crawler.substrate_client = connect_substrate()






