
import time
from transfer.common import connect_substrate, sorted_txs, total_amount
import json
import asyncio
from substrateinterface import SubstrateInterface, Keypair, ExtrinsicReceipt
from substrateinterface.exceptions import SubstrateRequestException
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException
from scalecodec.types import GenericExtrinsic
from db.base1 import DBInterface
from logging import Logger
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED, FIRST_COMPLETED


class Crawler:
    def __init__(self,  mysql_db: DBInterface, redis_db, logger: Logger, start_block: int = 0):
        self.substrate_client = connect_substrate()
        self.tick = "DOTA"
        self.start_block = start_block
        self.mysql_db = mysql_db
        self.ask_for_stopping = False
        self.is_stop = False
        self.redis_db = redis_db
        self.logger = logger

    def get_users_balances_from_mysql(self, tick: str, users: list) -> dict:
        if len(users) == 0:
            return dict()
        users_balance = self.mysql_db.get_balance(users, tick)
        self.logger.debug(f"从数据库中获得用户{users} 资产: {users_balance}")
        users_balance_dict = {user_balance[0]: {"available": user_balance[1], "hold": user_balance[2]} for user_balance in users_balance}
        for user in users:
            if users_balance_dict.get(user) is None:
                users_balance_dict[user] = {"available": 0, "hold": 0}
        self.logger.debug(f"处理后的用户资产: {users_balance_dict}")
        return users_balance_dict

    # 检查合法transfer交易，并返回
    def get_transfer_txs_with_vail_memo(self, generic_extrinsics: list, block_num: int, block_hash: str, extrinsic_index = None) -> list:
        self.logger.info(f"正在爬取区块#{block_num} 的交易")
        if extrinsic_index is not None:
            try:
                generic_extrinsics = [generic_extrinsics[extrinsic_index]]
            except Exception as e:
                self.logger.warning(f"根据extrinsic_index去获取单一交易失败: {e}")
                return []
        vail_txs = []
        for index, tx in enumerate(generic_extrinsics):
            if extrinsic_index is not None:
                index = extrinsic_index
            # 判断是否是正确的交易格式
            if isinstance(tx, GenericExtrinsic) is False:
                self.logger.error(f"区块#{block_num} 数据错误，程序停止。")
                exit(0)
            try:
                if tx.value.get("call").get("call_function") == "batch_all" and len(
                        tx.value.get("call").get("call_args")[0].get("value")) == 2:
                    if tx.value.get("call")["call_args"][0]["value"][0]['call_function'] == "transfer_keep_alive" and \
                            tx.value.get("call")["call_args"][0]["value"][1]['call_function'] == "remark_with_event":

                        try:
                            remark = tx.value.get("call")["call_args"][0]["value"][1]['call_args'][0]["value"]
                            memo = json.loads(remark)
                        except Exception as e:
                            self.logger.warning(f"memo非json格式: {remark}, err: {e}")
                            continue
                        try:
                            tick = str(memo.get("tick")).strip().upper()
                            amt = int(memo.get("amt"))
                            # {"p": "dot-20", "op": "transfer", "tick": "DOTA", "amt": 1000000}
                            self.logger.info(f"#{block_num}, memo: {memo}")
                            if str(memo.get("p")).strip() == "dot-20" and str(memo.get("op")).strip() == "transfer" and tick == self.tick and amt > 0:
                                to = tx.value.get("call")["call_args"][0]["value"][0]['call_args'][0]["value"]
                                # 判断地址格式是否合法
                                if tx.value.get("call")["call_args"][0]["value"][0]['call_args'][0]["type"] != "AccountIdLookupOf":
                                    self.logger.warning(f"非法地址格式: {to}")
                                    continue
                                from_ = tx.value.get("address")
                                # 不能自己转给自己
                                if from_ == to:
                                    self.logger.warning(f"{from_} 转账给自己")
                                    continue
                                tx_hash = tx.value['extrinsic_hash']

                                try:
                                    receipt = self.get_tx_receipt(tx_hash, block_hash, block_num, index, True)
                                except (SubstrateRequestException, WebSocketConnectionClosedException, WebSocketTimeoutException) as e:
                                    raise e

                                if receipt.is_success:
                                    tx_js = {"block_num": block_num, "tx_hash": tx_hash,
                                             "extrinsic_index": index, "from": from_, "to": to, "tick": tick,
                                             "amt": amt, "status": 0}
                                    vail_txs.append(tx_js)
                                    self.logger.debug(f"#{block_num} 交易: {tx_js} 成功")

                                else:
                                    self.logger.debug(f"#{block_num} 交易: {tx_hash} 失败")

                        except Exception as e:
                            self.logger.warning(f"非法memo {memo}, err: {e}")
                            continue
            except Exception as e:
                self.logger.error(f"未知错误: {e}")
                exit(0)
        print(json.dumps(vail_txs, indent=2))

        return vail_txs

    async def async_get_transfer_txs_by_block_num(self, block_num):
        loop = asyncio.get_event_loop()
        # 将同步操作委托给线程池中的线程
        result = await loop.run_in_executor(None, self.get_transfer_txs_by_block_num, block_num)
        return result

    def get_transfer_txs_by_block_num(self, block_num, extrinsic_index=None):
        redis_result = self.redis_db.get(str(block_num).strip())
        if redis_result:
            self.logger.info(f"在redis中直接获取交易: {redis_result}")
            res = json.loads(redis_result)
            if len(res) > 0:
                if int(res[0]["block_num"]) != block_num:
                    self.logger.error(f"redis获取数据错误 {block_num} - {res}")
                    exit(0)
            if extrinsic_index:
                res = list(filter(lambda x: int(x["extrinsic_index"]) == extrinsic_index, res))
                if len(res) > 1:
                    self.logger.error("有重复交易索引， 请检查redis原数据")
                    exit(0)
            return res
        try:
            block_hash = self.substrate_client.get_block_hash(block_num)
            txs = self.substrate_client.get_extrinsics(block_hash=block_hash)
            vail_txs = self.get_transfer_txs_with_vail_memo(txs, block_num=block_num, block_hash=block_hash, extrinsic_index=extrinsic_index)
            self.logger.debug(f"高度#{block_num} 获得合法交易 {vail_txs}")
            return vail_txs
        except (SubstrateRequestException, WebSocketConnectionClosedException, WebSocketTimeoutException) as e:
            raise e

    def get_tx_receipt(self, extrinsic_hash, block_hash, block_number, extrinsic_idx, finalized):
        return ExtrinsicReceipt(self.substrate_client, extrinsic_hash=extrinsic_hash,
                                block_hash=block_hash,
                                                       block_number=block_number,
                                                       extrinsic_idx=extrinsic_idx, finalized=finalized)

    # async def async_insert_txs_into_redis(self, start: int, end: int):
    #     self.logger.info(f"区块高度差距大， 直接先爬到redis中. {start} - {end}")
    #     v = []
    #     for i in range(start, end):
    #         if self.redis_db.get(str(i).strip()) is None:
    #             v.append(i)
    #         else:
    #             self.logger.debug(f"区块#{i}已经有数据在redis中")
    #     if len(v) == 0:
    #         return
    #     tasks = []
    #
    #     try:
    #         for i in v:
    #             task = asyncio.Task(self.async_get_transfer_txs_by_block_num(i), name=str(i))
    #             tasks.append(task)
    #         try:
    #             completed_tasks, _ = await asyncio.wait_for(tasks, timeout=60)
    #         except asyncio.TimeoutError as e:
    #             self.logger.warning(f"线程等待超时: {e}")
    #             await self.async_insert_txs_into_redis(
    #                 start, end
    #             )
    #         # 处理已完成的任务
    #         for task in completed_tasks:
    #             try:
    #                 vail_txs = task.result()  # 获取任务的结果
    #                 task_name = f"{task.get_name()}"  # 修改为任务名的获取方式
    #                 self.redis_db.set(str(task_name).strip(), json.dumps(vail_txs))
    #                 self.logger.debug(f"#{str(task_name).strip()} 入库")
    #             except Exception as e:
    #                 print(f"Task encountered an error: {e}")
    #     except Exception as e:
    #         await self.async_insert_txs_into_redis(
    #             start, end
    #         )

    def insert_txs_into_redis(self, start: int, end: int, executor: ThreadPoolExecutor):
        self.logger.info(f"区块高度差距大， 直接先爬到redis中. {start} - {end}")
        v = []
        for i in range(start, end):
            if self.redis_db.get(str(i).strip()) is None:
                v.append(i)
            else:
                self.logger.debug(f"区块#{i}已经有数据在redis中")
        if len(v) == 0:
            return
        try:
            # with ThreadPoolExecutor(max_workers=50) as executor:
            # 提交任务到线程池
            future_to_task = {
                executor.submit(self.get_transfer_txs_by_block_num, i): f"{i}"  for i in
                              v}
            # 设置超时时间为60秒
            timeout = 20

            # 等待所有任务完成，设置超时时间
            completed, not_completed = wait(
                future_to_task, timeout=timeout, return_when=ALL_COMPLETED
            )

            # 等待所有任务完成
            for future in completed:
                task_name = future_to_task[future]
                try:
                    vail_txs = future.result()  # 获取任务的结果
                    self.redis_db.set(str(task_name).strip(), json.dumps(vail_txs))
                    self.logger.debug(f"区块#{str(task_name).strip()} 入库redis")
                except Exception as e:
                    self.logger.debug(f"区块 #{task_name} 请求未超时， 但是出现了错误: {e}")
            # 理未完成的任务（超时的任务）
            for future in not_completed:
                task_name = future_to_task[future]
                self.logger.warning(f" 区块#{task_name} 数据请求超时， 未能成功入库redis")
            # executor.shutdown(wait=False)
            self.logger.info("insert_txs_into_redis 函数执行完毕")
                # executor.shutdown(wait=False)

        except Exception as e:
            self.logger.error(f"insert_txs_into_redis 函数执行发生错误: {e}")
            raise e
            # self.insert_txs_into_redis(start, end)
        self.logger.debug("end")

    def insert_txs_into_mysql(self, txs: list, block_num: int) -> bool:
        if len(txs) == 0:
            batch_name = "crawler_block_height"
            self.mysql_db.update_or_insert_crawler_block_height(batch_name, self.tick, block_num)
            if self.mysql_db.commit_batch_update_insert(batch_name):
                self.logger.info(f"高度#{self.start_block} crawler向数据库写入数据（crawler_block_height）成功")
            else:
                self.logger.error(f"#{self.start_block} crawler向数据库写入数据(crawler_block_height)失败")
                return False
            return True

        users = list(set([tx["from"] for tx in txs]))
        users_balance = self.get_users_balances_from_mysql(tick=self.tick, users=users)
        before_total = total_amount(users_balance)
        # 如果数据库中还没有该地址 那么就赋与0
        for user in users:
            if users_balance.get(user) is None:
                users_balance[user] = {"available": 0, "hold": 0}

        for index, tx in enumerate(txs):
            from_ = tx["from"]
            from_balance = users_balance[from_]
            amt = tx["amt"]
            self.logger.info(f"{from_}将要转账，转账金额是{amt}. 现在账户金额是: {from_balance}")
            available_amt = from_balance["available"] - amt

            if available_amt < 0:
                self.logger.error("余额不足，转账失败。向数据库存入一条失败交易")
                txs[index]["status"] = 9 # 失败状态
            else:
                from_balance["hold"] = from_balance["hold"] + amt
                from_balance["available"] = available_amt
                self.logger.info(f"{from_}锁仓成功， 锁仓后账户金额: {from_balance}")
                users_balance[from_] = from_balance

        after_total = total_amount(users_balance)
        if after_total != before_total:
            self.logger.error(f"记账金额对不上. {before_total} - {after_total}")
            return False
        batch_name = "crawler"
        self.mysql_db.update_or_insert_balance(batch_name, [[key, value["available"], value["hold"]] for key, value in users_balance.items()] , self.tick)
        self.mysql_db.insert_transaction_dota(batch_name, [[tx["from"], tx["tick"], tx["status"], tx["tx_hash"], tx["block_num"],
                        tx["extrinsic_index"], tx["amt"], tx["from"], tx["to"], 0] for tx in txs])
        self.mysql_db.update_or_insert_crawler_block_height(batch_name, self.tick, block_num)

        if self.mysql_db.commit_batch_update_insert(batch_name):
            self.logger.info(f"#{self.start_block} crawler向数据库写入数据(users_balance, txs, crawler_block_height)成功")
        else:
            self.logger.error(f"#{self.start_block} crawler向数据库写入数据(users_balance, txs, crawler_block_height)失败")
            return False
        return True

    # todo 获取所有hold不为0的用户金额 然后把hold转到available中
    # todo 从确认块开始重新同步数据
            
    # 爬取数据的主函数 只要没有接受到结束指令，就会一直爬取
    def run(self, checker_check_func):
        self.logger.info("crawler启动")
        with ThreadPoolExecutor(max_workers=50) as executor:

            while True:
                try:
                    latest_block_hash = self.substrate_client.get_chain_finalised_head()
                    latest_block_num = self.substrate_client.get_block_number(latest_block_hash)
                    self.logger.debug(f"最新区块高度 #{latest_block_num}")
                    if self.ask_for_stopping is True:
                        self.is_stop = True
                    if self.is_stop:
                        self.logger.info("接受到结束指令， 现在结束!")
                        break
                    if self.start_block >= latest_block_num:
                        time.sleep(2)
                        continue
                    if self.start_block + 10 < latest_block_num:
                        end = self.start_block + 10
                    else:
                        end = latest_block_num
                    # if self.start_block + 3 <= end:
                    #     self.logger.debug(f"正在爬取高度 {self.start_block} - {end} 的数据到redis")
                    #     # loop = asyncio.get_event_loop()
                    #     # loop.run_until_complete(self.async_insert_txs_into_redis(self.start_block, end))
                    #     self.insert_txs_into_redis(self.start_block, end, executor)
                    #     self.logger.debug(f"入库redis结束 {self.start_block} - {end}")
                    self.logger.debug(f"正在把区块{self.start_block} - {end}")
                    for n in range(self.start_block, end):
                        self.logger.debug(f"正在处理区块#{n} 的交易数据到mysql中")
                        vail_txs = self.get_transfer_txs_by_block_num(n)
                        if self.insert_txs_into_mysql(vail_txs, n) is False:
                            self.logger.error("程序结束!")
                            exit(0)
                        checker_check_func(n)
                        self.start_block = n + 1

                except (SubstrateRequestException, WebSocketConnectionClosedException, WebSocketTimeoutException):
                    self.logger.warning("rpc连接失败，正在重连。。。")
                    try:
                        self.substrate_client = connect_substrate()
                    except Exception as e:
                        self.logger.warning(f"多次rpc重连后还是失败， 请检查您的rpc地址是否正确。err：{e}")
                    time.sleep(2)
                    self.run()



if __name__ == "__main__":
    pass

