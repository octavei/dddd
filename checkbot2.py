# pip3 install python-telegram-bot
import asyncio
import telegram
import time
import redis
import json, os
from transfer.common import connect_substrate
from loguru import logger
from db.base1 import DBInterface, DBLog
import subprocess
from transfer.transfer_txs_crawler import Crawler
from dotenv import load_dotenv
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException
from substrateinterface.exceptions import SubstrateRequestException

DEBUG_LOG = True
TOKEN = "6645928388:AAEdxZKLY16-WOlzSGlzNO1IyWqxiZXswjk"  # 电报Bot Token
#CHATID = -1002052818237  # 电报聊天ID
CHATID = -1002090185934

db_interface = DBInterface(
    host=os.getenv("HOST"),
    user=os.getenv("USER"),
    pwd=os.getenv("PASSWORD"),
    dbname=os.getenv("DATABASE"),
    pool_size=int(os.getenv("POOL_SIZE")),
    log_lv=DBLog.LV_VERBOSE)


async def send(TEXT):  # 电报推送TEXT到群内
    bot = telegram.Bot(TOKEN)
    async with bot:
        await bot.send_message(text=TEXT, chat_id=CHATID)


def kill():
    p = subprocess.Popen(["pgrep", "py"], stdout=subprocess.PIPE)
    output = p.communicate()
    s = output[0].decode().strip().split("\n")
    for i in s:
        subprocess.run(["kill", i])
        print("进程已终止")
    else:
        print("未找到对应进程")


def check_balance(now, crawler: Crawler):
    query = db_interface.drv.query_db(table_name="user_currency_balance",
                                      query_columns=["currency_tick", "available", "hold"],
                                      condition_columns=[],
                                      condition_values=[])
    rows = db_interface.drv.fetch_batch_query([query])
    result = []
    for row in rows:
        result.append([row[0], row[1], row[2]])  # Tick/Available/Hold
    summary = {}
    for item in result:
        key = item[0]
        value = item[1] + item[2]
        if key in summary:
            summary[key] += value
        else:
            summary[key] = value

    query2 = db_interface.drv.query_db(table_name="token_list_status",
                                       query_columns=["currency_tick", "circulating_supply"],
                                       condition_columns=[],
                                       condition_values=[])
    tokenlist = db_interface.drv.fetch_batch_query([query2])
    for row in tokenlist:
        if row[0] in summary:  # 判断user_currency_balance里是否有这个Tick
            if summary[row[0]] == row[1]:  # 比对各币种的user_currency_balance计算总和与tokenlist表的流通数量是否相同
                text = str(row[0]) + " checked✅ with total balance of " + str(int(row[1]))
                asyncio.run(send(text))
            else:
                asyncio.run(send(
                    "⚠️Error Ledger, Service Stopped!!\nSumBalance:" + str(int(summary[row[0]])) + "\nCirculating:" + str(
                        row[1])))
                kill()
                return False
        else:
            asyncio.run(send("⚠️Error Tick **" + str(row[0]) + "** Found, Service Stopped!!"))
            kill()
            return False

    query3 = db_interface.drv.query_db(table_name="user_bill",
                                       query_columns=["sum(amount)", "sum(before_balance)", "sum(after_balance)"],
                                       condition_columns=[],
                                       condition_values=[])
    sumbill = db_interface.drv.fetch_batch_query([query3])
    if sumbill[0][0] + sumbill[0][1] != sumbill[0][2]:
        asyncio.run(send(
            "⚠️Error SumBill Found, Service Stopped!!\nAmount:" + str(int(sumbill[0][0])) + "\nBeforeBalance: " + str(
                int(sumbill[0][1])) + "\nAfterBalance: " + str(int(sumbill[0][2]))))
        kill()
        return False

    query4 = db_interface.drv.query_db(table_name="user_bill",
                                       query_columns=["user_address", "currency_tick", "after_balance", "id",
                                                      "from_address", "to_address", "type", "tx_hash", "block_height",
                                                      "extrinsic_index", "amount", "before_balance"],
                                       condition_columns=[],
                                       condition_values=[],
                                       extra="where modify_time>=\"" + str(time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                         time.localtime(
                                                                                             now - 120)) + "\" order by id desc"))  # 获取最近2分钟内的bill，按id降序排序
    bill = db_interface.drv.fetch_batch_query([query4])
    for bill_item in bill:
        bill_item_json = {"user_address": bill_item[0], "currency_tick": bill_item[1], "after_balance": bill_item[2], "id": bill_item[3],
                          "from_address": bill_item[4], "to_address": bill_item[5], "type": bill_item[6], "tx_hash": bill_item[7],"block_height": bill_item[8],
                          "extrinsic_index": bill_item[9], "amount": bill_item[10], "before_balance": bill_item[11]}
        try:
            vail_txs = crawler.get_transfer_txs_by_block_num(block_num=bill_item_json["block_height"], extrinsic_index=bill_item_json["extrinsic_index"])
        except (SubstrateRequestException, WebSocketConnectionClosedException, WebSocketTimeoutException) as e:
            crawler.substrate_client = connect_substrate()
            vail_txs = crawler.get_transfer_txs_by_block_num(block_num=bill_item_json["block_height"],
                                                             extrinsic_index=bill_item_json["extrinsic_index"])

        if len(vail_txs) == 1:
            vail_tx = vail_txs[0]
            # 转出
            if int(bill_item_json["type"]) == 3:
                if bill_item_json["user_address"] != bill_item_json["from_address"]:
                    asyncio.run(send("Vail user address. address should be {} but {}".format(bill_item_json["from_address"], bill_item_json["user_address"])))
                    kill()
                    return False
                if int(bill_item_json["amount"]) != int(0-vail_tx["amt"]):
                    asyncio.run(send("Diff amount"))
                    kill()
                    return False
            # 转入
            elif int(bill_item_json["type"]) == 2:
                if bill_item_json["user_address"] != bill_item_json["to_address"]:
                    asyncio.run(send(
                        "Vail user address. address should be {} but {}".format(bill_item_json["to_address"],bill_item_json["user_address"])))
                    kill()
                    return False
                if int(bill_item_json["amount"]) != int(vail_tx["amt"]):
                    asyncio.run(send("Diff amount"))
                    kill()
                    return False
            else:
                asyncio.run(send("Not support type"))
                kill()
                return False

            if int(bill_item_json["before_balance"] + bill_item_json["amount"]) != int(bill_item_json["after_balance"]):
                asyncio.run(send("before_balance + amount != after_balance"))
                kill()
                return False
            if bill_item_json["from_address"] != vail_tx["from"] or bill_item_json["to_address"] != vail_tx["to"]:
                asyncio.run(send("Diff address"))
                kill()
                return False
            if bill_item_json["currency_tick"] != vail_tx["tick"]:
                asyncio.run(send("Diff tick"))
                kill()
                return False
            if vail_tx["tx_hash"] != bill_item_json["tx_hash"]:
                asyncio.run(send("Diff tx hash"))
                kill()
                return False
        else:
            asyncio.run(send("len(vail_txs) != 1"))
            kill()
            return False


    billist = {}
    for item in bill:
        key = item[:2]
        value = item[2:4]
        if key in billist:
            continue
        else:
            billist[key] = value  # 保留id最高的unique address+tick及对应值

    query5 = db_interface.drv.query_db(table_name="user_currency_balance",
                                       query_columns=["user_address", "currency_tick", "available", "hold"],
                                       condition_columns=[],
                                       condition_values=[])  # 获取用户余额表，核对summary
    balance = db_interface.drv.fetch_batch_query([query5])
    dict = {}
    for item in balance:
        key = item[:2]
        value = item[2:]
        dict[key] = value  # 建立address+tick唯一索引，核对balance

    for i in billist:  # 检查
        if dict[i][0] + dict[i][1] != billist[i][0]:
            asyncio.run(send("⚠️Error Single Bill Found!!\nAvailable: " + str(int(dict[i][0])) + "\nHold: " + str(
                int(dict[i][1])) + "\nBalance: " + str(int(billist[i][0])) + "\nUserBillId: " + str(
                billist[i][1])))  # 检查余额是否相同
            kill()
            return False

    # 规则四开始
    '''
    上面query4返回最近2分钟的bill

    格式为:
    "user_address","currency_tick","after_balance","bill_id","from_address","to_address","type","tx_hash","block_height","extrinsic_index","amount","before_balance"

    样本数据
    ('5CfpYxhcYJ9TE4DPUe2ok7StJLkrmDWhvaZYotCQDTZCGdyo', 'DOTA', Decimal('778525.000000'), 2491, '5GruTawFpEvGrCW33UnuKpYSoGRWfC8kJ4RHanxMoNEHE6rX', '5CfpYxhcYJ9TE4DPUe2ok7StJLkrmDWhvaZYotCQDTZCGdyo', 2, '0x87e47e944356d2af3d7bbcaabc000c5d1ca018884ceba52d72609b4cbdcc0504', 28542, 2, Decimal('15000.00000000'), Decimal('763525.000000'))
    '''

    # 规则四结束
    return True


def while_check(crawler: Crawler):
    while check_balance(time.time(), crawler):
        time.sleep(60)  # 每分钟一次检查


if __name__ == "__main__":
    load_dotenv()
    redis_host = os.getenv("REDIS_HOST")  # Redis 服务器地址
    redis_port = int(os.getenv("REDIS_PORT"))  # Redis 服务器端口
    redis_db = int(os.getenv("REDIS_DB"))  # Redis 数据库索引
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    # redis_client.flushdb()

    logger.add("file.log", level="INFO", rotation="{} day".format(os.getenv("ROTATION")),
               retention="{} weeks".format(os.getenv("RENTENTION")))
    crawler = Crawler(db_interface, redis_client, logger)
    while_check(crawler)





