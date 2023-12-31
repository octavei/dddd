# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, redirect, Response
import json
from db.base1 import DBInterface, DBLog

# client = pymongo.MongoClient("mongodb://127.0.0.1/", 27017)
#
# block_db = client.dota.blocks
# tick_db = client.dota.ticks
# user_db = client.dota.users
# mint_db = client.dota.mints
# transfer_db = client.dota.transfers
# tx_amount_db = client.dota.tx_amount


DEBUG_LOG = True

db_interface = DBInterface(host='rm-3ns3253p1640igl8r9o.mysql.rds.aliyuncs.com',
                               user='dota20_test',
                               pwd='XEKnyUd2NerLHTs#',
                               dbname='dota20_test',
                               pool_size=5,
                               log_lv=DBLog.LV_VERBOSE)

tick_safe_list = ['tick', 'supply', 'p', 'op', "deployer", "deploy_number", 'total_supply', 'start_block']
mint_safe_list = ['tick', "address_to", 'address_from', 'amount', "ts"]
tx_safe_list = ["address_from", "address_to", 'tick', 'amount', 'ts']


app = Flask(__name__)


##TODO,查询token_list_status
##deploy_number,deploy_height; deployer,deploy_address;market_supply,total_supply ;"op",tick_memo; start_block,start_height;supply,completed_height-start_height ;
##tick,tick;total_supply,total_supply;
##holders的话，需要拿tick，去user_balance表里，查总数，tick假如有1000条记录，那么holders=1000
###{"code":0,"ticks":[{"deploy_number":18681973,"deployer":"14drmsz2ixltvqt69uvk9mmefun4oqeptytnm44wheecaeou","holders":26501,"market_supply":209927114779,"op":"deploy","p":"dot-20","start_block":18681993,"supply":5000000,"tick":"DOTA","total_supply":210000000000}],"total":1}
##使用例子：http://192.168.2.104:1950/v1/get_tick_list
@app.route("/v1/get_tick_list", methods=["GET"])
def get_tick_list():
    query = db_interface.drv.query_db(table_name="token_list_status",
                                      query_columns=None,
                                      condition_columns=None,
                                      condition_values=None)
    tick_list_rows = db_interface.drv.fetch_batch_query([query])
    res_keys = ["deploy_number", "deployer", "market_supply", "tick_memo", "start_block", "tick", "total_supply"] # "holders", "supply"
    keys_map_rows = [10,         2,           5,              7,     3,            1,       5]
    tick_list = []
    for tick_row in tick_list_rows:
        json_tick = {}
        for kid, key in zip(keys_map_rows, res_keys):
            json_tick[key] = tick_row[kid]
        json_tick["supply"] = tick_row[4] - json_tick["start_block"]  # completed_height-start_height
        json_tick["tick_memo"] = json.loads("{" + str(json_tick["tick_memo"]) + "}")
        cur_tick = json_tick["tick"]
        query = db_interface.drv.query_db(table_name="user_currency_balance",
                                          query_columns=None,
                                          condition_columns=["currency_tick"],
                                          condition_values=[cur_tick])
        rows = db_interface.drv.fetch_batch_query([query])
        json_tick["holder"] = len(rows)
        tick_list.append(json_tick)
    if DEBUG_LOG:
        print("get_tick_list() tick_list_rows:", tick_list_rows)
    return jsonify({"ticks": tick_list, 'total': len(tick_list)})


## 查询user_currency_balance , tick不传的就默认dota,返回available就可以，扩展一下吧，返回值里面有available和hold，
##{"balance_list":{"DOTA":53801467},"hold":xxx,"code":0}
# 使用例子：http://192.168.2.104:1950/v1/get_balance_list?tick=DOTA
@app.route("/v1/get_balance_list", methods=["GET"])
def get_balance_list():
    tick = request.args['tick'].strip()
    if tick is None:
        tick = "DOTA"

    query = db_interface.drv.query_db(table_name="user_currency_balance",
                                      query_columns=["available", "hold"],
                                      condition_columns=["currency_tick"],
                                      condition_values=[tick])
    rows = db_interface.drv.fetch_batch_query([query])
    balance_list = []
    for row in rows:
        balance_list.append({"tick": tick, "available": row[0], "hold": row[1]})
    if DEBUG_LOG:
        print(f"balance_list:{balance_list}")
    return jsonify({'balance_list': balance_list})


##前台查询转账有没有成功，tx_hash,后台查一下最新的这一条tx_hash, 返回status，前台拿status 0 ，待确认，1交易成功，失败：9, 查不到数据：10000
##查询transation_dota那张表就可以
# 使用例子：http://192.168.2.104:1950/v1/get_trsanction_status?tx_hash=0x2115d16eef21dd6af7657c1b7fe49e198564befab35150793e11b1cc8dccb5e9
@app.route("/v1/get_trsanction_status", methods=["GET"])
def get_trsanction_status():
    tx_hash = request.args['tx_hash'].strip()
    query = db_interface.drv.query_db(table_name="transaction_dota",
                                      query_columns=["id", "status"],
                                      condition_columns=["tx_hash"],
                                      condition_values=[tx_hash])

    rows = db_interface.drv.fetch_batch_query([query])
    if rows is None or len(rows) <= 0:
        rows = [(0, 10000)]
    if DEBUG_LOG:
        print(f"/v1/get_trsanction_status get rows:{rows}")

    res_row = rows[0]
    for row in rows:
        if row[0] > res_row[0]:
            res_row = row

    return jsonify({"status": res_row[1]})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1950)

