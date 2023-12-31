# -*- coding: utf-8 -*-

from flask import Flask,request,jsonify,redirect,Response
import pymongo
import os,time,json,datetime
from LidamaoToolkit import json_require,create_token,safe_dict,err,insert_cover

client = pymongo.MongoClient("mongodb://127.0.0.1/", 27017)

block_db = client.dota.blocks
tick_db = client.dota.ticks
user_db = client.dota.users
mint_db = client.dota.mints 
transfer_db = client.dota.transfers
tx_amount_db = client.dota.tx_amount


tick_safe_list = ['tick','supply','p','op',"deployer","deploy_number",'total_supply','start_block']
mint_safe_list = ['tick',"address_to",'address_from','amount',"ts"]
tx_safe_list = ["address_from","address_to",'tick','amount','ts']

app = Flask(__name__)


##TODO,查询token_list_status
##deploy_number,deploy_height; deployer,deploy_address;market_supply,total_supply ;"op",tick_memo; start_block,start_height;supply,completed_height-start_height ;
##tick,tick;total_supply,total_supply;
##holders的话，需要拿tick，去user_balance表里，查总数，tick假如有1000条记录，那么holders=1000
###{"code":0,"ticks":[{"deploy_number":18681973,"deployer":"14drmsz2ixltvqt69uvk9mmefun4oqeptytnm44wheecaeou","holders":26501,"market_supply":209927114779,"op":"deploy","p":"dot-20","start_block":18681993,"supply":5000000,"tick":"DOTA","total_supply":210000000000}],"total":1}
##
@app.route("/v1/get_tick_list",methods=["GET"])
def get_tick_list():
    page = int(request.args.get('page_index',1))
    page_size = int(request.args.get("page_size",20))
    keyword = request.args.get("keyword")
    f = {}
    if keyword:
        f['tick'] = keyword
    ticks = list(tick_db.find(f).limit(page_size).skip((page-1)*page_size))
    ticks = [safe_dict(x,tick_safe_list) for x in ticks]
    tick_name_list = [x['tick'] for x in ticks]
    count = tick_db.count_documents({})
    for tick in ticks:
        supply = 0
        users = list(user_db.find({'tick':tick['tick'],'balance':{"$gt":0}}))
        for user in users:
            supply+=user['balance']
        tick['market_supply'] = supply
        tick['holders'] = len(users)

    
    return jsonify({"code":0,"ticks":ticks,'total':count})


## 查询user_currency_balance , tick不传的就默认dota,返回available就可以，扩展一下吧，返回值里面有available和hold，
##{"balance_list":{"DOTA":53801467},"code":0}
@app.route("/v1/get_balance_list",methods=["GET","POST"])
@json_require({'address':"str",'tick':'DOTA'},'GET')
def get_balance_list(info):
    addr = request.args['address'].strip().lower()
    ticks = list(tick_db.find({}))
    tick_name_list = [x['tick'] for x in ticks]

    users = list(user_db.find({"address":addr,'tick':{"$in":tick_name_list}}))
    user_balance_list = {x['tick']:x['balance'] for x in users}
    return jsonify({"code":0,'balance_list':user_balance_list})


##前台查询转账有没有成功，tx_hash,后台查一下最新的这一条tx_hash, 返回status，前台拿status 0 ，待确认，1交易成功，失败：9
##查询transation_dota那张表就可以
@app.route("/v1/get_trsanction_status",methods=["GET","POST"])
@json_require({'tx_hash':"str"},'GET')
def get_balance_list(info):
    addr = request.args['address'].strip().lower()
    ticks = list(tick_db.find({}))
    tick_name_list = [x['tick'] for x in ticks]

    users = list(user_db.find({"address":addr,'tick':{"$in":tick_name_list}}))
    user_balance_list = {x['tick']:x['balance'] for x in users}
    return jsonify({"code":0,'balance_list':user_balance_list})




## 废弃



@app.route("/v1/get_transaction_amount", methods=["GET"])
def get_transaction_amount():
    tx_list = tx_amount_db.find({}).sort("block_height", -1).limit(10)
    tx_list = [{"block_height": tx['block_height'], "dota_amount": tx['memo_amount'], "tx_amount": tx['tx_amount']} for
               tx in tx_list]

    return jsonify({"code": 0, "tx_list": tx_list})

@app.route("/v1/search",methods=["GET","POST"])
@json_require({'tick':"str"},'GET')
def search(info):
    ticks = tick_db.find({'tick':info['tick']})
    ticks = [safe_dict(x,tick_safe_list) for x in ticks]
    
    return jsonify({"code":0,'ticks':ticks})



@app.route("/v1/get_balance",methods=["GET"])
@json_require({"address":"str",'tick':"str"},'GET')
def get_balance(info):
    address = info['address'].lower()
    user = user_db.find_one({'tick':tick['tick'],'address':address})
    balance = user['balance'] if user else 0
    
    return jsonify({"code":0,"balance":balance})

@app.route("/v1/latest_mint",methods=["GET","POST"])
@json_require({'tick':"str"},'GET')
def latest_mint(info):
    tick_name = info['tick']
    mints = list(mint_db.find({'tick':tick_name}))
    mints = [safe_dict(mint,mint_safe_list) for mint in mints]
    
    return jsonify({"code":0,"mints":mints})

@app.route("/v1/latest_transfer",methods=["GET","POST"])
@json_require({'tick':"str"},'GET')
def latest_transfer(info):
    tick_name = info['tick']
    transfers = list(transfer_db.find({'tick':tick_name})) 
    transfers = [safe_dict(tx,tx_safe_list) for tx in transfers]
    
    return jsonify({"code":0,'txs':transfers})






if __name__ == "__main__":
    app.run(host="0.0.0.0",port=6666)
