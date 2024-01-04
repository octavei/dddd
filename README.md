
# 安装redis
```angular2html
sudo apt install redis-server
sudo systemctl start redis-server
```
# 克隆项目
```
git clone https://github.com/octavei/dddd.git
```
# 创建python虚拟环境并激活
```angular2html
python3 -m venv myenv
source myenv/bin/activate
```
# 安装依赖
```angular2html
pip install -r requirements.txt
```

# 设置环境变量
```bash
export HOST="rm-3ns3253p1640igl8r9o.mysql.rds.aliyuncs.com"
export USER="dota20_test"
export PASSWORD="XEKnyUd2NerLHTs#"
export DATABASE="dota20_test"
export POOL_SIZE=5

# redis
export REDIS_HOST="localhost"
export REDIS_PORT=6379
export REDIS_DB=0

# 连接的网络名称 波卡主网是Polkadot 测试网是Development
export CHAIN="Polkadot"
# 节点连接的地址
export URLS="wss://polkadot-rpc.dwellir.com"

# log配置
# 多少天做一次备份
export ROTATION=1
# 最多保留多少周的数据
export RENTENTION=4

# 开始接受转账的区块
export TRANSFER_START_BLOCK=18884000
```
# 运行
```angular2html
python main.py
```

# 启动api
```angular2html
python run_api.py
```

# 启动checkbot
```angular2html
python checkbot.py
```