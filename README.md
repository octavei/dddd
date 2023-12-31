
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
# 数据库配置
export HOST="xxx"
export USER="xxx"
export PASSWORD="xxx#"
export DATABASE="xxx"
export POOL_SIZE=5

# 节点连接的地址
export URLS="xxx"

# log配置
# 多少天做一次备份
export ROTATION=1
# 最多保留多少周的数据
export RENTENTION=4
```
# 运行
```angular2html
python main.py
```
