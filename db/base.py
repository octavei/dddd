import os
from sqlalchemy import create_engine, Column, Integer, String, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv


# 加载环境变量
load_dotenv()
user = os.getenv("USER")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
database = os.getenv("DATABASE")
database_url = f"mysql+pymysql://{user}:{password}@{host}/{database}?charset=utf8mb4"

# 创建数据库引擎和会话
engine = create_engine(database_url, echo=True, pool_size=10, max_overflow=20, )  # 设置 echo=True 可以输出 SQL 语句
Base = declarative_base()
metadata = MetaData()

# 创建会话类
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

with Session() as session:
    # 在此块中执行数据库操作
    pass
# session.
# def print_url():
#     print(database_url)