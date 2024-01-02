from mysql.connector import pooling
import threading


# VERBOSE输出debug信息 LV_INFO输出操作信息,LV_ERROR只输出error,SILENCE日志沉默. 由高到低日志越来越少
class DBLog:
    LV_SILENCE = 0
    LV_ERROR = 1
    LV_INFO = 2
    LV_VERBOSE = 3

    __LV_DICT = ["DBIntf_SILENCE", "DBIntf_ERROR", "DBIntf_INFO", "DBIntf_VERBOSE"]
    CUR_LOG_LV = LV_SILENCE

    @staticmethod
    def log(log_lv, *msg):
        if log_lv <= DBLog.CUR_LOG_LV:
            print(DBLog.__LV_DICT[log_lv] + ":", *msg)


# 数据库原子事务增删改查操作
class DBInterfaceDriver:
    __VERSION = "1.5.3"

    # 初始化并连接数据库，连接失败抛出Exception需要处理
    def __init__(self, host, user, pwd, dbname, pool_size, log_lv=DBLog.LV_ERROR):
        DBLog.CUR_LOG_LV = log_lv
        DBLog.log(DBLog.LV_INFO, f"DBInterfaceDriver version:{DBInterfaceDriver.__VERSION}")

        self.host = host
        self.user = user
        self.pwd = pwd
        self.dbname = dbname
        self.pool_size = pool_size

        self.connection_pool = None
        self.__init_connection_pool()

        self.__batch_mapped_queue = {}
        self.__batch_mapped_queue_lock = threading.Lock()

    def __init_connection_pool(self):
        try:
            self.connection_pool = pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                               pool_size=self.pool_size,
                                                               pool_reset_session=True,
                                                               host=self.host,
                                                               database=self.dbname,
                                                               user=self.user,
                                                               password=self.pwd)

            # check pool connection
            connection_object = self.connection_pool.get_connection()
            if connection_object.is_connected():
                db_info = connection_object.get_server_info()
                DBLog.log(DBLog.LV_INFO, f"Connection Pool connected, MySQL Server version: ", db_info)

        except Exception as e:
            DBLog.log(DBLog.LV_ERROR, "Error while connecting to MySQL using Connection pool ", e)
            raise Exception("Connection Error while init DBInterface")
        finally:
            # return database connection.
            if self.connection_pool is not None and connection_object.is_connected():
                connection_object.close()
                DBLog.log(DBLog.LV_VERBOSE, "init check connection is returned")

    def __push_batch(self, batch_name, not_query_sql):
        self.__batch_mapped_queue_lock.acquire()
        batch = self.__batch_mapped_queue.get(batch_name)
        if batch is None:
            batch_op_queue = [not_query_sql]
            batch_lock = threading.Lock()
            self.__batch_mapped_queue[batch_name] = {"queue": batch_op_queue, "lock": batch_lock}
            self.__batch_mapped_queue_lock.release()
        else:
            self.__batch_mapped_queue_lock.release()
            batch_lock = batch["lock"]
            batch_op_queue = batch["queue"]
            batch_lock.acquire()
            batch_op_queue.append(not_query_sql)
            batch_lock.release()

    # 批量原子性操作, 某条失败回滚并返回False, 成功返回True
    def __batch_op(self, sql_op_list, querycb=None, commit=False):
        if sql_op_list is None or len(sql_op_list) <= 0:
            DBLog.log(DBLog.LV_ERROR, "batch_op() get zero size sql_op_list")
            return False
        ret = True
        try:
            # check connection
            connection_object = self.connection_pool.get_connection()
            if not connection_object.is_connected():
                DBLog.log(DBLog.LV_ERROR, "mysql connection failed in batch_op()")
                return False

            cursor = connection_object.cursor()

            if querycb is not None:
                res_rows = []
                DBLog.log(DBLog.LV_VERBOSE, f"batch_op() sql_query:{sql_op_list}")
                for sql_op in sql_op_list:
                    cursor.execute(sql_op)
                    rows = cursor.fetchall()
                    if rows is not None and len(rows) > 0:
                        res_rows.extend(rows)
                querycb(res_rows)
            else:
                # transactions begin
                connection_object.autocommit = False
                for sql_op in sql_op_list:
                    DBLog.log(DBLog.LV_VERBOSE, f"batch_op() sql_op:{sql_op}")
                    cursor.execute(sql_op)
                connection_object.commit()

        except Exception as e:
            DBLog.log(DBLog.LV_ERROR, f"Exception in batch_op() rolling back, error:", e, f" sql_op_list:{sql_op_list}")
            connection_object.rollback()
            ret = False
        finally:
            if connection_object.is_connected():
                cursor.close()
                connection_object.close()
                DBLog.log(DBLog.LV_VERBOSE, "connection is returned")

        DBLog.log(DBLog.LV_VERBOSE, f"batch_op successful:{ret}")
        return ret

    def commit_batch_update_insert(self, batch_name):
        self.__batch_mapped_queue_lock.acquire()
        batch = self.__batch_mapped_queue.get(batch_name)
        ret = True
        if batch is None:
            self.__batch_mapped_queue_lock.release()
            DBLog.log(DBLog.LV_ERROR, f"commit batch:{batch_name} before db not_query_operation")
            return False
        else:
            batch_lock = batch["lock"]
            batch_lock.acquire()
            self.__batch_mapped_queue_lock.release()
            batch_queue = batch["queue"]
            batch["queue"] = []
            batch_lock.release()
            DBLog.log(DBLog.LV_VERBOSE, f"commit batch:{batch_name}")
            ret = self.__batch_op(batch_queue, querycb=None, commit=True)
        return ret

    # 每次batch query必须只对同一个表询问
    def fetch_batch_query(self, query_list):
        res_rows = []
        def querycb(rows):
            nonlocal res_rows
            res_rows = rows

        self.__batch_op(query_list, querycb)
        return res_rows

    def __str_filter(self, value):
        if isinstance(value, str):
            return f"\"{value}\""
        return value

    # ---------- 通用增删除改查接口 -------------------------
    def update_db(self, batch_name, table_name, condition_columns, condition_values, update_columns, update_values):
        update_sql = f"UPDATE {table_name} SET "
        for column, value in zip(update_columns, update_values):
            value_str = self.__str_filter(value)
            update_sql = update_sql + f"{column} = {value_str},"
        update_sql = update_sql[:-1] + " WHERE "

        for column, value in zip(condition_columns, condition_values):
            value_str = self.__str_filter(value)
            update_sql = update_sql + f"{column} = {value_str} AND "

        self.__push_batch(batch_name, update_sql[:-5])

    # update_if_exist参数：如果存在则更新，需要确保有唯一性的列
    def insert_db(self, batch_name, table_name, insert_columns, insert_values, update_if_exist=False):
        columns = "("
        for column in insert_columns:
            columns = columns + column + ", "
        columns = columns[:-2] + ")"
        values = ""
        for value in insert_values:
            value_str = self.__str_filter(value)
            values = values + f"{value_str}, "

        sql_insert = f"INSERT INTO {table_name} {columns} VALUES ({values[:-2]})"

        if update_if_exist:
            sql_insert = sql_insert + " ON DUPLICATE KEY UPDATE "
            for column, value in zip(insert_columns, insert_values):
                sql_insert = sql_insert + f"{column} = {self.__str_filter(value)}, "
            sql_insert = sql_insert[:-2]
        self.__push_batch(batch_name, sql_insert)

    def query_db(self, table_name, query_columns, condition_columns, condition_values,order=None, limit=None, offset=None):
        conditions = ""
        if condition_columns is not None and len(condition_columns) > 0:
            conditions = "WHERE "
            for column, value in zip(condition_columns, condition_values):
                value_str = self.__str_filter(value)
                conditions = conditions + f"{column} = {value_str} AND "
            conditions = conditions[:-5]

        columns = "*"
        if query_columns is not None and len(query_columns) > 0:
            columns = ""
            for column in query_columns:
                columns = columns + f"{column}, "
            columns = columns[:-2]
        query_sql = f"SELECT {columns} from {table_name} {conditions}"

        if order is not None:
            query_sql += f" ORDER BY available DESC"
        if limit is not None:
            query_sql += f" LIMIT {limit}"
        if offset is not None:
            query_sql += f" OFFSET {offset}"

        return query_sql



    def delete_db(self, batch_name, table_name, condition_columns, condition_values):
        conditions = ""
        for column, value in zip(condition_columns, condition_values):
            value_str = self.__str_filter(value)
            conditions = conditions + f"{column} = {value_str} AND "

        delete_sql = f"DELETE FROM {table_name} WHERE {conditions[:-5]}"
        self.__push_batch(batch_name, delete_sql)


# 业务接口，今后可自行根据需要扩展添加
class DBInterface:
    # 初始化并连接数据库，连接失败抛出Exception需要处理
    def __init__(self, host, user, pwd, dbname, pool_size=5, log_lv=DBLog.LV_ERROR):
        self.drv = DBInterfaceDriver(host, user, pwd, dbname, pool_size, log_lv)

    def commit_batch_update_insert(self, batch_name):
        return self.drv.commit_batch_update_insert(batch_name)

    def insert_transaction_dota(self, batch_name, transaction_list):
        columns = ["user_address, currency_tick, status, tx_hash, block_height, extrinsic_index, amount, from_address, to_address, fee"]
        for values in transaction_list:
            self.drv.insert_db(batch_name=batch_name,
                               table_name="transaction_dota",
                               insert_columns=columns,
                               insert_values=values)

    # 更新"状态"和"修改时间"
    def update_transaction_dota(self, batch_name, transaction_list):
        for [currency_tick, status, tx_hash, block_height, extrinsic_index] in transaction_list:
            self.drv.update_db(batch_name=batch_name,
                               table_name="transaction_dota",
                               condition_columns=["currency_tick", "tx_hash", "block_height", "extrinsic_index"],
                               condition_values=[currency_tick, tx_hash, block_height, extrinsic_index],
                               update_columns=["status"],
                               update_values=[status])

    def get_transaction_dota(self, block_num, currency_tick):
        columns = ["block_height, tx_hash, extrinsic_index, from_address, to_address, currency_tick, amount, status"]
        query = self.drv.query_db(
            table_name="transaction_dota",
            query_columns=columns,
            condition_columns=["block_height", "currency_tick"],
            condition_values=[block_num, currency_tick])
        return self.drv.fetch_batch_query([query])

    def get_balance(self, address_list, currency_tick):
        query_list = []
        for user_address in address_list:
            query = self.drv.query_db(
                table_name="user_currency_balance",
                query_columns=["user_address, available, hold"],
                condition_columns=["user_address", "currency_tick"],
                condition_values=[user_address, currency_tick])
            query_list.append(query)

        res = self.drv.fetch_batch_query(query_list)
        return res

    def delete_tables(self, batch_name):
        sql_update = ["transaction_dota", "user_bill", "sys_config"]
        for i in sql_update:
            self.drv.delete_db(batch_name=batch_name,
                               table_name=i,
                               condition_columns=[1],
                               condition_values=[1]
            )

    def update_or_insert_balance(self, batch_name, address_available_hold_list, currency_tick):
        for [address, available, hold] in address_available_hold_list:
            self.drv.insert_db(batch_name=batch_name,
                               table_name="user_currency_balance",
                               insert_columns=["currency_tick", "user_address", "available", "hold"],
                               insert_values=[currency_tick, address, available, hold],
                               update_if_exist=True)

    def get_block_height(self, currency_tick):
        query = self.drv.query_db(table_name="sys_config",
                                  query_columns=["checker_block_height", "crawler_block_height"],
                                  condition_columns=["currency_tick"],
                                  condition_values=[currency_tick])
        return self.drv.fetch_batch_query([query])

    def update_or_insert_checker_block_height(self, batch_name, currency_tick, block_num):
        self.drv.insert_db(batch_name=batch_name,
                           table_name="sys_config",
                           insert_columns=["checker_block_height", "currency_tick"],
                           insert_values=[block_num, currency_tick],
                           update_if_exist=True)

    def update_or_insert_crawler_block_height(self, batch_name, currency_tick, block_num):
        self.drv.insert_db(batch_name=batch_name,
                           table_name="sys_config",
                           insert_columns=["crawler_block_height", "currency_tick"],
                           insert_values=[block_num, currency_tick],
                           update_if_exist=True)

    def update_or_insert_checker_crawler_block_height(self, batch_name, currency_tick, checker_block_num,crawler_block_num):
        self.drv.insert_db(batch_name=batch_name,
                           table_name="sys_config",
                           insert_columns=["checker_block_height","crawler_block_height","currency_tick"],
                           insert_values=[checker_block_num, crawler_block_num ,currency_tick],
                           update_if_exist=True)

    def insert_user_bill(self, batch_name, bill_list):
        columns = ["user_address, from_address, to_address, currency_tick, type, tx_hash, block_height, extrinsic_index, amount, before_balance, after_balance, fee"]
        for values in bill_list:
            self.drv.insert_db(batch_name=batch_name,
                               table_name="user_bill",
                               insert_columns=columns,
                               insert_values=values)


# # 接口使用例子
# if __name__ == "__main__":
#     db_interface = DBInterface(host='rm-3ns3253p1640igl8r9o.mysql.rds.aliyuncs.com',
#                                user='dota20_test',
#                                pwd='XEKnyUd2NerLHTs#',
#                                dbname='dota20_test',
#                                pool_size=5,
#                                log_lv=DBLog.LV_VERBOSE)

    # db_interface.drv.insert_db("sys_config", ["currency_tick", "checker_block_height", "crawler_block_height"], ["A", 1000, 1200])
    # db_interface.drv.insert_db("sys_config", ["currency_tick", "checker_block_height", "crawler_block_height"], ["B", 1000, 1200])
    # db_interface.drv.insert_db("sys_config", ["currency_tick", "checker_block_height", "crawler_block_height"], ["C", 1000, 1200])

    # db_interface.drv.query_db("sys_config", ["user_address", "checker_block_height", "crawler_block_height"], ["checker_block_height", "crawler_block_height"], [1200, 1300])
    # db_interface.drv.query_db("sys_config", ["user_address", "checker_block_height", "crawler_block_height"], ["checker_block_height", "crawler_block_height"], [1000, 12213123])
    # print(db_interface.drv.fetch_batch_query())

    # db_interface.drv.delete_db("sys_config", ["checker_block_height"], [1000])
    # db_interface.drv.update_db("sys_config", ["checker_block_height", "crawler_block_height"], [1000, 1200], ["checker_block_height", "crawler_block_height"], [1200, 1300])

    # list:[user_address, currency_tick, status, tx_hash, block_height, extrinsic_index, amount, from_address, to_address, fee]
    # db_interface.insert_transaction_dota([
    #     ["d1", "D", 0, "txhash1", 1003, 1050, 50, "d1", "d2", 0.5],
    #     ["d2", "D", 0, "txhash1", 1004, 1050, 50, "d1", "d2", 0.5],
    #     ["d3", "D", 0, "txhash1", 1005, 1050, 50, "d1", "d2", 0.5],
    # ])

    # list:[currency_tick, status, tx_hash, block_height, extrinsic_index]
    # db_interface.update_transaction_dota([["0", 15, "txhash1", 1000, 1050]])


    # res = db_interface.get_block_height("DOTA")
    # print(res)
    #
    # res = db_interface.get_balance(
    #     ["5CiPPseXPECbkjWCa6MnjNokrgYjMqmKndv2rSnekmSK2DjL", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"],
    #     "DOTA"
    # )
    # print(res)

    # # list:[addr, available, hold], currency_tick
    # db_interface.update_balance(
    #     [["5CiPPseXPECbkjWCa6MnjNokrgYjMqmKndv2rSnekmSK2DjL777", 51, 61],
    #      ["5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty777", 71, 81]],
    #     "DOTA"
    # )

    # print(db_interface.get_block_height("0"))

    # db_interface.insert_user_bill([
    #     ["d1", "df", "dt", "0", 0, "txhash1", 1001, 1050, 50, 100, 150, 0.5]
    # ])

    # db_interface.update_or_insert_checker_block_height("A", 1200)
    #
    # db_interface.update_or_insert_crawler_block_height("0", 12213123)

    # print(db_interface.commit_batch_update_insert())