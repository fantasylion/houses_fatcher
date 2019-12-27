<<<<<<< HEAD
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pymysql
import logging


class MySqlHelper:

    @classmethod
    def find_results(cls, conn_dict, sql, cursor_type=pymysql.cursors.DictCursor):
        # 自定义连接配置
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor(cursor_type)
        try:
            # 执行SQL语句
            # logging.info('find_results: '+sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logging.error("find_results, 查询失败 \n conn:{} \n sql:{} \n exception:{}".format(conn_dict, sql, e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def exec_sql(cls, conn_dict, sql, values=None):
        # 连接配置中心
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor()
        try:
            # logging.info('exec_sql: '+sql)
            # 执行SQL语句
            if not values:
                cursor.execute(sql)
            else:
                cursor.execute(sql, values)
            conn.commit()
            return True
        except Exception as e:
            logging.error("exec_sql, 执行失败 \n conn:{} \n sql:{} \n exception:{}".format(conn_dict, sql, e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def exec_many(cls, conn_dict, sql, values):
        # 连接配置中心
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor()
        try:
            # 执行SQL语句
            # logging.info('exec_many: ' + sql)
            exec_ret = cursor.executemany(sql, values)
            conn.commit()
            return exec_ret
        except Exception as e:
            logging.error('exec_many,批量执行失败:{}'.format(e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def create_table(cls, conn_src, conn_tgt, table_name, table_name_tgt, archive_field):
        # 创建新表
        sql = "show create table {}".format(table_name)
        results = cls.find_results(conn_src, sql)
        # 原始表不存在
        if not results:
            return False

        # 获取创建表sql
        sql = results[0]['Create Table']
        # 此处防止表名替换时，字段名被替换
        sql = sql.replace('TABLE `' + table_name + '`', 'TABLE `' + table_name_tgt + '`')
        exec_ret = cls.exec_sql(conn_tgt, sql)
        if not exec_ret:
            return exec_ret

        # 添加归档字段
        sql = "alter table `{}` add column `{}` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP \
          COMMENT '归档时间'".format(table_name_tgt, archive_field)
        exec_ret = cls.exec_sql(conn_tgt, sql)
        return exec_ret


=======
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pymysql
import logging


class MySqlHelper:

    @classmethod
    def find_results(cls, conn_dict, sql, cursor_type=pymysql.cursors.DictCursor):
        # 自定义连接配置
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor(cursor_type)
        try:
            # 执行SQL语句
            # logging.info('find_results: '+sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logging.error("find_results, 查询失败 \n conn:{} \n sql:{} \n exception:{}".format(conn_dict, sql, e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def exec_sql(cls, conn_dict, sql, values=None):
        # 连接配置中心
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor()
        try:
            # logging.info('exec_sql: '+sql)
            # 执行SQL语句
            if not values:
                cursor.execute(sql)
            else:
                cursor.execute(sql, values)
            conn.commit()
            return True
        except Exception as e:
            logging.error("exec_sql, 执行失败 \n conn:{} \n sql:{} \n exception:{}".format(conn_dict, sql, e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def exec_many(cls, conn_dict, sql, values):
        # 连接配置中心
        conn = pymysql.connect(host=conn_dict['host'], user=conn_dict['login'], passwd=conn_dict['password'],
                               db=conn_dict['schema'], port=conn_dict['port'], charset=conn_dict['charset_type'])
        cursor = conn.cursor()
        try:
            # 执行SQL语句
            # logging.info('exec_many: ' + sql)
            exec_ret = cursor.executemany(sql, values)
            conn.commit()
            return exec_ret
        except Exception as e:
            logging.error('exec_many,批量执行失败:{}'.format(e))
            raise
        finally:
            cursor.close()
            conn.close()

    @classmethod
    def create_table(cls, conn_src, conn_tgt, table_name, table_name_tgt, archive_field):
        # 创建新表
        sql = "show create table {}".format(table_name)
        results = cls.find_results(conn_src, sql)
        # 原始表不存在
        if not results:
            return False

        # 获取创建表sql
        sql = results[0]['Create Table']
        # 此处防止表名替换时，字段名被替换
        sql = sql.replace('TABLE `' + table_name + '`', 'TABLE `' + table_name_tgt + '`')
        exec_ret = cls.exec_sql(conn_tgt, sql)
        if not exec_ret:
            return exec_ret

        # 添加归档字段
        sql = "alter table `{}` add column `{}` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP \
          COMMENT '归档时间'".format(table_name_tgt, archive_field)
        exec_ret = cls.exec_sql(conn_tgt, sql)
        return exec_ret


>>>>>>> Add py
