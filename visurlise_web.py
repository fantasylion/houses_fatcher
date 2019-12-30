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

import random
import math
import schedule
import time
from pyecharts.charts import Line, Page
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pyecharts.faker import Collector, Faker

from mysql_helper import MySqlHelper

class VisurliseWeb:


    conn_dict = {
        'host': "172.16.10.123",
        'login': 'serio',
        'password': '1234',
        'schema': 'serio_local',
        'port': 3306,
        'charset_type': 'utf8'
    }

    def __init__(self):
        self.city_names = ['所有城区', '主城区', '余杭', '萧山', '富阳', '淳安', '桐庐']

    def get_max(self, city):
        sql = "SELECT max(sets_number) max_num from hz_second_hand_housing where area = '{}'".format(city)
        result = MySqlHelper.find_results(self.conn_dict, sql)
        return result[0]['max_num']

    def get_min(self, city):
            sql = "SELECT min(sets_number) min_num from hz_second_hand_housing where area = '{}'".format(city)
            result = MySqlHelper.find_results(self.conn_dict, sql)
            return result[0]['min_num']

    def get_setting(self):
        setting = {}
        for city in self.city_names:
            setting[city] = [self.get_min(city)-100, self.get_max(city)+100]
        return setting

    def line_yaxis_log(self) -> Line:
        settings = self.get_setting()
        lines = []
        for city in self.city_names:
            line_gird = Line(init_opts=opts.InitOpts(opts.InitOpts(width='1200px', height='1200px')))
            sql = "SELECT * from hz_second_hand_housing where area = '{}'".format(city)
            result = MySqlHelper.find_results(self.conn_dict, sql)
            xname = []
            ynum = []

            # 遍历表里的数据
            for x in result:
                xname.append(x['query_time'])
                ynum.append(x['sets_number'])
            print(xname)
            print(ynum)
            line_gird.set_global_opts(
                title_opts=opts.TitleOpts(title="{}二手房库存走势图".format(city)),
                xaxis_opts=opts.AxisOpts(name="查询时间"),
                yaxis_opts=opts.AxisOpts(
                    type_="log",
                    name="套数",
                    splitline_opts=opts.SplitLineOpts(is_show=True),
                    is_scale=True,
                    # split_number=30,
                    # min_interval=100,
                    # max_interval=1000,
                    min_=settings[city][0],
                    max_=settings[city][1],
                )
            )
            line_gird.add_xaxis(xaxis_data=xname)
            line_gird.add_yaxis(
                city,
                y_axis=ynum,
                linestyle_opts=opts.LineStyleOpts(width=2),
            )

            lines.append(line_gird)
        Page().add(*lines).render()


VisurliseWeb().line_yaxis_log()

