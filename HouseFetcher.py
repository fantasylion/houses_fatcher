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
import sets as sets
from selenium import webdriver
import time
import re
from mysql_helper import MySqlHelper
import logging

class HouseFeatcher:

    select_option = ['ListingMethod', 'CommissionPrice', 'area', 'SortBy', 'HousingPurposes', 'Type', 'IsThereAPicture',
                     'CitySelection', 'Zoning']
    cities = ['all', 'mainCity', 'yuhang', 'xiaoshan', 'fuyang', 'cunan', 'tonglu']
    city_names = ['所有城区', '主城区', '余杭', '萧山', '富阳', '淳安', '桐庐']

    conn_dict = {
        'host': "172.16.10.123",
        'login': 'serio',
        'password': '1234',
        'schema': 'serio_local',
        'port': 3306,
        'charset_type': 'utf8'
    }


    def __init__(self):
        self.browser = webdriver.Chrome('D:\DATA\software\chromedriver\chromedriver.exe')
        self.url = 'http://jjhygl.hzfc.gov.cn/guid/index.html'
        self.current_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def open_main_page(self):

        self.browser.get(self.url)
        self.browser.find_element_by_css_selector('.content .ml185').click()

    def fetch_house_number(self, city):
        sel1 = self.browser.find_elements_by_css_selector('#pageForm #selectBox2')
        city_selection = sel1[self.select_option.index('CitySelection')]
        city_selection.click()
        time.sleep(1)
        sel2 = city_selection.find_elements_by_css_selector('#selectBox2 .submenu li')
        sel2[self.cities.index(city)].click()
        result = self.browser.find_element_by_css_selector('.page_list').text
        et = re.findall(r"\(共(\d*)条\)", result)
        if self.check_success() is not True:
            raise RuntimeError('no house found.')
        return et[0]

    def check_success(self):
        house_items = self.browser.find_elements_by_css_selector('.table2 tr')
        return len(house_items) > 0

    def already_counted(self, city_name):
        query_exists = "SELECT count(1) is_exists from hz_second_hand_housing where query_time = '{}' and area = '{}'"
        sql = query_exists.format(self.current_date, city_name)
        logging.info(sql)
        result = MySqlHelper.find_results(self.conn_dict, sql)
        return result[0]['is_exists']

    def insert_data(self, city_name, house_number):
        insert_sql = "INSERT INTO `hz_second_hand_housing`(`area`, `query_time`, `sets_number`) VALUES ( '{}', '{}', {})"
        insert_sql = insert_sql.format(city_name, self.current_date, str(house_number))
        logging.info(insert_sql)
        MySqlHelper.exec_sql(self.conn_dict, insert_sql)

    def start_fetch_house(self):
        self.open_main_page()
        for index, city in enumerate(self.cities):
            time.sleep(1)
            house_number = self.fetch_house_number(city)
            city_name = self.city_names[index]
            logging.info("查询时间：{},{} 有 {} 套挂牌".format(self.current_date, city_name, str(house_number)))

            if self.already_counted(city_name) <= 0:
                self.insert_data(city_name, house_number)
            else:
                logging.info('{} is already insert in {}'.format(city_name, self.current_date))
        self.browser.close()

