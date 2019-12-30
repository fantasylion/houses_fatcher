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
import schedule
import time
import logging
from HouseFetcher import HouseFeatcher
from visurlise_web import VisurliseWeb


def start_fetch_house():
    excute_fetch_house(1)
    VisurliseWeb().line_yaxis_log()


def excute_fetch_house(count):
    if count > 10:
        logging.info('exception more than 10. give up it')
    try:
        house_fetcher = HouseFeatcher()
        house_fetcher.start_fetch_house()
    except:
        logging.error("Catch exception try fetch_house again alter. ", exc_info=True)
        time.sleep(60 * 1)
        ++count
        excute_fetch_house(count)


start_fetch_house()
schedule.every(2).hours.do(start_fetch_house)
logging.info('schedule start')
while True:
    schedule.run_pending()




