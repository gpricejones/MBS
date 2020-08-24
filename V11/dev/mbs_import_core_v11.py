import Logger
import DBhandler
import LogCleaner
import EventLogger
import AlertManager
import DirectoryCleaner
import LicenseValidator
from StdTime import time, file_time, s_stamp, year_stamp, short_year_stamp, short_now, current_Date
import os
import sys
import time
import json
import base64
import shutil
import hashlib
import requests
import traceback
from pyDes import *
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def main():
    try:

        global file_name, drive_letter, use_pricer_level, log_level, log_delete_after, log_max, log_name, log_path, log_file, use_pricer_db, local_db, data_in_save, data_input_delete_after, input_data_path, wait_time, sort_order, none_100, usage_convert, IPF100, term_multi, date_format, ipf1x5_threshold, display_original, section_commas, New_On_Hand, Used_On_Hand, New_Addl, Used_Addl, New_Pend_Ret, Used_Pend_Ret, New_insite_Pend_Ord, Used_insite_Pend_Ord, New_Rental_insite_Pend_Ord, Used_Rental_insite_Pend_Ord, use_pfi, use_api, use_soap, i1_path, m1_path, r7_path, api_page_count, soap_api_ip, soap_token, soap_user, rest_api_url, rest_token
        global requests, db, client, config_path, license_status, pricer_api_alive, logger, origin_time, total_api_req_counter

        origin_time = time.time()

        key = 'yjhtlsic64,9w6H1'

        requests = requests.Session()
        retry = Retry(total=10, backoff_factor=1, status_forcelist=[404])
        adapter = HTTPAdapter(retry)
        requests.mount('http', adapter)

        type_id = 5

        pricer_api_alive = False

        api_out_page_count = 0
        api_out_line_count = 0
        api_req_counter = 0
        page_line_count = 0
        out_count = 0
        row_counter = 0
        total_api_req_counter = 0

        category_list = {}

        api_responses = []
        pricer_response = []
        json_outstring = []
        soap_update_str = []

        delete_array = []

        command_id = 0
        api_requestid = ""
        last_page_flag = 0

        ##########get file name without extension
        file_name_raw = __file__
        file_name_start = file_name_raw.rfind('/') + 1
        file_name = file_name_raw[file_name_start: -3]

        config_path = ""

        dev_mode = 'PYCHARM_HOSTED' in os.environ

        if not dev_mode:
            if len(sys.argv) == 1:
                if os.name == 'nt':
                    import EncrypterWinEmb
                    EncrypterWinEmb.encrypter()
                    sys.exit()
                else:
                    import EncrypterLinux
                    EncrypterLinux.encrypter()

            config_path = sys.argv[1]
        else:
            if os.name == 'nt':
                config_path = "c:\\pricer\\import\\configuration\\mbs_import.mms"
            else:
                config_path = "/pricer/import/configuration/mbs_import.mms"

        try:
            with open(config_path) as config_source:
                content = config_source.read()
            config = BeautifulSoup(content, 'html.parser')

            ###drive
            if os.name == 'nt':
                drive_letter = config.mmimport.get('drive') + ':\\'
            else:
                drive_letter = '/'

            ###logging
            if (config.logging.get('use_pricer_level')).lower() == "true":
                use_pricer_level = True
            else:
                use_pricer_level = False

            log_level = config.logging.get('level').lower()
            log_delete_after = config.logging.get('delete_after')
            log_max = config.logging.get('log_max_mb')
            log_name = config.logging.get('log_name')
            log_path = config.logging.get('path')

            log_file = os.path.join(drive_letter, log_path, config.logging.get('log_name'))

            if os.name != 'nt':
                log_file = log_file.replace('\\', '/')
                log_path = log_path.replace('\\', '/')

            ##########set up logging

            if use_pricer_level:
                if os.name == 'nt':
                    pricer_conf_path = drive_letter + "Pricer\\R3Server\\config\\log4j2.xml"
                else:
                    pricer_conf_path = "/pricer/r3server/config/log4j2.xml"
                pricer_content = open(pricer_conf_path, "r").read()
                pricer_level = pricer_content[(pricer_content.find('Root level') + 12): (pricer_content.find('Root level') + 16)]

                if pricer_level.lower() == 'info':
                    log_level = 'info'
                elif pricer_level.lower() == 'erro':
                    log_level = 'error'
                elif pricer_level.lower() == 'warn':
                    log_level = 'warning'
                else:
                    log_level = 'debug'

            # clean up first
            try:
                LogCleaner.log_cleaner(drive_letter, log_file, log_level, config_path, log_max)
            except:
                e = traceback.format_exc(limit=None, chain=True)
                AlertManager.alert_manager(config_path, log_path, 'warning', "Log Cleaner failed on " + log_file + " with error " + str(e))

            try:
                logger, consolehandler, filehandler = Logger.log_handlers(log_file, log_level, config_path, log_path)
            except:
                e = traceback.format_exc(limit=None, chain=True)
                AlertManager.alert_manager(config_path, log_path, 'fatal', "Logging failed to start with error " + str(e))
                sys.exit(99)

            if not os.path.isdir(log_path):
                os.makedirs(log_path, mode=0o777)
                logger.debug('Successfully created {} input folder for archiving.'.format(log_path))

            logger.info(file_name + ' successfully started with logging at ' + log_level.upper() + '.')
            if year_stamp != '2020':
                logger.info('©Copyright 2020-' + year_stamp + ' MarginMate Corporation. All Rights Reserved.')
            else:
                logger.info('©Copyright 2020 MarginMate Corporation. All Rights Reserved.')
            #################################################

            if os.name == 'nt':
                EventLogger.event_log_writer(file_name, "info", "Pricer", 100, log_path)
            else:
                EventLogger.event_log_writer(file_name_raw, "info", "Pricer", 100, log_path)

            ##data local
            if (config.datalocal.get('usepricer')).lower() == "true":
                use_pricer_db = True
            else:
                use_pricer_db = False

            local_db = str(triple_des(key).decrypt(base64.b64decode(config.datalocal.get('localdb')), padmode=2), 'utf-8')

            ####Datain
            if (config.datain.get('datainputsave')).lower() == "true":
                data_in_save = True
            else:
                data_in_save = False

            data_input_delete_after = config.datain.get('deleteafter')
            input_data_path = os.path.join(drive_letter, config.datain.get('path'))
            wait_time = config.datain.get('filewaittime')

            #### Sort
            sort_order = str(config.sort.get('order')).lower()

            #### Price
            if (config.price.get('none_100')).lower() == "true":
                none_100 = True
            else:
                none_100 = False

            ### Usage
            if (config.usage.get('convert')).lower() == "true":
                usage_convert = True
            else:
                usage_convert = False

            ### Zero_Zero
            if (config.zero_zero.get('ipf100')).lower() == "true":
                IPF100 = True
            else:
                IPF100 = False

            ### Term
            if (config.term.get('multi')).lower() == "true":
                term_multi = True
            else:
                term_multi = False

            ### Date formate
            date_format = config.date.get('format')

            ### IPF1X5
            ipf1x5_threshold = config.ipf1x5.get('threshold')

            ### Replace
            if (config.replace_section_commas.get('displayoriginal')).lower() == "true":
                display_original = True
            else:
                display_original = False

            section_commas = config.replace_section_commas.get('char')

            #### QOH CALC

            New_On_Hand = config.qoh_calc.get('new_on_hand')
            Used_On_Hand = config.qoh_calc.get('used_on_hand')
            New_Addl = config.qoh_calc.get('new_addl')
            Used_Addl = config.qoh_calc.get('used_addl')
            New_Pend_Ret = config.qoh_calc.get('new_pend_ret')
            Used_Pend_Ret = config.qoh_calc.get('used_pend_ret')
            New_insite_Pend_Ord = config.qoh_calc.get('new_insite_pend_ord')
            Used_insite_Pend_Ord = config.qoh_calc.get('used_insite_pend_ord')
            New_Rental_insite_Pend_Ord = config.qoh_calc.get('new_rental_insite_pend_ord')
            Used_Rental_insite_Pend_Ord = config.qoh_calc.get('used_rental_insite_pend_ord')

            ####Dataout
            if (config.dataout.get('usepfi')).lower() == "true":
                use_pfi = True
            else:
                use_pfi = False

            if (config.dataout.get('useapi')).lower() == "true":
                use_api = True
            else:
                use_api = False

            if (config.dataout.get('usesoap')).lower() == "true":
                use_soap = True
            else:
                use_soap = False

            i1_path = config.dataout.get('i1_path')
            m1_path = config.dataout.get('m1_path')
            r7_path = config.dataout.get('r7_path')
            api_page_count = config.dataout.get('apipagecount')
            soap_api_ip = str(triple_des(key).decrypt(base64.b64decode(config.dataout.get('soapip')), padmode=2), 'utf-8')
            soap_token = str(triple_des(key).decrypt(base64.b64decode(config.dataout.get('soaptoken')), padmode=2), 'utf-8')
            soap_user = str(triple_des(key).decrypt(base64.b64decode(config.dataout.get('soapuser')), padmode=2), 'utf-8')
            rest_api_url = str(triple_des(key).decrypt(base64.b64decode(config.dataout.get('soapip')), padmode=2), 'utf-8')
            rest_token = str(triple_des(key).decrypt(base64.b64decode(config.dataout.get('soaptoken')), padmode=2), 'utf-8')



        except IOError as e:
            EventLogger.event_log_writer(file_name + " failed to find configuration file " + config_path + " I/O error({0}): {1}".format(e.errno, e.strerror), "error", "Pricer", 101, log_path)
            sys.exit(99)

            # start soap client if used

        if (use_api and use_soap):
            from requests import Session
            from zeep import Client
            from zeep.transports import Transport
            import zeep
            from hashlib import sha256

            api_wait_count = 0

            minute_timer = time.time() + 60 * 1
            while not pricer_api_alive:
                wsdl = 'http://' + soap_api_ip + '/pricer_5_0?wsdl'
                client = Client(wsdl)
                login_challenge = client.service.getLoginChallenge(soap_user)
                login_key = base64.b64encode(sha256((login_challenge + soap_token).encode()).digest()).decode()

                session = Session()
                client.transport.session.headers.update({'username': soap_user})
                client.transport.session.headers.update({'authentication-token': login_key})

                pricer_api_alive = client.service.isAlive({'level': 0})

                if str(pricer_api_alive).lower() == 'true':
                    pricer_api_alive = True
                    logger.debug("Pricer SOAP API returned status alive.")
                else:
                    pricer_api_alive = False
                    logger.debug("Pricer SOAP API returned status NOT alive.")
                    if time.time() > minute_timer:
                        api_wait_count += 1
                        minute_timer = time.time() + 60 * 1
                        logger.info("Pricer SOAP API returned status NOT alive. after {} retry's".format(api_wait_count))
                        if api_wait_count >= 5:
                            logger.critical("Pricer SOAP API has failed to start. ")
                            break

        # set up https header values
        patch_header = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": rest_token}

        # type 5 is MBS
        try:
            license_status = LicenseValidator.license_validator(drive_letter, type_id, log_file, log_path, log_level, config_path, 'True', "")

            if license_status == 0:
                logger.info('This software is not licensed, execution limited to first 50 rows of input data.')
                logger.info('See http://www.marginmate.com/software/license/eula.php for license agreement.')

            if license_status == 2:
                logger.info('This software license is expired, execution limited to first 50 rows of input data.')
                logger.info('See http://www.marginmate.com/software/license/eula.php for license agreement.')

            if license_status == 1:
                logger.info('License validated.')
                logger.info('Used under license. See http://www.marginmate.com/software/license/eula.php for license agreement.')
        except:
            logger.critical("Critical error has occured see event viewer/log for more information.")
            e = traceback.format_exc(limit=None, chain=True)
            AlertManager.alert_manager(config_path, log_path, 'fatal', "License Validation failed with error " + str(e))
            sys.exit(99)

        ##########connect pricer db
        try:
            db = DBhandler.pricer_db(drive_letter, log_file, log_path, log_level, config_path, file_name, 'True', '')
        except:
            logger.critical("Critical error has occured see event viewer/log for more information.")
            e = traceback.format_exc(limit=None, chain=True)
            AlertManager.alert_manager(config_path, log_path, 'fatal', "Failed to connect to database with error " + str(e))
            sys.exit(99)

        # clean up input directory
        if data_in_save and int(data_input_delete_after) > 0:
            DirectoryCleaner.clean_directory(data_input_delete_after, input_data_path, log_file, log_level, config_path)

        ################### Main File Content Processing ###################

        file_list = {}
        file_list_new = []

        for ext_file_name in os.listdir(input_data_path):
            if ext_file_name.lower().endswith("tx1") or ext_file_name.lower().endswith("txt"):
                file_list.update({ext_file_name: str(os.stat(input_data_path + ext_file_name).st_mtime)})

        for current_file in sorted(file_list, key=file_list.get, reverse=False):
            file_list_new.append(current_file)

        file_txt_list = {}
        file_tx1_list = {}
        file_list_new = []

        for ext_file_name in os.listdir(input_data_path):
            if ext_file_name.lower().endswith("txt"):
                file_txt_list.update({ext_file_name: str(os.stat(input_data_path + ext_file_name).st_mtime)})
            if ext_file_name.lower().endswith("tx1"):
                file_tx1_list.update({ext_file_name: str(os.stat(input_data_path + ext_file_name).st_mtime)})

        for current_file in sorted(file_txt_list, key=file_txt_list.get, reverse=False):
            sleep_count = 0
            if current_file[:-4] + '.tx1' in sorted(file_tx1_list, key=file_tx1_list.get, reverse=False):
                file_list_new.append(current_file)
                logger.debug('Found matching file for {}'.format(current_file))
            else:
                while current_file[:-4] + '.tx1' not in sorted(file_tx1_list, key=file_tx1_list.get, reverse=False):
                    time.sleep(1)
                    sleep_count += 1
                    if sleep_count == wait_time:
                        logger.debug("could not find matching file for {}".format(current_file))
                        break

        for current_file in sorted(file_tx1_list, key=file_tx1_list.get, reverse=False):
            sleep_count = 0
            if current_file[:-4] + '.txt' in sorted(file_txt_list, key=file_txt_list.get, reverse=False):
                file_list_new.append(current_file)
                logger.debug('Found matching file for {}'.format(current_file))
            else:
                while current_file[:-4] + '.txt' not in sorted(file_txt_list, key=file_txt_list.get, reverse=False):
                    time.sleep(1)
                    sleep_count += 1
                    if sleep_count == wait_time:
                        logger.debug("could not find matching file for {}".format(current_file))
                        break

        logger.debug(sorted(file_list_new))

        if len(file_list_new) > 0:

            api_out_page_count = 0
            api_out_line_count = 0
            page_line_count = 0
            out_count = 0
            row_counter = 0

            json_outstring = []
            soap_update_str = []

            # process each file

            for new_file in sorted(file_list_new):
                file_content = file_handler(new_file)
                logger.info("Start processing output from {} to API".format(new_file))

                if new_file.lower().endswith("tx1"):

                    row_counter = 0

                    secondary_file_data_list = []

                    for file_row in file_content:
                        row_counter += 1
                        file_row = file_row.rstrip(os.linesep)
                        row_fields = file_row.split('|')

                        secondary_file_dict = {}

                        if len(row_fields) >= 19:
                            secondary_file_dict.update({"GenKey": row_fields[0]})
                            secondary_file_dict.update({"csn": row_fields[1]})
                            secondary_file_dict.update({"seq_no": row_fields[2]})
                            secondary_file_dict.update({"ISBN": row_fields[3]})
                            secondary_file_dict.update({"Term": row_fields[4]})
                            secondary_file_dict.update({"Term_Description": row_fields[5]})
                            secondary_file_dict.update({"dept": row_fields[6]})
                            secondary_file_dict.update({"course": row_fields[7]})
                            secondary_file_dict.update({"section": row_fields[8]})
                            secondary_file_dict.update({"loc_code": row_fields[9]})
                            secondary_file_dict.update({"bookxofy": row_fields[10]})
                            secondary_file_dict.update({"course_id": row_fields[11]})
                            secondary_file_dict.update({"instructor": row_fields[12]})
                            secondary_file_dict.update({"course_code": row_fields[13]})
                            secondary_file_dict.update({"delete_flag": row_fields[14]})
                            secondary_file_dict.update({"ebook_adopted": row_fields[15]})
                            secondary_file_dict.update({"class_cap": row_fields[16]})
                            secondary_file_dict.update({"prof_requested": row_fields[17]})
                            secondary_file_dict.update({"estimated_sales": row_fields[18]})
                            secondary_file_dict.update({"ebook1_vendor": "0.00"})
                            secondary_file_dict.update({"ebook1_period_1": "0.00"})
                            secondary_file_dict.update({"ebook1_price_1": "0.00"})
                            secondary_file_dict.update({"ebook1_period_2": "0.00"})
                            secondary_file_dict.update({"ebook1_price_2": "0.00"})
                            secondary_file_dict.update({"ebook1_period_3": "0.00"})
                            secondary_file_dict.update({"ebook1_price_3": "0.00"})
                            secondary_file_dict.update({"ebook1_period_4": "0.00"})
                            secondary_file_dict.update({"ebook1_price_4": "0.00"})
                            secondary_file_dict.update({"ebook1_period_5": "0.00"})
                            secondary_file_dict.update({"ebook1_price_5": "0.00"})
                            secondary_file_dict.update({"ebook2_vendor": "0.00"})
                            secondary_file_dict.update({"ebook2_period_1": "0.00"})
                            secondary_file_dict.update({"ebook2_price_1": "0.00"})
                            secondary_file_dict.update({"ebook2_period_2": "0.00"})
                            secondary_file_dict.update({"ebook2_price_2": "0.00"})
                            secondary_file_dict.update({"ebook2_period_3": "0.00"})
                            secondary_file_dict.update({"ebook2_price_3": "0.00"})
                            secondary_file_dict.update({"ebook2_period_4": "0.00"})
                            secondary_file_dict.update({"ebook2_price_4": "0.00"})
                            secondary_file_dict.update({"ebook2_period_5": "0.00"})
                            secondary_file_dict.update({"ebook2_price_5": "0.00"})
                        if len(row_fields) >= 20:
                            if row_fields[19] != "":
                                secondary_file_dict.update({"ebook1_vendor": row_fields[19]})
                            else:
                                secondary_file_dict.update({"ebook1_vendor": "0.00"})
                        if len(row_fields) >= 21:
                            if row_fields[20] != "":
                                secondary_file_dict.update({"ebook1_period_1": row_fields[20]})
                            else:
                                secondary_file_dict.update({"ebook1_period_1": "0.00"})

                        if len(row_fields) >= 22:
                            ebook1_price_1 = row_fields[21]
                            ebook1_price_1 = "%.2f" % (float(ebook1_price_1) / 1)
                            if row_fields[21] != "":
                                secondary_file_dict.update({"ebook1_price_1": row_fields[21]})
                            else:
                                secondary_file_dict.update({"ebook1_period_1": "0.00"})

                        if len(row_fields) >= 23:
                            if row_fields[22] != "":
                                secondary_file_dict.update({"ebook1_period_2": row_fields[22]})
                            else:
                                secondary_file_dict.update({"ebook1_period_2": "0.00"})

                        if len(row_fields) >= 24:
                            ebook1_price_2 = row_fields[23]
                            ebook1_price_2 = "%.2f" % (float(ebook1_price_2) / 1)
                            if row_fields[23] != "":
                                secondary_file_dict.update({"ebook1_price_2": ebook1_price_2})
                            else:
                                secondary_file_dict.update({"ebook1_price_2": "0.00"})

                        if len(row_fields) >= 25:
                            if row_fields[24] != "":
                                secondary_file_dict.update({"ebook1_period_3": row_fields[24]})
                            else:
                                secondary_file_dict.update({"ebook1_period_3": "0.00"})

                        if len(row_fields) >= 26:
                            ebook1_price_3 = row_fields[25]
                            ebook1_price_3 = "%.2f" % (float(ebook1_price_3) / 1)
                            if row_fields[25] != "":
                                secondary_file_dict.update({"ebook1_price_3": ebook1_price_3})
                            else:
                                secondary_file_dict.update({"ebook1_price_3": "0.00"})

                        if len(row_fields) >= 27:
                            if row_fields[26] != "":
                                secondary_file_dict.update({"ebook1_period_4": row_fields[26]})
                            else:
                                secondary_file_dict.update({"ebook1_period_4": "0.00"})

                        if len(row_fields) >= 28:
                            ebook1_price_4 = row_fields[27]
                            ebook1_price_4 = "%.2f" % (float(ebook1_price_4) / 1)
                            if row_fields[27] != "":
                                secondary_file_dict.update({"ebook1_price_4": row_fields[27]})
                            else:
                                secondary_file_dict.update({"ebook1_price_4": "0.00"})

                        if len(row_fields) >= 29:
                            if row_fields[28] != "":
                                secondary_file_dict.update({"ebook1_period_5": row_fields[28]})
                            else:
                                secondary_file_dict.update({"ebook1_period_5": "0.00"})

                        if len(row_fields) >= 30:
                            ebook1_price_5 = row_fields[29]
                            ebook1_price_5 = "%.2f" % (float(ebook1_price_5) / 1)
                            if row_fields[29] != "":
                                secondary_file_dict.update({"ebook1_price_5": ebook1_price_5})
                            else:
                                secondary_file_dict.update({"ebook1_price_5": "0.00"})

                        if len(row_fields) >= 31:
                            if row_fields[30] != "":
                                secondary_file_dict.update({"ebook2_vendor": row_fields[30]})
                            else:
                                secondary_file_dict.update({"ebook2_vendor": "0.00"})

                        if len(row_fields) >= 32:
                            if row_fields[31] != "":
                                secondary_file_dict.update({"ebook2_period_1": row_fields[31]})
                            else:
                                secondary_file_dict.update({"ebook2_period_1": "0.00"})

                        if len(row_fields) >= 33:
                            ebook2_price_1 = row_fields[32]
                            ebook2_price_1 = "%.2f" % (float(ebook2_price_1) / 1)
                            if row_fields[32] != "":
                                secondary_file_dict.update({"ebook2_price_1": ebook2_price_1})
                            else:
                                secondary_file_dict.update({"ebook2_price_1": "0.00"})

                        if len(row_fields) >= 34:
                            if row_fields[33] != "":
                                secondary_file_dict.update({"ebook2_period_2": row_fields[33]})
                            else:
                                secondary_file_dict.update({"ebook2_period_2": "0.00"})

                        if len(row_fields) >= 35:
                            ebook2_price_2 = row_fields[34]
                            ebook2_price_2 = "%.2f" % (float(ebook2_price_2) / 1)
                            if row_fields[34] != "":
                                secondary_file_dict.update({"ebook2_price_2": ebook2_price_2})
                            else:
                                secondary_file_dict.update({"ebook2_price_2": "0.00"})

                        if len(row_fields) >= 36:
                            if row_fields[35] != "":
                                secondary_file_dict.update({"ebook2_period_3": row_fields[35]})
                            else:
                                secondary_file_dict.update({"ebook2_period_3": "0.00"})

                        if len(row_fields) >= 37:
                            ebook2_price_3 = row_fields[36]
                            ebook2_price_3 = "%.2f" % (float(ebook2_price_3) / 1)
                            if row_fields[36] != "":
                                secondary_file_dict.update({"ebook2_price_3": ebook2_price_3})
                            else:
                                secondary_file_dict.update({"ebook2_price_3": "0.00"})

                        if len(row_fields) >= 38:
                            if row_fields[37] != "":
                                secondary_file_dict.update({"ebook2_period_4": row_fields[37]})
                            else:
                                secondary_file_dict.update({"ebook2_period_4": "0.00"})

                        if len(row_fields) >= 39:
                            ebook2_price_4 = row_fields[38]
                            ebook2_price_4 = "%.2f" % (float(ebook2_price_4) / 1)
                            if row_fields[38] != "":
                                secondary_file_dict.update({"ebook2_price_4": ebook2_price_4})
                            else:
                                secondary_file_dict.update({"ebook2_price_4": "0.00"})

                        if len(row_fields) >= 40:
                            if row_fields[39] != "":
                                secondary_file_dict.update({"ebook2_period_5": row_fields[39]})
                            else:
                                secondary_file_dict.update({"ebook2_period_5": "0.00"})

                        if len(row_fields) == 41:
                            ebook2_price_5 = row_fields[40]
                            ebook2_price_5 = "%.2f" % (float(ebook2_price_5) / 1)
                            if row_fields[40] != "":
                                secondary_file_dict.update({"ebook2_price_5": ebook2_price_5})
                            else:
                                secondary_file_dict.update({"ebook2_price_5": "0.00"})

                        secondary_file_data_list.append(secondary_file_dict)
                        logger.debug("Row: {}, with a length of {}.".format(row_counter, len(row_fields)))
                        logger.debug(secondary_file_dict)
                    logger.debug("secondary file data list {}.".format(secondary_file_data_list))

                if new_file.lower().endswith("txt"):

                    row_counter = 0

                    for file_row in file_content:
                        row_counter += 1
                        file_row = file_row.rstrip(os.linesep)
                        row_fields = file_row.split('|')

                        base_GenKey = row_fields[0]
                        base_FormatFlag = row_fields[1]
                        base_Author = row_fields[2]
                        base_Title = row_fields[3]
                        base_ISBN = row_fields[4]
                        base_Used_ISBN = row_fields[5]
                        base_ISBN_HR = row_fields[6]
                        base_Vendor_Style = row_fields[7]
                        base_Publisher = row_fields[8]
                        base_Imprint = row_fields[9]
                        base_Edition = row_fields[10]
                        base_Edition_Status = row_fields[11]
                        base_New_Price = row_fields[12]
                        base_New_Price_Text = row_fields[13]
                        base_Used_Price = row_fields[14]
                        base_Used_Price_Text = row_fields[15]
                        base_New_Rental_Price = row_fields[16]
                        base_New_Rental_Price_Text = row_fields[76]
                        base_Ebook_Price = row_fields[18]
                        base_Ebook_Price_Text = row_fields[19]
                        base_Used_Rental_Price = row_fields[20]
                        base_Used_Rental_Price_Text = row_fields[21]
                        base_Sale_Price1 = row_fields[22]
                        base_Sale_Start_Date1 = row_fields[23]
                        base_Sale_End_Date1 = row_fields[24]
                        base_Sale_Start_Time1 = row_fields[25]
                        base_Sale_End_Time1 = row_fields[26]
                        base_Sale_Price2 = row_fields[27]
                        base_Sale_Start_Date2 = row_fields[28]
                        base_Sale_End_Date2 = row_fields[29]
                        base_Sale_Start_Time2 = row_fields[30]
                        base_Sale_End_Time2 = row_fields[31]
                        base_Sale_Price3 = row_fields[32]
                        base_Sale_Start_Date3 = row_fields[33]
                        base_Sale_End_Date3 = row_fields[34]
                        base_Sale_Start_Time3 = row_fields[35]
                        base_Sale_End_Time3 = row_fields[36]
                        base_Sale_Price4 = row_fields[37]
                        base_Sale_Start_Date4 = row_fields[38]
                        base_Sale_End_Date4 = row_fields[39]
                        base_Sale_Start_Time4 = row_fields[40]
                        base_Sale_End_Time4 = row_fields[41]
                        base_Term = row_fields[42]
                        base_Term_Description = row_fields[43]
                        base_Requested_Qty = row_fields[44]
                        base_Class_Capacity_Qty = row_fields[45]
                        base_Actual_Enrollment_Qty = row_fields[46]
                        base_Est_Sales_Qty = row_fields[47]
                        base_Category = row_fields[48]
                        base_Division = row_fields[49]
                        base_Department = row_fields[50]
                        base_Class = row_fields[51]
                        base_New_Store_Qty = row_fields[52]
                        base_New_Warehouse_Qty = row_fields[53]
                        base_Used_Store_Qty = row_fields[54]
                        base_Used_Warehouse_Qty = row_fields[55]
                        base_New_Pending_Return_Qty = row_fields[56]
                        base_Used_Pending_Return_Qty = row_fields[57]
                        base_New_insite_Pending_Order = row_fields[58]
                        base_Used_insite_Pending_Order = row_fields[59]
                        base_New_Rental_insite_Pending_Order = row_fields[60]
                        base_Used_Rental_insite_Pending_Order = row_fields[61]
                        base_On_Order_PO1 = row_fields[62]
                        base_On_Order_PO1_Vendor = row_fields[63]
                        base_On_Order_Qty1 = row_fields[64]
                        base_On_Order_Qty1_Used = row_fields[65]
                        base_On_Order_Date1 = row_fields[66]
                        base_On_Order_PO2 = row_fields[67]
                        base_On_Order_PO2_Vendor = row_fields[68]
                        base_On_Order_Qty2 = row_fields[69]
                        base_On_Order_Qty2_Used = row_fields[70]
                        base_On_Order_Date2 = row_fields[71]
                        base_On_Order_PO3 = row_fields[72]
                        base_On_Order_PO3_Vendor = row_fields[73]
                        base_On_Order_Qty3 = row_fields[74]
                        base_On_Order_Qty3_Used = row_fields[75]
                        base_On_Order_Date3 = row_fields[76]
                        base_Total_PO_Qty = row_fields[77]
                        base_image_name = base_ISBN + ".png"

                        base_ISBN = base_ISBN.strip('0')

                        logger.debug(
                            "base_GenKey {}, base_FormatFlag {}, base_Author {}, base_Title {}, base_ISBN {}, base_ISBN_HR {}, base_Vendor_Style {}, base_Publisher {}, base_Imprint {}, base_Edition {}, base_Edition_Status {}, base_New_Price {}, base_New_Price_Text {}, base_Used_Price {}, base_Used_Price_Text {}, base_New_Rental_Price {}, base_New_Rental_Price_Text {}, base_Ebook_Price {}, base_Ebook_Price_Text {}, base_Used_Rental_Price {}, base_Used_Rental_Price_Text {}, base_Sale_Price1 {}, base_Sale_Start_Date1 {}, base_Sale_End_Date1 {}, base_Sale_Start_Time1 {}, base_Sale_End_Time1 {}, base_Sale_Price2 {}, base_Sale_Start_Date2 {}, base_Sale_End_Date2 {}, base_Sale_Start_Time2 {}, base_Sale_End_Time2 {}, base_Sale_Price3 {}, base_Sale_Start_Date3 {}, base_Sale_End_Date3 {}, base_Sale_Start_Time3 {}, base_Sale_End_Time3 {}, base_Sale_Price4 {}, base_Sale_Start_Date4 {}, base_Sale_End_Date4 {}, base_Sale_Start_Time4 {}, base_Sale_End_Time4 {}, base_Term {}, base_Term_Description {}, base_Requested_Qty {}, base_Class_Capacity_Qty {}, base_Actual_Enrollment_Qty {}, base_Est_Sales_Qty {}, base_Category {}, base_Division {}, base_Department {}, base_Class {}, base_New_Store_Qty {}, base_New_Warehouse_Qty {}, base_Used_Store_Qty {}, base_Used_Warehouse_Qty {}, base_New_Pending_Return_Qty {}, base_Used_Pending_Return_Qty {}, base_New_insite_Pending_Order {}, base_Used_insite_Pending_Order {}, base_New_Rental_insite_Pending_Order {}, base_Used_Rental_insite_Pending_Order {}, base_On_Order_PO1 {}, base_On_Order_PO1_Vendor {}, base_On_Order_Qty1 {}, base_On_Order_Qty1_Used {}, base_On_Order_Date1 {}, base_On_Order_PO2 {}, base_On_Order_PO2_Vendor {}, base_On_Order_Qty2 {}, base_On_Order_Qty2_Used {}, base_On_Order_Date2 {}, base_On_Order_PO3 {}, base_On_Order_PO3_Vendor {}, base_On_Order_Qty3 {}, base_On_Order_Qty3_Used {}, base_On_Order_Date3 {}, base_Total_PO_Qty {}, base_image_name {}".format(
                                base_GenKey, base_FormatFlag, base_Author, base_Title, base_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division,
                                base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name))

                        ###### Trim author and title to correct lenghts

                        base_Author = base_Author[0:254]
                        base_Title = base_Title[0:254]

                        ###format PO Dates

                        if date_format != "YYYYMMDD":

                            year_part1 = ''
                            year_part2 = ''
                            year_part3 = ''
                            month_part1 = ''
                            month_part2 = ''
                            month_part3 = ''
                            day_part1 = ''
                            day_part2 = ''
                            day_part3 = ''

                            if len(base_On_Order_Date1) == 8:
                                year_part1 = base_On_Order_Date1[0:4]
                                month_part1 = base_On_Order_Date1[4:6]
                                day_part1 = base_On_Order_Date1[6:8]
                            if len(base_On_Order_Date2) == 8:
                                year_part2 = base_On_Order_Date2[0:4]
                                month_part2 = base_On_Order_Date2[4:6]
                                day_part2 = base_On_Order_Date2[6:8]
                            if len(base_On_Order_Date3) == 8:
                                year_part3 = base_On_Order_Date3[0:4]
                                month_part3 = base_On_Order_Date3[4:6]
                                day_part3 = base_On_Order_Date3[6:8]

                        if date_format == "MM/DD/YYYY":
                            base_On_Order_Date1 = month_part1 + '/' + day_part1 + '/' + year_part1
                            base_On_Order_Date2 = month_part2 + '/' + day_part2 + '/' + year_part2
                            base_On_Order_Date3 = month_part3 + '/' + day_part3 + '/' + year_part3

                        if date_format == "DD/MM/YYYY":
                            base_On_Order_Date1 = day_part1 + '/' + month_part1 + '/' + year_part1
                            base_On_Order_Date2 = day_part2 + '/' + month_part2 + '/' + year_part2
                            base_On_Order_Date3 = day_part3 + '/' + month_part3 + '/' + year_part3

                        if date_format == "YYYY/MM/DD":
                            base_On_Order_Date1 = year_part1 + '/' + month_part1 + '/' + day_part1
                            base_On_Order_Date2 = year_part2 + '/' + month_part2 + '/' + day_part2
                            base_On_Order_Date3 = year_part3 + '/' + month_part3 + '/' + day_part3

                        if date_format == "DD/MM/YY":
                            base_On_Order_Date1 = day_part1 + '/' + month_part1 + '/' + year_part1[:-2]
                            base_On_Order_Date2 = day_part2 + '/' + month_part2 + '/' + year_part2[:-2]
                            base_On_Order_Date3 = day_part3 + '/' + month_part3 + '/' + year_part3[:-2]

                        if date_format == "MM/DD/YY":
                            base_On_Order_Date1 = month_part1 + '/' + day_part1 + '/' + year_part1[:-2]
                            base_On_Order_Date2 = month_part2 + '/' + day_part2 + '/' + year_part2[:-2]
                            base_On_Order_Date3 = month_part3 + '/' + day_part3 + '/' + year_part3[:-2]

                        if date_format == "MM-DD-YYYY":
                            base_On_Order_Date1 = month_part1 + '-' + day_part1 + '-' + year_part1
                            base_On_Order_Date2 = month_part2 + '-' + day_part2 + '-' + year_part2
                            base_On_Order_Date3 = month_part3 + '-' + day_part3 + '-' + year_part3

                        if date_format == "DD-MM-YYYY":
                            base_On_Order_Date1 = day_part1 + '-' + month_part1 + '-' + year_part1
                            base_On_Order_Date2 = day_part2 + '-' + month_part2 + '-' + year_part2
                            base_On_Order_Date3 = day_part3 + '-' + month_part3 + '-' + year_part3

                        if date_format == "DD-MM-YYYY":
                            base_On_Order_Date1 = day_part1 + '-' + month_part1 + '-' + year_part1
                            base_On_Order_Date2 = day_part2 + '-' + month_part2 + '-' + year_part2
                            base_On_Order_Date3 = day_part3 + '-' + month_part3 + '-' + year_part3

                        if date_format == "DD-MM-YY":
                            base_On_Order_Date1 = day_part1 + '-' + month_part1 + '-' + year_part1[:-2]
                            base_On_Order_Date2 = day_part2 + '-' + month_part2 + '-' + year_part2[:-2]
                            base_On_Order_Date3 = day_part3 + '-' + month_part3 + '-' + year_part3[:-2]

                        if date_format == "MM-DD-YY":
                            base_On_Order_Date1 = month_part1 + '-' + day_part1 + '-' + year_part1[:-2]
                            base_On_Order_Date2 = month_part2 + '-' + day_part2 + '-' + year_part2[:-2]
                            base_On_Order_Date3 = month_part3 + '-' + day_part3 + '-' + year_part3[:-2]

                        logger.debug("Date_format: {}, base_On_Order_Date1: {}, base_On_Order_Date2: {}, base_On_Order_Date3: {}.".format(date_format, base_On_Order_Date1, base_On_Order_Date2, base_On_Order_Date3))

                        ######process if valid ISBN/SKU
                        if base_ISBN is not None and base_ISBN != "":
                            logger.debug("Base ISBN: {}".format(base_ISBN))

                            base_Sale_Price = ''

                            if base_New_Price is None or base_New_Price == "":
                                base_New_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid NEW Price, price field setting to {}".format(base_ISBN, base_New_Price))

                            if base_Used_Price is None or base_Used_Price == "":
                                base_Used_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid USED Price, price field setting to {}".format(base_ISBN, base_Used_Price))

                            if base_New_Rental_Price is None or base_New_Rental_Price == "":
                                base_New_Rental_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid NEW RENTAL, price field setting to {}".format(base_ISBN, base_New_Rental_Price))

                            if base_Used_Rental_Price is None or base_Used_Rental_Price == "":
                                base_Used_Rental_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid USED RENTAL, field setting to {}".format(base_ISBN, base_Used_Rental_Price))

                            if base_Ebook_Price is None or base_Ebook_Price == "":
                                base_Ebook_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid EBOOK, price field setting to {}".format(base_ISBN, base_Ebook_Price))

                            if base_Sale_Price is None or base_Sale_Price == "":
                                base_Sale_Price = 0
                                logger.debug("Base ISBN: {}, has a invalid Sale Price, price field setting to {}".format(base_ISBN, base_Sale_Price))
                            
                            if base_Requested_Qty is None or base_Requested_Qty == "":
                                base_Requested_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Quantity field setting to {}".format(base_ISBN, base_Ebook_Price))

                            if base_New_Store_Qty is None or base_New_Store_Qty == "":
                                base_New_Store_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Requested Quantity, field setting to {}".format(base_ISBN, base_New_Store_Qty))

                            if base_New_Warehouse_Qty is None or base_New_Warehouse_Qty == "":
                                base_New_Warehouse_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid New Warehouse Quantity, field setting to {}".format(base_ISBN, base_New_Warehouse_Qty))

                            if base_Used_Store_Qty is None or base_Used_Store_Qty == "":
                                base_Used_Store_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Store Quantity, field setting to {}".format(base_ISBN, base_Used_Store_Qty))

                            if base_Used_Warehouse_Qty is None or base_Used_Warehouse_Qty == "":
                                base_Used_Warehouse_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Warehouse Quantity, field setting to {}".format(base_ISBN, base_Used_Warehouse_Qty))

                            if base_On_Order_Qty1 is None or base_On_Order_Qty1 == "":
                                base_On_Order_Qty1 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 1 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty1))

                            if base_On_Order_Qty2 is None or base_On_Order_Qty2 == "":
                                base_On_Order_Qty2 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 2 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty2))

                            if base_On_Order_Qty3 is None or base_On_Order_Qty3 == "":
                                base_On_Order_Qty3 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 3 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty3))

                            if base_Total_PO_Qty is None or base_Total_PO_Qty == "":
                                base_Total_PO_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Total PO Quantity, field setting to {}".format(base_ISBN, base_Total_PO_Qty))

                            if base_Class_Capacity_Qty is None or base_Class_Capacity_Qty == "":
                                base_Class_Capacity_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Class Capacity Quantity, field setting to {}".format(base_ISBN, base_Class_Capacity_Qty))

                            if base_Actual_Enrollment_Qty is None or base_Actual_Enrollment_Qty == "":
                                base_Actual_Enrollment_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Actual Enrollment Quantity, field setting to {}".format(base_ISBN, base_Actual_Enrollment_Qty))

                            if base_Est_Sales_Qty is None or base_Est_Sales_Qty == "":
                                base_Est_Sales_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Est Sales Quantity, field setting to {}".format(base_ISBN, base_Est_Sales_Qty))

                            if base_New_Pending_Return_Qty is None or base_New_Pending_Return_Qty == "":
                                base_New_Pending_Return_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid New Pending Return Quantity, field setting to {}".format(base_ISBN, base_New_Pending_Return_Qty))

                            if base_Used_Pending_Return_Qty is None or base_Used_Pending_Return_Qty == "":
                                base_Used_Pending_Return_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Pending Return Quantity, field setting to {}".format(base_ISBN, base_Used_Pending_Return_Qty))

                            if base_New_insite_Pending_Order is None or base_New_insite_Pending_Order == "":
                                base_New_insite_Pending_Order = 0
                                logger.debug("Base ISBN: {}, has a invalid New insite Pending Order Quantity, field setting to {}".format(base_ISBN, base_New_insite_Pending_Order))

                            if base_Used_insite_Pending_Order is None or base_Used_insite_Pending_Order == "":
                                base_Used_insite_Pending_Order = 0
                                logger.debug("Base ISBN: {}, has a invalid Used insite Pending Order Quantity, field setting to {}".format(base_ISBN, base_Used_insite_Pending_Order))

                            if base_On_Order_Qty1_Used is None or base_On_Order_Qty1_Used == "":
                                base_On_Order_Qty1_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 1 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty1_Used))

                            if base_On_Order_Qty2_Used is None or base_On_Order_Qty2_Used == "":
                                base_On_Order_Qty2_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 2 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty2_Used))

                            if base_On_Order_Qty3_Used is None or base_On_Order_Qty3_Used == "":
                                base_On_Order_Qty3_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 3 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty3_Used))
                                
                        logger.debug("base_ISBN: {}, base_New_Price: {}, base_Used_Price: {}, base_New_Rental_Price: {}, base_Ebook_Price: {}, base_Used_Rental_Price: {}, base_Requested_Qty: {}, base_New_Store_Qty: {}, base_New_Warehouse_Qty: {}, base_Used_Store_Qty: {}, base_Used_Warehouse_Qty: {}, base_On_Order_Qty1: {}, base_On_Order_Qty2: {}, base_On_Order_Qty3: {}, base_Total_PO_Qty: {}, base_Class_Capacity_Qty: {}, base_Actual_Enrollment_Qty: {}, base_Est_Sales_Qty: {}, base_Sale_Price: {}, base_New_Pending_Return_Qty: {}, base_Used_Pending_Return_Qty: {}, base_New_insite_Pending_Order: {}, base_Used_insite_Pending_Order: {}, base_On_Order_Qty1_Used: {}, base_On_Order_Qty2_Used: {}, base_On_Order_Qty3_Used: {}.".format(base_ISBN, base_New_Price, base_Used_Price, base_New_Rental_Price, base_Ebook_Price, base_Used_Rental_Price, base_Requested_Qty, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_On_Order_Qty1, base_On_Order_Qty2, base_On_Order_Qty3, base_Total_PO_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Sale_Price, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_On_Order_Qty1_Used, base_On_Order_Qty2_Used, base_On_Order_Qty3_Used))
                        ##### Start book record###

                        if base_Category.lower() == "t":
                            ### Determine base item IPF
                            ITEMIPF = 0
                            item_ipf_dict = {}


                            if float(base_New_Price) > 0:
                                ITEMIPF += 1
                                item_ipf_dict.update({(ITEMIPF): (base_New_Price, base_New_Price_Text)})

                            if float(base_Used_Price) > 0:
                                ITEMIPF += 1
                                item_ipf_dict.update({(ITEMIPF): (base_Used_Price, base_Used_Price_Text)})

                            if float(base_New_Rental_Price) > 0:
                                ITEMIPF += 1
                                item_ipf_dict.update({(ITEMIPF): (base_New_Rental_Price, base_New_Rental_Price_Text)})

                            if float(base_Used_Rental_Price) > 0:
                                ITEMIPF += 1
                                item_ipf_dict.update({(ITEMIPF): (base_Used_Rental_Price, base_Used_Rental_Price_Text)})

                            if float(base_Ebook_Price) > 0:
                                ITEMIPF += 1
                                item_ipf_dict.update({(ITEMIPF): (base_Ebook_Price, base_Ebook_Price_Text)})

                            logger.debug("base_Category : {}, ITEMIPF : {}, base_New_Price: {}, base_Used_Price: {}, base_Used_Rental_Price: {}, base_Used_Rental_Price: {}, base_Ebook_Price: {}, item_ipf_dict : {}.".format(base_Category, ITEMIPF, base_New_Price, base_Used_Price, base_New_Rental_Price, base_Used_Rental_Price, base_Ebook_Price, item_ipf_dict))

                            out_price = []
                            out_text = []
                            out_price_1 = 0
                            out_text_1 = ''
                            out_price_2 = 0
                            out_text_2 = ''
                            out_price_3 = 0
                            out_text_3 = ''
                            out_price_4 = 0
                            out_text_4 = ''
                            out_price_5 = 0
                            out_text_5 = ''

                            ######Ascending order output

                            if sort_order == "ascending":
                                item_ipf_dict = sorted(item_ipf_dict.items(), key=lambda kv: kv[0])

                                for item_ipf_key in item_ipf_dict:

                                    ITEMIPF = list(item_ipf_dict.keys())[-1] * 100
                                    out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                    out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 2:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 4:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 6:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 8:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])


                            ####### Natural order output

                            if sort_order == "natural":

                                for item_ipf_key in item_ipf_dict:

                                    ITEMIPF = list(item_ipf_dict.keys())[-1] * 100
                                    out_price_1 =item_ipf_dict.get(item_ipf_key)[0]
                                    out_text_1 =item_ipf_dict.get(item_ipf_key)[1]
                                    out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                    out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 2:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 4:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 6:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])
                                    if len(item_ipf_dict) > 8:
                                        out_price.append(item_ipf_dict.get(item_ipf_key)[0])
                                        out_text.append(item_ipf_dict.get(item_ipf_key)[1])

                            logger.debug('Using sort order {}, item_ipf_dict : {}'.format(sort_order, item_ipf_dict))
                            logger.debug('Current IPF {}, set for ISBN {}.'.format(ITEMIPF, base_ISBN))
                            logger.debug("out Price list: {}".format(out_price))
                            logger.debug("out Text list: {}".format(out_text))

                            ##### format all price fields if not none

                            if base_New_Price != '':
                                base_New_Price = "%.2f" % (float(base_New_Price) / 1)

                            if base_Used_Price != '':
                                base_Used_Price = "%.2f" % (float(base_Used_Price) / 1)

                            if base_New_Rental_Price != '':
                                base_New_Rental_Price = "%.2f" % (float(base_New_Rental_Price) / 1)

                            if base_Used_Rental_Price != '':
                                base_Used_Rental_Price = "%.2f" % (float(base_Used_Rental_Price) / 1)

                            if base_Ebook_Price != '':
                                base_Ebook_Price = "%.2f" % (float(base_Ebook_Price) / 1)

                            if base_Sale_Price != '':
                                base_Sale_Price = "%.2f" % (float(base_Sale_Price) / 1)

                            if isinstance(out_price_1, float):
                                out_price_1 = "%.2f" % (float(out_price_1) / 1)

                            if isinstance(out_price_2, float):
                                out_price_2 = "%.2f" % (float(out_price_2) / 1)

                            if isinstance(out_price_3, float):
                                out_price_3 = "%.2f" % (float(out_price_3) / 1)

                            if isinstance(out_price_4, float):
                                out_price_4 = "%.2f" % (float(out_price_4) / 1)

                            if isinstance(out_price_5, float):
                                out_price_5 = "%.2f" % (float(out_price_5) / 1)

                            ######## Format Quantities

                            if base_New_Store_Qty != '':
                                base_New_Store_Qty = "%d" % (float(base_New_Store_Qty) / 1)

                            if base_New_Warehouse_Qty != '':
                                base_New_Warehouse_Qty = "%.d" % (float(base_New_Warehouse_Qty) / 1)

                            if base_Used_Store_Qty != '':
                                base_Used_Store_Qty = "%.d" % (float(base_Used_Store_Qty) / 1)

                            if base_Used_Warehouse_Qty != '':
                                base_Used_Warehouse_Qty = "%.d" % (float(base_Used_Warehouse_Qty) / 1)

                            if base_On_Order_Qty1 != '':
                                base_On_Order_Qty1 = "%.d" % (float(base_On_Order_Qty1) / 1)

                            if base_On_Order_Qty2 != '':
                                base_On_Order_Qty2 = "%.d" % (float(base_On_Order_Qty2) / 1)

                            if base_On_Order_Qty3 != '':
                                base_On_Order_Qty3 = "%.d" % (float(base_On_Order_Qty3) / 1)

                            if base_Total_PO_Qty != '':
                                base_Total_PO_Qty = "%.d" % (float(base_Total_PO_Qty) / 1)

                            if base_Requested_Qty != '':
                                base_Requested_Qty = "%.d" % (float(base_Requested_Qty) / 1)

                            if base_Class_Capacity_Qty != '':
                                base_Class_Capacity_Qty = "%.d" % (float(base_Class_Capacity_Qty) / 1)

                            if base_Actual_Enrollment_Qty != '':
                                base_Actual_Enrollment_Qty = "%.d" % (float(base_Actual_Enrollment_Qty) / 1)

                            if base_Est_Sales_Qty != '':
                                base_Est_Sales_Qty = "%.d" % (float(base_Est_Sales_Qty) / 1)

                            if base_On_Order_Qty1_Used != '':
                                base_On_Order_Qty1_Used = "%.d" % (float(base_On_Order_Qty1_Used) / 1)

                            if base_On_Order_Qty2_Used != '':
                                base_On_Order_Qty2_Used = "%.d" % (float(base_On_Order_Qty2_Used) / 1)

                            if base_On_Order_Qty3_Used != '':
                                base_On_Order_Qty3_Used = "%.d" % (float(base_On_Order_Qty3_Used) / 1)

                            if base_New_Pending_Return_Qty != '':
                                base_New_Pending_Return_Qty = "%.d" % (float(base_New_Pending_Return_Qty) / 1)

                            if base_Used_Pending_Return_Qty != '':
                                base_Used_Pending_Return_Qty = "%.d" % (float(base_Used_Pending_Return_Qty) / 1)

                            if base_New_insite_Pending_Order != '':
                                base_New_insite_Pending_Order = "%.d" % (float(base_New_insite_Pending_Order) / 1)

                            if base_Used_insite_Pending_Order != '':
                                base_Used_insite_Pending_Order = "%.d" % (float(base_Used_insite_Pending_Order) / 1)

                            if base_New_Rental_insite_Pending_Order != '':
                                base_New_Rental_insite_Pending_Order = "%.d" % (float(base_New_Rental_insite_Pending_Order) / 1)

                            if base_Used_insite_Pending_Order != '':
                                base_Used_insite_Pending_Order = "%.d" % (float(base_Used_insite_Pending_Order) / 1)

                            ##### Calculate Total QOH

                            Total_QOH = 0

                            if New_On_Hand.lower() == "add":
                                Total_QOH = Total_QOH + int(base_New_Store_Qty)
                            elif New_On_Hand.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_New_Store_Qty)

                            if Used_On_Hand.lower() == "add":
                                Total_QOH = Total_QOH + int(base_Used_Store_Qty)
                            elif Used_On_Hand.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_Used_Store_Qty)

                            if New_Addl.lower() == "add":
                                Total_QOH = Total_QOH + int(base_New_Warehouse_Qty)
                            elif New_Addl.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_New_Warehouse_Qty)

                            if Used_Addl.lower() == "add":
                                Total_QOH = Total_QOH + int(base_Used_Warehouse_Qty)
                            elif Used_Addl.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_Used_Warehouse_Qty)

                            if New_Pend_Ret.lower() == "add":
                                Total_QOH = Total_QOH + int(base_New_Pending_Return_Qty)
                            elif New_Pend_Ret.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_New_Pending_Return_Qty)

                            if Used_Pend_Ret.lower() == "add":
                                Total_QOH = Total_QOH + int(base_Used_Pending_Return_Qty)
                            elif Used_Pend_Ret.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_Used_Pending_Return_Qty)

                            if New_insite_Pend_Ord.lower() == "add":
                                Total_QOH = Total_QOH + int(base_New_insite_Pending_Order)
                            elif New_insite_Pend_Ord.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_New_insite_Pending_Order)

                            if Used_insite_Pend_Ord.lower() == "add":
                                Total_QOH = Total_QOH + int(base_Used_insite_Pending_Order)
                            elif Used_insite_Pend_Ord.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_Used_insite_Pending_Order)

                            if New_Rental_insite_Pend_Ord.lower() == "add":
                                Total_QOH = Total_QOH + int(base_New_Rental_insite_Pending_Order)
                            elif New_Rental_insite_Pend_Ord.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_New_Rental_insite_Pending_Order)

                            if Used_Rental_insite_Pend_Ord.lower() == "add":
                                Total_QOH = Total_QOH + int(base_Used_Rental_insite_Pending_Order)
                            elif Used_Rental_insite_Pend_Ord.lower() == "subtract":
                                Total_QOH = Total_QOH - int(base_Used_Rental_insite_Pending_Order)

                            ####### handler for missing all prices

                            if ITEMIPF == 0 and none_100:
                                ITEMIPF = 100
                                logger.info("record {}, {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

                            elif ITEMIPF == 0 and not none_100 and str(base_ISBN).lower() == "ntr":
                                ITEMIPF = 104
                                logger.info("record {}, {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

                            ##### Check ipf1x5 threshold exceeded, if so assign IPF1X5
                            logger.debug("Out Prices 1-5, {}. IPF1X5 Threshold {}.".format(out_price, ipf1x5_threshold))
                            for i in out_price:
                                if float(i) > float(ipf1x5_threshold) > 0:
                                    ITEMIPF = 105
                                    logger.debug("This ISBN {}, has Prices {} greater than IPF threshold {}, setting ITEMIPF to {}.".format(base_ISBN, out_price, ipf1x5_threshold, ITEMIPF))

                            ##### ipf for none on hand but some on order

                            if Total_QOH <= 0 and int(base_Total_PO_Qty) > 0 and ITEMIPF != 104:
                               ITEMIPF = 101
                               logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                            ##### ipf for none on hand and none on order

                            if Total_QOH <= 0 and int(base_Total_PO_Qty) <= 0 and ITEMIPF != 104:
                                ITEMIPF = 102
                                logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                            ##### ipf for none on hand and none on order, but prefix 281 selected via IPF100 parameter

                            if Total_QOH <= 0 and int(base_Total_PO_Qty) <= 0 and IPF100 and base_ISBN[0:3] == 281:
                                ITEMIPF = 100
                                logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                            ##### ipf for ISBN = "NTR"

                            if str(base_ISBN).lower() == "ntr":
                                ITEMIPF = 106
                                logger.debug("This ISBN {}, is flagged as NTR setting ITEMIPF to {}.".format(base_ISBN, ITEMIPF))

                            ##### go to IPF x5x series for store format "D" (organized by Dept/Course/Section) vs "A", which is alpha by author/title

                            if str(base_FormatFlag).lower() == "d":
                                ITEMIPF = ITEMIPF + 50
                            logger.debug("This ISBN {}, is flagged for formating with {}. Incrementing ITEMIPF by 50".format(base_ISBN, base_FormatFlag))
                            ##### check for digital edition, set base IPF at 700

                            if str(base_ISBN_HR).lower() == "dgt":
                                ITEMIPF = 700
                                logger.debug("This ISBN {}, is only digital setting ITEMIPF to {}.".format(base_ISBN, ITEMIPF))
                            ##### Always create base ISBN record
                            itemid = base_ISBN
                            logger.debug("{} This ISBN {}, has an ITEMIPF of {}.".format(row_counter, base_ISBN, ITEMIPF))

                        ##### get secondary info from secondary_data_file_list

                        sec_data_dict_list = list(filter(lambda kv: (kv["ISBN"] == base_ISBN and kv["Term"] == base_Term), secondary_file_data_list))
                        logger.debug(sec_data_dict_list)

                        for sec_data_dict in sec_data_dict_list:
                            sec_GenKey = sec_data_dict.get("GenKey")
                            sec_csn = sec_data_dict.get("csn")
                            sec_seq_no = sec_data_dict.get("seq_no")
                            sec_ISBN = sec_data_dict.get("ISBN")
                            sec_Term = sec_data_dict.get("Term")
                            sec_Term_Description = sec_data_dict.get("Term_Description")
                            sec_dept = sec_data_dict.get("dept")
                            sec_course = sec_data_dict.get("course")
                            sec_section = sec_data_dict.get("section")
                            sec_loc_code = sec_data_dict.get("loc_code")
                            sec_bookxofy = sec_data_dict.get("bookxofy")
                            sec_course_id = sec_data_dict.get("course_id")
                            sec_instructor = sec_data_dict.get("instructor")
                            sec_course_code = sec_data_dict.get("course_code")
                            sec_delete_flag = sec_data_dict.get("delete_flag")
                            sec_ebook_adopted = sec_data_dict.get("ebook_adopted")
                            sec_class_cap = sec_data_dict.get("class_cap")
                            sec_prof_requested = sec_data_dict.get("prof_requested")
                            sec_estimated_sales = sec_data_dict.get("estimated_sales")
                            sec_ebook1_vendor = sec_data_dict.get("ebook1_vendor")
                            sec_ebook1_period_1 = sec_data_dict.get("ebook1_period_1")
                            sec_ebook1_price_1 = sec_data_dict.get("ebook1_price_1")
                            sec_ebook1_period_2 = sec_data_dict.get("ebook1_period_2")
                            sec_ebook1_price_2 = sec_data_dict.get("ebook1_price_2")
                            sec_ebook1_period_3 = sec_data_dict.get("ebook1_period_3")
                            sec_ebook1_price_3 = sec_data_dict.get("ebook1_price_3")
                            sec_ebook1_period_4 = sec_data_dict.get("ebook1_period_4")
                            sec_ebook1_price_4 = sec_data_dict.get("ebook1_price_4")
                            sec_ebook1_period_5 = sec_data_dict.get("ebook1_period_5")
                            sec_ebook1_price_5 = sec_data_dict.get("ebook1_price_5")
                            sec_ebook2_vendor = sec_data_dict.get("ebook2_vendor")
                            sec_ebook2_period_1 = sec_data_dict.get("ebook2_period_1")
                            sec_ebook2_price_1 = sec_data_dict.get("ebook2_price_1")
                            sec_ebook2_period_2 = sec_data_dict.get("ebook2_period_2")
                            sec_ebook2_price_2 = sec_data_dict.get("ebook2_price_2")
                            sec_ebook2_period_3 = sec_data_dict.get("ebook2_period_3")
                            sec_ebook2_price_3 = sec_data_dict.get("ebook2_price_3")
                            sec_ebook2_period_4 = sec_data_dict.get("ebook2_period_4")
                            sec_ebook2_price_4 = sec_data_dict.get("ebook2_price_4")
                            sec_ebook2_period_5 = sec_data_dict.get("ebook2_period_5")
                            sec_ebook2_price_5 = sec_data_dict.get("ebook2_price_5")

                            logger.debug("sec_GenKey {}, sec_csn {}, sec_seq_no {}, sec_ISBN {}, sec_Term {}, sec_Term_Description {}, sec_dept {}, sec_course {}, sec_section {}, sec_loc_code {}, sec_bookxofy {}, sec_course_id {}, sec_instructor {}, sec_course_code {}, sec_delete_flag {}, sec_ebook_adopted {}, sec_class_cap {}, sec_prof_requested {}, sec_estimated_sales {}, sec_ebook1_vendor {}, sec_ebook1_period_1 {}, sec_ebook1_price_1 {}, sec_ebook1_period_2 {}, sec_ebook1_price_2 {}, sec_ebook1_period_3 {}, sec_ebook1_price_3 {}, sec_ebook1_period_4 {}, sec_ebook1_price_4 {}, sec_ebook1_period_5 {}, sec_ebook1_price_5 {}, sec_ebook2_vendor {}, sec_ebook2_period_1 {}, sec_ebook2_price_1 {}, sec_ebook2_period_2 {}, sec_ebook2_price_2 {}, sec_ebook2_period_3 {}, sec_ebook2_price_3 {}, sec_ebook2_period_4 {}, sec_ebook2_price_4 {}, sec_ebook2_period_5 {}, sec_ebook2_price_5 {}.".format(sec_GenKey, sec_csn, sec_seq_no, sec_ISBN, sec_Term, sec_Term_Description, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_instructor, sec_course_code, sec_delete_flag, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5))

                            ### if DisplayOriginal False
                            if not display_original:
                                sec_section = sec_section.replace(",", section_commas)
                                sec_section = sec_section.replace("'", section_commas)
                                sec_section = sec_section.replace(" ", section_commas)

                                logger.debug("Cleaning up section field.")

                            ####### Clean up Usages
                            usage = ""

                            if usage_convert:
                                if str(sec_course_code).lower() == "required":
                                    usage = "RQ"

                                if str(sec_course_code).lower() == "recommended":
                                    usage = "RC"

                                if str(sec_course_code).lower() == "optional":
                                    usage = "OP"

                                logger.debug("usage_convert is set to {}, sec_course_code {} has been read and usage has been assigned as {}.".format(usage_convert, sec_course_code, usage))

                            else:
                                usage = sec_course_code

                                logger.debug("usage_convert is set to {}, sec_course_code {} has been read and usage has been assigned as {}.".format(usage_convert, sec_course_code, usage))



                        if str(base_ISBN_HR).lower() != "dgt":
                            sec_ebook1_vendor = ""
                            sec_ebook1_period_1 = ""
                            sec_ebook1_price_1 = ""
                            sec_ebook1_period_2 = ""
                            sec_ebook1_price_2 = ""
                            sec_ebook1_period_3 = ""
                            sec_ebook1_price_3 = ""
                            sec_ebook1_period_4 = ""
                            sec_ebook1_price_4 = ""
                            sec_ebook1_period_5 = ""
                            sec_ebook1_price_5 = ""
                            sec_ebook2_vendor = ""
                            sec_ebook2_period_1 = ""
                            sec_ebook2_price_1 = ""
                            sec_ebook2_period_2 = ""
                            sec_ebook2_price_2 = ""
                            sec_ebook2_period_3 = ""
                            sec_ebook2_price_3 = ""
                            sec_ebook2_period_4 = ""
                            sec_ebook2_price_4 = ""
                            sec_ebook2_period_5 = ""
                            sec_ebook2_price_5 = ""

                        else:
                            if sec_ebook1_price_2 > 0 or sec_ebook2_price_2 > 0:
                                ITEMIPF = ITEMIPF + 1
                            if sec_ebook1_price_3 > 0 or sec_ebook2_price_3 > 0:
                                ITEMIPF = ITEMIPF + 1
                            if sec_ebook1_price_4 > 0 or sec_ebook2_price_4 > 0:
                                ITEMIPF = ITEMIPF + 1
                            if sec_ebook1_price_5 > 0 or sec_ebook2_price_5 > 0:
                                ITEMIPF = ITEMIPF + 1

                            if sec_ebook2_vendor != "":
                                ITEMIPF = ITEMIPF + 10

                        if use_pfi:
                            send_pfi(itemid, base_FormatFlag, base_Author, base_Title, base_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name,  sec_csn, sec_seq_no, sec_ISBN, sec_Term, sec_Term_Description, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_instructor, sec_course_code, sec_delete_flag, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)




    except:
        e = traceback.format_exc(limit=None, chain=True)
        AlertManager.alert_manager(config_path, log_path, 'fatal', "XML Import Processor crashed inelegantly with: " + str(e))
        logger.critical(file_name + " crashed inelegantly with: " + str(e))
        logger.info("---")
        sys.exit(99)

    total_time = str("%.3f" % (time.time() - origin_time))
    if float(total_time) < 60:
        logger.info("Execution time: " + str(total_time) + " seconds.")
    else:
        logger.info("Execution time: " + str("%.2f" % (float(total_time) / 60)) + " minutes.")
    logger.info("---")


###end main

#################################################start subroutines###########################################################
def file_handler(data_file):
    file_path = input_data_path + data_file

    # check file for stability
    current_size = 0
    new_size = os.path.getsize(file_path)
    while new_size > current_size:
        time.sleep(0.25)
        current_size = new_size
        new_size = os.path.getsize(file_path)

    file_content = open(input_data_path + data_file, "r", encoding='utf-8')

    return file_content

def send_pfi(OUTFILE, itemid, base_FormatFlag, base_Author, base_Title, base_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name,  sec_csn, sec_seq_no, sec_ISBN, sec_Term, sec_Term_Description, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_instructor, sec_course_code, sec_delete_flag, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5):

    #determine where to send output
    new_line = "0001 " + str(itemid) + \
               " 7 0 |" + str(base_Author) + " " + str(base_Title) + \
               "| 23 0 |" + "%.2f" % (float() / 1) + \
               "| 121 0 |" + str(ipf) + \
               "| 9800 0 |" + str(item_desc) + \
               "| 9801 0 |" + str(notes) + \
               "| 9802 0 |" + str(size) + \
               "| 9803 0 |" + "%.0f" % (float(case_quantity) / 1) + \
               "| 9804 0 |" + str(upc) + \
               "| 9805 0 |" + str(vin) + \
               "| 9806 0 |" + str(vintage) + \
               "| 9807 0 |" + str(container) + \
               "| 9808 0 |" + str(status) + \
               "| 9809 0 |" + str(alcohol_content) + \
               "| 9810 0 |" + str(department) + \
               "| 9811 0 |" + str(category) + \
               "| 9812 0 |" + str(sub_category) + \
               "| 9813 0 |" + str(producer) + \
               "| 9814 0 |" + str(supplier) + \
               "| 9815 0 |" + str(country) + \
               "| 9816 0 |" + str(region) + \
               "| 9817 0 |" + str(appellation) + \
               "| 9818 0 |" + str(varietal) + \
               "| 9819 0 |" + str(vendor) + \
               "| 9820 0 |" + str(vendor_code) + \
               "| 9821 0 |" + str(locationid) + \
               "| 9822 0 |" + "%.2f" % (float(case_price) / 1) + \
               "| 9823 0 |" + "%.0f" % (float(qtyonhand) / 1) + \
               "| 9824 0 |" + "%.0f" % (float(qtyonorder) / 1) + \
               "| 9825 0 |" + "%.0f" % (float(qtyavailable) / 1) + \
               "| 9826 0 |" + str(deleted) + \
               "| 9827 0 |" + "%.2f" % (float(pdiscountamount) / 1) + \
               "| 9828 0 |" + "%.1f" % (float(pdiscountpercent) / 1) + \
               "| 9829 0 |" + "%.0f" % (float(pminimumquantity) / 1) + \
               "| 9830 0 |" + "%.2f" % (float(sale_price) / 1) + \
               "| 9831 0 |" + str(penddate) + \
               "| 9832 0 |" + str(pstartdate) + \
               "| 9833 0 |" + "%.0f" % (float(pack_quantity) / 1) + \
               "| 9834 0 |" + str(sale_end) + \
               "| 9835 0 |" + str(aisle) + \
               "| 9836 0 |" + str(bin) + \
               "| 9837 0 |" + "%.0f" % (float(soldllast_30) / 1) + \
               "| 9838 0 |" + "%.0f" % (float(sold_last_60) / 1) + \
               "| 9839 0 |" + "%.0f" % (float(sold_last_90) / 1) + \
               "| 9840 0 |" + str(pdescription) + \
               "| 9841 0 |" + str(promotionid) + \
               "| 9842 0 |" + str(lastupdated)

    new_line = new_line + "|,\n"

    OUTFILE.write(new_line)

    return OUTFILE

def send_api(use_soap, json_outstring, rest_responses, page_line_count, api_out_page_count, patch_header, rest_api_url, soap_update_str, api_req_counter, soap_command_id, command_id, api_out_line_count):

    if not use_soap:
        #REST API
        item_out_json = {}
        api_out_line_count +=1

        item_out_json.update({"AISLE": str(aisle),
            "ALCOHOL_CONTENT": str(alcohol_content),
            "APPELLATION": str(appellation),
            "BIN": str(bin),
            "CASEQUANTITY": "%.0f" % (float(case_quantity) / 1),
            "CASE_PRICE": "%.2f" % (float(case_price) / 1),
            "CASE_QUANTITY": "%.0f" % (float(pack_quantity) / 1),
            "CATEGORY": str(category),
            "CONTAINER": str(container),
            "COUNTRY": str(country),
            "DELETED": str(deleted),
            "DESCRIPTION": str(item_desc),
            "ENDDATE": str(penddate),
            "LAST_UPDATED": str(lastupdated),
            "LOCATIONID": str(locationid),
            "MINIMUMQTY": "%.0f" % (float(pminimumquantity) / 1),
            "NOTES": str(notes),
            "PDESCRIPTION": str(pdescription),
            "PDISCOUNTAMOUNT": "%.2f" % (float(pdiscountamount) / 1),
            "PDISCOUNTPERCENT": "%.1f" % (float(pdiscountpercent) / 1),
            "PRODUCER": str(producer),
            "PROMOTION_ID": str(promotionid),
            "QTYAVAILABLE": "%.0f" % (float(qtyavailable) / 1),
            "QTYONHAND": "%.0f" % (float(qtyonhand) / 1),
            "QTYONORDER": "%.0f" % (float(qtyonorder) / 1),
            "REGION": str(region),
            "SALE_END": str(sale_end),
            "SALE_PRICE": "%.2f" % (float(sale_price) / 1),
            "SIZE": str(size),
            "SOLDLAST_30": "%.0f" % (float(soldllast_30) / 1),
            "SOLDLAST_60": "%.0f" % (float(sold_last_60) / 1),
            "SOLDLAST_90": "%.0f" % (float(sold_last_90) / 1),
            "STARTDATE": str(pstartdate),
            "STATUS": str(status),
            "SUB_CATEGORY": str(sub_category),
            "SUPPLIER": str(supplier),
            "UPC": str(upc),
            "VARIETAL": str(varietal),
            "VENDOR": str(vendor),
            "VENDOR_CODE": str(vendor_code),
            "VIN": str(vin),
            "VINTAGE": str(vintage),
            "API_ORIGIN": str(loop_page_count)})


        json_outstring.append({"department": str(department),
            "itemId": str(skunumber),
            "itemName": str(item_name),
            "presentation": str(ipf),
            "price": "%.2f" % (float(price) / 1),
            "properties": item_out_json})

        page_line_count += 1

        if page_line_count == api_page_count:
            if len(json_outstring) > 0:

                # execute rest request and store responseid in list
                logger.info('Sending data page ' + str(api_out_page_count) + ' with ' + str(api_page_count) + ' item rows to Pricer REST API.')
                response_id = (json.loads(requests.patch(rest_api_url, json=json_outstring, headers=patch_header).content).get('requestId'))
                rest_responses.append(response_id)
                logger.debug("ResponseId for last REST API resuest sent: " + str(response_id) + ".")

            else:
                logger.info('REST API had no data to send.')

            if float(api_out_line_count) != float(api_page_count):
                logger.critical("REST API failed to send expected " + str(api_page_count) + " records, " + str(api_out_line_count) + " sent.")

            api_out_page_count += 1
            page_line_count = 0
            json_outstring = []
            api_out_line_count = 0

    else:
        #SOAP API

        itemproperties_str = [{ 'id': {'number':  23 }, 'value':  "%.2f" % (float(price) / 1) }
                , {'id': {'number': 7}, 'value': str(item_name)}
                , {'id': {'number': 121}, 'value': str(ipf)}
                , {'id': {'number': 9800}, 'value': str(item_desc)}
                , {'id': {'number': 9801}, 'value': str(notes)}
                , {'id': {'number': 9802}, 'value': str(size)}
                , {'id': {'number': 9803}, 'value': "%.0f" % (float(case_quantity) / 1)}
                , {'id': {'number': 9804}, 'value': str(upc)}
                , {'id': {'number': 9805}, 'value': str(vin)}
                , {'id': {'number': 9806}, 'value': str(vintage)}
                , {'id': {'number': 9807}, 'value': str(container)}
                , {'id': {'number': 9808}, 'value': str(status)}
                , {'id': {'number': 9809}, 'value': str(alcohol_content)}
                , {'id': {'number': 9810}, 'value': str(department)}
                , {'id': {'number': 9811}, 'value': str(category)}
                , {'id': {'number': 9812}, 'value': str(sub_category)}
                , {'id': {'number': 9813}, 'value': str(producer)}
                , {'id': {'number': 9814}, 'value': str(supplier)}
                , {'id': {'number': 9815}, 'value': str(country)}
                , {'id': {'number': 9816}, 'value': str(region)}
                , {'id': {'number': 9817}, 'value': str(appellation)}
                , {'id': {'number': 9818}, 'value': str(varietal)}
                , {'id': {'number': 9819}, 'value': str(vendor)}
                , {'id': {'number': 9820}, 'value': str(vendor_code)}
                , {'id': {'number': 9821}, 'value': str(locationid)}
                , {'id': {'number': 9822}, 'value': "%.2f" % (float(case_price) / 1)}
                , {'id': {'number': 9823}, 'value': "%.0f" % (float(qtyonhand) / 1)}
                , {'id': {'number': 9824}, 'value': "%.0f" % (float(qtyonorder) / 1)}
                , {'id': {'number': 9825}, 'value': "%.0f" % (float(qtyavailable) / 1)}
                , {'id': {'number': 9826}, 'value': str(deleted)}
                , {'id': {'number': 9827}, 'value': "%.2f" % (float(pdiscountamount) / 1)}
                , {'id': {'number': 9828}, 'value': "%.1f" % (float(pdiscountpercent) / 1)}
                , {'id': {'number': 9829}, 'value': "%.0f" % (float(pminimumquantity) / 1)}
                , {'id': {'number': 9830}, 'value': "%.2f" % (float(sale_price) / 1)}
                , {'id': {'number': 9831}, 'value': str(penddate)}
                , {'id': {'number': 9832}, 'value': str(pstartdate)}
                , {'id': {'number': 9833}, 'value': "%.0f" % (float(pack_quantity) / 1)}
                , {'id': {'number': 9834}, 'value': str(sale_end)}
                , {'id': {'number': 9835}, 'value': str(aisle)}
                , {'id': {'number': 9836}, 'value': str(bin)}
                , {'id': {'number': 9837}, 'value': "%.0f" % (float(soldllast_30) / 1)}
                , {'id': {'number': 9838}, 'value': "%.0f" % (float(sold_last_60) / 1)}
                , {'id': {'number': 9839}, 'value': "%.0f" % (float(sold_last_90) / 1)}
                , {'id': {'number': 9840}, 'value': str(pdescription)}
                , {'id': {'number': 9841}, 'value': str(promotionid)}
                , {'id': {'number': 9842}, 'value': str(lastupdated)}
                , {'id': {'number': 9843}, 'value': str(loop_page_count)}]

        soap_update_str.append({'itemId': skunumber, 'itemProperties': itemproperties_str})

        page_line_count += 1
        api_out_line_count += 1

        if page_line_count == api_page_count:

            api_req_counter, soap_command_id, command_id, api_out_line_count = soap_api_updateitem(soap_update_str, soap_command_id, api_req_counter, api_out_line_count)

            logger.info('Command ID for last SOAP API request sent: ' + str(command_id))

            if float(api_out_line_count) != float(api_page_count):
                logger.critical("SOAP API failed to send expected " + str(api_page_count) + " records, " + str(api_out_line_count) + " sent.")

            api_out_page_count += 1
            page_line_count = 0
            soap_update_str = []
            api_out_line_count = 0

    return rest_responses, json_outstring, soap_update_str, api_req_counter, api_out_page_count, soap_command_id, command_id, page_line_count, api_out_line_count

def soap_api_updateitem(soap_update_str, soap_command_id, api_req_counter, api_out_line_count):

    good_content = 0

    if len(soap_update_str) > 0:
        command_id = client.service.updateItems(soap_update_str)
        api_req_counter += 1
        good_content = 1

        if command_id != None and command_id.isnumeric():
            soap_command_id.append(command_id)
        else:
            logger.critical("Last Update failed to return valid command id.")

    if good_content == 0:
        logger.info("No data to write to SOAP API.")

    return api_req_counter, soap_command_id, command_id, api_out_line_count

#################################################end subroutines#############################################################

if __name__ == '__main__':
    main()
