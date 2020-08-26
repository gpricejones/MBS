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
from collections import OrderedDict
from operator import getitem


def main():
    try:

        global file_name, drive_letter, use_pricer_level, log_level, log_delete_after, log_max, log_name, log_path, log_file, use_pricer_db, local_db, data_in_save, data_input_delete_after, input_data_path, wait_time, sort_order, none_100, usage_convert, IPF100, term_multi, date_format, ipf1x5_threshold, display_original, section_commas, New_On_Hand, Used_On_Hand, New_Addl, Used_Addl, New_Pend_Ret, Used_Pend_Ret, New_insite_Pend_Ord, Used_insite_Pend_Ord, New_Rental_insite_Pend_Ord, Used_Rental_insite_Pend_Ord, use_pfi, use_api, use_soap, i1_path, m1_path, r7_path, api_page_count, soap_api_ip, soap_token, soap_user, rest_api_url, rest_token
        global requests, db, client, config_path, license_status, pricer_api_alive, logger, origin_time, output_format, page_line_count, total_api_req_counter
        global out_price, out_text

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

        regular_price = 0
        save_amount = 0

        target_delay = ""
        target_date_start = ""
        target_date_end = ""

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
                output_format = "PFI FILE"
            else:
                use_pfi = False

            if (config.dataout.get('useapi')).lower() == "true":
                use_api = True
                output_format = "REST API"
            else:
                use_api = False

            if (config.dataout.get('usesoap')).lower() == "true":
                use_soap = True
                output_format = "SOAP API"
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

        ##### setup outfiles for pfi use

        if use_pfi:
            data_file = 'MBS_' + file_time
            i1_file = os.path.join(drive_letter, i1_path, data_file + ".i1")
            m1_file = os.path.join(drive_letter, m1_path, data_file + ".m1")
            r7_file = os.path.join(drive_letter, r7_path, data_file + ".r7")
            temp_m1_file = os.path.join(drive_letter, i1_path, data_file + ".m1")

            logger.debug("i1 file: {}".format(i1_file))

            OUTFILE = open(i1_file, "w")

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
            license_status = LicenseValidator.license_validator(drive_letter, type_id, log_file, log_path, log_level, config_path, use_pricer_db, local_db)

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

        #### get all itemids in t_links
        all_itemids = []

        db_sel = db.cursor()
        db_sel.callproc("sp_lookup_mbs_itemid", [])
        db.commit()
        data = db_sel.fetchall()
        for row in data:
            all_itemids.append(row[0])
        db_sel.close()

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


                if new_file.lower().endswith("tx1"):
                    logger.info("Start processing output from {} to dictionary".format(new_file))

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
                            secondary_file_dict.update({"ebook1_vendor": ""})
                            secondary_file_dict.update({"ebook1_period_1": ""})
                            secondary_file_dict.update({"ebook1_price_1": "0.00"})
                            secondary_file_dict.update({"ebook1_period_2": ""})
                            secondary_file_dict.update({"ebook1_price_2": "0.00"})
                            secondary_file_dict.update({"ebook1_period_3": ""})
                            secondary_file_dict.update({"ebook1_price_3": "0.00"})
                            secondary_file_dict.update({"ebook1_period_4": ""})
                            secondary_file_dict.update({"ebook1_price_4": "0.00"})
                            secondary_file_dict.update({"ebook1_period_5": ""})
                            secondary_file_dict.update({"ebook1_price_5": "0.00"})
                            secondary_file_dict.update({"ebook2_vendor": ""})
                            secondary_file_dict.update({"ebook2_period_1": ""})
                            secondary_file_dict.update({"ebook2_price_1": "0.00"})
                            secondary_file_dict.update({"ebook2_period_2": ""})
                            secondary_file_dict.update({"ebook2_price_2": "0.00"})
                            secondary_file_dict.update({"ebook2_period_3": ""})
                            secondary_file_dict.update({"ebook2_price_3": "0.00"})
                            secondary_file_dict.update({"ebook2_period_4": ""})
                            secondary_file_dict.update({"ebook2_price_4": "0.00"})
                            secondary_file_dict.update({"ebook2_period_5": ""})
                            secondary_file_dict.update({"ebook2_price_5": "0.00"})
                        if len(row_fields) >= 20:
                            if row_fields[19] != "":
                                secondary_file_dict.update({"ebook1_vendor": row_fields[19]})
                            else:
                                secondary_file_dict.update({"ebook1_vendor": ""})
                        if len(row_fields) >= 21:
                            if row_fields[20] != "":
                                secondary_file_dict.update({"ebook1_period_1": row_fields[20]})
                            else:
                                secondary_file_dict.update({"ebook1_period_1": ""})

                        if len(row_fields) >= 22:
                            ebook1_price_1 = row_fields[21]
                            ebook1_price_1 = "%.2f" % (float(ebook1_price_1) / 1)
                            if row_fields[21] != "":
                                secondary_file_dict.update({"ebook1_price_1": ebook1_price_1})
                            else:
                                secondary_file_dict.update({"ebook1_price_1": "0.00"})

                        if len(row_fields) >= 23:
                            if row_fields[22] != "":
                                secondary_file_dict.update({"ebook1_period_2": row_fields[22]})
                            else:
                                secondary_file_dict.update({"ebook1_period_2": ""})

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
                                secondary_file_dict.update({"ebook1_period_3": ""})

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
                                secondary_file_dict.update({"ebook1_period_4": ""})

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
                                secondary_file_dict.update({"ebook1_period_5": ""})

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
                                secondary_file_dict.update({"ebook2_vendor": ""})

                        if len(row_fields) >= 32:
                            if row_fields[31] != "":
                                secondary_file_dict.update({"ebook2_period_1": row_fields[31]})
                            else:
                                secondary_file_dict.update({"ebook2_period_1": ""})

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
                                secondary_file_dict.update({"ebook2_period_2": ""})

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
                                secondary_file_dict.update({"ebook2_period_3": ""})

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
                                secondary_file_dict.update({"ebook2_period_4": ""})

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
                                secondary_file_dict.update({"ebook2_period_5": ""})

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
                    logger.info("Start processing output from {} to {}".format(new_file, output_format))

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

                        if base_New_Rental_Price_Text.lower() != "new rental price":
                            base_New_Rental_Price_Text = "New Rental Price"

                        if base_Used_Rental_Price_Text.lower() != "used rental price":
                            base_Used_Rental_Price_Text = "Used Rental Price"

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

                            base_Sale_Price = 0

                            if not isinstance(float(base_New_Price), (int, float)):
                                base_New_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid NEW Price, price field setting to {}".format(base_ISBN, base_New_Price))

                            if not isinstance(float(base_Used_Price), (int, float)):
                                base_Used_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid USED Price, price field setting to {}".format(base_ISBN, base_Used_Price))

                            if not isinstance(float(base_New_Rental_Price), (int, float)):
                                base_New_Rental_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid NEW RENTAL, price field setting to {}".format(base_ISBN, base_New_Rental_Price))

                            if not isinstance(float(base_Used_Rental_Price), (int, float)):
                                base_Used_Rental_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid USED RENTAL, field setting to {}".format(base_ISBN, base_Used_Rental_Price))

                            if not isinstance(float(base_Ebook_Price), (int, float)):
                                base_Ebook_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid EBOOK, price field setting to {}".format(base_ISBN, base_Ebook_Price))

                            if not isinstance(float(base_Sale_Price), (int, float)):
                                base_Sale_Price = 0.00
                                logger.debug("Base ISBN: {}, has a invalid Sale Price, price field setting to {}".format(base_ISBN, base_Sale_Price))

                            if base_Requested_Qty == " " or not isinstance(int(base_Requested_Qty), int):
                                base_Requested_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Quantity field setting to {}".format(base_ISBN, base_Requested_Qty))

                            if base_New_Store_Qty == " " or not isinstance(int(base_New_Store_Qty), int):
                                base_New_Store_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Requested Quantity, field setting to {}".format(base_ISBN, base_New_Store_Qty))

                            if base_New_Warehouse_Qty == " " or not isinstance(int(base_New_Warehouse_Qty), int):
                                base_New_Warehouse_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid New Warehouse Quantity, field setting to {}".format(base_ISBN, base_New_Warehouse_Qty))

                            if base_Used_Store_Qty == " " or not isinstance(int(base_Used_Store_Qty), int):
                                base_Used_Store_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Store Quantity, field setting to {}".format(base_ISBN, base_Used_Store_Qty))

                            if base_Used_Warehouse_Qty == " " or not isinstance(int(base_Used_Warehouse_Qty), int):
                                base_Used_Warehouse_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Warehouse Quantity, field setting to {}".format(base_ISBN, base_Used_Warehouse_Qty))

                            if base_On_Order_Qty1 == " " or not isinstance(int(base_On_Order_Qty1), int):
                                base_On_Order_Qty1 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 1 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty1))

                            if base_On_Order_Qty2 == " " or not isinstance(int(base_On_Order_Qty2), int):
                                base_On_Order_Qty2 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 2 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty2))

                            if base_On_Order_Qty3 == " " or not isinstance(int(base_On_Order_Qty3), int):
                                base_On_Order_Qty3 = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order 3 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty3))

                            if base_Total_PO_Qty == " " or not isinstance(int(base_Total_PO_Qty), int):
                                base_Total_PO_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Total PO Quantity, field setting to {}".format(base_ISBN, base_Total_PO_Qty))

                            if base_Class_Capacity_Qty == " " or not isinstance(int(base_Class_Capacity_Qty), int):
                                base_Class_Capacity_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Class Capacity Quantity, field setting to {}".format(base_ISBN, base_Class_Capacity_Qty))

                            if base_Actual_Enrollment_Qty == " " or not isinstance(int(base_Actual_Enrollment_Qty), int):
                                base_Actual_Enrollment_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Actual Enrollment Quantity, field setting to {}".format(base_ISBN, base_Actual_Enrollment_Qty))

                            if base_Est_Sales_Qty == " " or not isinstance(int(base_Est_Sales_Qty), int):
                                base_Est_Sales_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Est Sales Quantity, field setting to {}".format(base_ISBN, base_Est_Sales_Qty))

                            if base_New_Pending_Return_Qty == " " or not isinstance(int(base_New_Pending_Return_Qty), int):
                                base_New_Pending_Return_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid New Pending Return Quantity, field setting to {}".format(base_ISBN, base_New_Pending_Return_Qty))

                            if base_Used_Pending_Return_Qty == " " or not isinstance(int(base_Used_Pending_Return_Qty), int):
                                base_Used_Pending_Return_Qty = 0
                                logger.debug("Base ISBN: {}, has a invalid Used Pending Return Quantity, field setting to {}".format(base_ISBN, base_Used_Pending_Return_Qty))

                            if base_New_insite_Pending_Order == " " or not isinstance(int(base_New_insite_Pending_Order), int):
                                base_New_insite_Pending_Order = 0
                                logger.debug("Base ISBN: {}, has a invalid New insite Pending Order Quantity, field setting to {}".format(base_ISBN, base_New_insite_Pending_Order))

                            if base_On_Order_Qty1 == " " or not isinstance(int(base_Used_insite_Pending_Order), int):
                                base_Used_insite_Pending_Order = 0
                                logger.debug("Base ISBN: {}, has a invalid Used insite Pending Order Quantity, field setting to {}".format(base_ISBN, base_Used_insite_Pending_Order))

                            if base_On_Order_Qty1_Used == " " or not isinstance(int(base_On_Order_Qty1_Used), int):
                                base_On_Order_Qty1_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 1 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty1_Used))

                            if base_On_Order_Qty2_Used == " " or not isinstance(int(base_On_Order_Qty2_Used), int):
                                base_On_Order_Qty2_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 2 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty2_Used))

                            if base_On_Order_Qty3_Used == " " or not isinstance(int(base_On_Order_Qty3_Used), int):
                                base_On_Order_Qty3_Used = 0
                                logger.debug("Base ISBN: {}, has a invalid On Order Used 3 Quantity, field setting to {}".format(base_ISBN, base_On_Order_Qty3_Used))

                            logger.debug("base_ISBN: {}, base_New_Price: {}, base_Used_Price: {}, base_New_Rental_Price: {}, base_Ebook_Price: {}, base_Used_Rental_Price: {}, base_Requested_Qty: {}, base_New_Store_Qty: {}, base_New_Warehouse_Qty: {}, base_Used_Store_Qty: {}, base_Used_Warehouse_Qty: {}, base_On_Order_Qty1: {}, base_On_Order_Qty2: {}, base_On_Order_Qty3: {}, base_Total_PO_Qty: {}, base_Class_Capacity_Qty: {}, base_Actual_Enrollment_Qty: {}, base_Est_Sales_Qty: {}, base_Sale_Price: {}, base_New_Pending_Return_Qty: {}, base_Used_Pending_Return_Qty: {}, base_New_insite_Pending_Order: {}, base_Used_insite_Pending_Order: {}, base_On_Order_Qty1_Used: {}, base_On_Order_Qty2_Used: {}, base_On_Order_Qty3_Used: {}.".format(base_ISBN, base_New_Price, base_Used_Price, base_New_Rental_Price, base_Ebook_Price, base_Used_Rental_Price, base_Requested_Qty, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_On_Order_Qty1, base_On_Order_Qty2, base_On_Order_Qty3, base_Total_PO_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Sale_Price, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_On_Order_Qty1_Used, base_On_Order_Qty2_Used, base_On_Order_Qty3_Used))
                            ##### Start book record###

                            if base_Category.lower() == "t":
                                ### Determine base item IPF
                                ITEMIPF = 0
                                item_ipf_dict = []


                                if float(base_New_Price) > 0:
                                    ITEMIPF += 1
                                    item_ipf_dict.append(Book(ITEMIPF, base_New_Price, base_New_Price_Text))

                                if float(base_Used_Price) > 0:
                                    ITEMIPF += 1
                                    item_ipf_dict.append(Book(ITEMIPF, base_Used_Price, base_Used_Price_Text))

                                if float(base_New_Rental_Price) > 0:
                                    ITEMIPF += 1
                                    item_ipf_dict.append(Book(ITEMIPF, base_New_Rental_Price, base_New_Rental_Price_Text))

                                if float(base_Used_Rental_Price) > 0:
                                    ITEMIPF += 1
                                    item_ipf_dict.append(Book(ITEMIPF, base_Used_Rental_Price, "Used Rental Price"))

                                if float(base_Ebook_Price) > 0:
                                    ITEMIPF += 1
                                    item_ipf_dict.append(Book(ITEMIPF, base_Ebook_Price, base_Ebook_Price_Text))

                                logger.debug("base_Category : {}, ITEMIPF : {}, base_New_Price: {}, base_Used_Price: {}, base_Used_Rental_Price: {}, base_Used_Rental_Price: {}, base_Ebook_Price: {}, item_ipf_dict : {}.".format(base_Category, ITEMIPF, base_New_Price, base_Used_Price, base_New_Rental_Price, base_Used_Rental_Price, base_Ebook_Price, item_ipf_dict))

                                out_price = []
                                out_text = []

                                class Book:
                                    def __init__(self, ipf, price, condition):
                                        self.ipf = ipf
                                        self.price = price
                                        self.condition = condition
                                        out_price.append(self.price)
                                        out_text.append(self.condition)

                                    def __repr__(self):
                                        return repr((self.ipf, self.price, self.condition))

                                ######Ascending order output

                                if sort_order == "ascending":
                                    if len(item_ipf_dict) > 0:
                                        ITEMIPF = len(item_ipf_dict) * 100

                                        sorted_item_ipf = sorted(item_ipf_dict, key=lambda book: book.condition)

                                        for item_ipf_key in sorted_item_ipf:
                                            out_price.append(item_ipf_key.price)
                                            out_text.append(item_ipf_key.condition)

                                        logger.debug("Sorted IPF List: {}.".format(sorted_item_ipf))

                                ####### Natural order output

                                if sort_order == "natural":
                                    if len(item_ipf_dict) > 0:
                                        ITEMIPF = len(item_ipf_dict) * 100

                                        sorted_item_ipf = sorted(item_ipf_dict, key=lambda book: book.ipf)

                                        for item_ipf_key in sorted_item_ipf:
                                            out_price.append(item_ipf_key.price)
                                            out_text.append(item_ipf_key.condition)

                                        logger.debug("Sorted IPF List: {}.".format(sorted_item_ipf))

                                ####### Price order output

                                if sort_order == "price":
                                    if len(item_ipf_dict) > 0:
                                        ITEMIPF = len(item_ipf_dict) * 100

                                        sorted_item_ipf = sorted(item_ipf_dict, key=lambda book: book.price)

                                        for item_ipf_key in sorted_item_ipf:
                                            out_price.append(item_ipf_key.price)
                                            out_text.append(item_ipf_key.condition)

                                        logger.debug("Sorted IPF List: {}.".format(sorted_item_ipf))

                                logger.debug('Using sort order {}, item_ipf_dict : {}'.format(sort_order, item_ipf_dict))
                                logger.debug('Current IPF {}, set for ISBN {}.'.format(ITEMIPF, base_ISBN))
                                logger.debug("out Price list: {}".format(out_price))
                                logger.debug("out Text list: {}".format(out_text))

                                ###### set out price values from out price list

                                if len(out_price) >= 1:
                                    out_price_1 = out_price[0]
                                else:
                                    out_price_1 = 0

                                if len(out_price) >= 2:
                                    out_price_2 = out_price[1]
                                else:
                                    out_price_2 = 0

                                if len(out_price) >= 3:
                                    out_price_3 = out_price[2]
                                else:
                                    out_price_3 = 0

                                if len(out_price) >= 4:
                                    out_price_4 = out_price[3]
                                else:
                                    out_price_4 = 0

                                if len(out_price) == 5:
                                    out_price_5 = out_price[4]
                                else:
                                    out_price_5 = 0

                                logger.debug("out_price 1: {}, out_price 2: {}, out_price 3: {}, out_price 4: {}, out_price 5: {}.".format(out_price_1, out_price_2, out_price_3, out_price_4, out_price_5))

                                ###### set out text values from out text list

                                if len(out_text) >= 1:
                                    out_text_1 = out_text[0]
                                else:
                                    out_text_1 = ""

                                if len(out_text) >= 2:
                                    out_text_2 = out_text[1]
                                else:
                                    out_text_2 = ""

                                if len(out_text) >= 3:
                                    out_text_3 = out_text[2]
                                else:
                                    out_text_3 = ""

                                if len(out_text) >= 4:
                                    out_text_4 = out_text[3]
                                else:
                                    out_text_4 = ""

                                if len(out_text) == 5:
                                    out_text_5 = out_text[4]
                                else:
                                    out_text_5 = ""

                                logger.debug("out_text 1: {}, out_text 2: {}, out_text 3: {}, out_text 4: {}, out_text 5: {}.".format(out_text_1, out_text_2, out_text_3, out_text_4, out_text_5))

                                ##### format all price fields if not none

                                if isinstance(base_New_Price, (int, float)):
                                    base_New_Price = "%.2f" % (float(base_New_Price) / 1)

                                if isinstance(base_Used_Price, (int, float)):
                                    base_Used_Price = "%.2f" % (float(base_Used_Price) / 1)

                                if isinstance(base_New_Rental_Price, (int, float)):
                                    base_New_Rental_Price = "%.2f" % (float(base_New_Rental_Price) / 1)

                                if isinstance(base_Used_Rental_Price, (int, float)):
                                    base_Used_Rental_Price = "%.2f" % (float(base_Used_Rental_Price) / 1)

                                if isinstance(base_Ebook_Price, (int, float)):
                                    base_Ebook_Price = "%.2f" % (float(base_Ebook_Price) / 1)

                                if isinstance(base_Sale_Price, (int, float)):
                                    base_Sale_Price = "%.2f" % (float(base_Sale_Price) / 1)

                                if isinstance(out_text_1, (int, float)):
                                    out_price_1 = "%.2f" % (float(out_price_1) / 1)

                                if isinstance(out_price_2, (int, float)):
                                    out_price_2 = "%.2f" % (float(out_price_2) / 1)

                                if isinstance(out_price_3, (int, float)):
                                    out_price_3 = "%.2f" % (float(out_price_3) / 1)

                                if isinstance(out_price_4, (int, float)):
                                    out_price_4 = "%.2f" % (float(out_price_4) / 1)

                                if isinstance(out_price_5, (int, float)):
                                    out_price_5 = "%.2f" % (float(out_price_5) / 1)

                                ######## Format Quantities

                                if isinstance(base_New_Store_Qty, (int, float)):
                                    base_New_Store_Qty = "%d" % (float(base_New_Store_Qty) / 1)

                                if isinstance(base_New_Warehouse_Qty, (int, float)):
                                    base_New_Warehouse_Qty = "%.d" % (float(base_New_Warehouse_Qty) / 1)

                                if isinstance(base_Used_Store_Qty, (int, float)):
                                    base_Used_Store_Qty = "%.d" % (float(base_Used_Store_Qty) / 1)

                                if isinstance(base_Used_Warehouse_Qty, (int, float)):
                                    base_Used_Warehouse_Qty = "%.d" % (float(base_Used_Warehouse_Qty) / 1)

                                if isinstance(base_On_Order_Qty1, (int, float)):
                                    base_On_Order_Qty1 = "%.d" % (float(base_On_Order_Qty1) / 1)

                                if isinstance(base_On_Order_Qty2, (int, float)):
                                    base_On_Order_Qty2 = "%.d" % (float(base_On_Order_Qty2) / 1)

                                if isinstance(base_On_Order_Qty3, (int, float)):
                                    base_On_Order_Qty3 = "%.d" % (float(base_On_Order_Qty3) / 1)

                                if isinstance(base_Total_PO_Qty, (int, float)):
                                    base_Total_PO_Qty = "%.d" % (float(base_Total_PO_Qty) / 1)

                                if isinstance(base_Requested_Qty, (int, float)):
                                    base_Requested_Qty = "%.d" % (float(base_Requested_Qty) / 1)

                                if isinstance(base_Class_Capacity_Qty, (int, float)):
                                    base_Class_Capacity_Qty = "%.d" % (float(base_Class_Capacity_Qty) / 1)

                                if isinstance(base_Actual_Enrollment_Qty, (int, float)):
                                    base_Actual_Enrollment_Qty = "%.d" % (float(base_Actual_Enrollment_Qty) / 1)

                                if isinstance(base_Est_Sales_Qty, (int, float)):
                                    base_Est_Sales_Qty = "%.d" % (float(base_Est_Sales_Qty) / 1)

                                if isinstance(base_On_Order_Qty1_Used, (int, float)):
                                    base_On_Order_Qty1_Used = "%.d" % (float(base_On_Order_Qty1_Used) / 1)

                                if isinstance(base_On_Order_Qty2_Used, (int, float)):
                                    base_On_Order_Qty2_Used = "%.d" % (float(base_On_Order_Qty2_Used) / 1)

                                if isinstance(base_On_Order_Qty3_Used, (int, float)):
                                    base_On_Order_Qty3_Used = "%.d" % (float(base_On_Order_Qty3_Used) / 1)

                                if isinstance(base_New_Pending_Return_Qty, (int, float)):
                                    base_New_Pending_Return_Qty = "%.d" % (float(base_New_Pending_Return_Qty) / 1)

                                if isinstance(base_Used_Pending_Return_Qty, (int, float)):
                                    base_Used_Pending_Return_Qty = "%.d" % (float(base_Used_Pending_Return_Qty) / 1)

                                if isinstance(base_New_insite_Pending_Order, (int, float)):
                                    base_New_insite_Pending_Order = "%.d" % (float(base_New_insite_Pending_Order) / 1)

                                if isinstance(base_Used_insite_Pending_Order, (int, float)):
                                    base_Used_insite_Pending_Order = "%.d" % (float(base_Used_insite_Pending_Order) / 1)

                                if isinstance(base_New_Rental_insite_Pending_Order, (int, float)):
                                    base_New_Rental_insite_Pending_Order = "%.d" % (float(base_New_Rental_insite_Pending_Order) / 1)

                                if isinstance(base_Used_insite_Pending_Order, (int, float)):
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
                                    logger.info("record on line {},with ISBN of {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

                                elif ITEMIPF == 0 and not none_100 and str(base_ISBN).lower() == "ntr":
                                    ITEMIPF = 104
                                    logger.info("record on line {},with ISBN of {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

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

                            if base_Category.lower() == "gm":
                                sale_start1 = "1900-01-01 00:00";
                                sale_end1 = "1900-01-01 00:00:00";
                                sale_start2 = "1900-01-01 00:00:00";
                                sale_end2 = "1900-01-01 00:00:00";
                                sale_start3 = "1900-01-01 00:00:00";
                                sale_end3 = "1900-01-01 00:00:00";
                                sale_start4 = "1900-01-01 00:00:00";
                                sale_end4 = "1900-01-01 00:00:00";

                                if len(base_Sale_Start_Date1) == 8 and len(base_Sale_Start_Time1) == 4:
                                    sale_start1 = base_Sale_Start_Date1[0:4] + "-" + base_Sale_Start_Date1[4:6] + "-" + base_Sale_Start_Date1[6:8] + " " + base_Sale_Start_Time1[0:2] + ":" + base_Sale_Start_Time1[2:4] + ":00"
                                if len(base_Sale_End_Date1) == 8 and len(base_Sale_End_Time1) == 4:
                                    sale_end1 = base_Sale_End_Date1[0:4] + "-" + base_Sale_End_Date1[4:6] + "-" + base_Sale_End_Date1[6:8] + " " + base_Sale_End_Time1[0:2] + ":" + base_Sale_End_Time1[2:4] + ":00"

                                if len(base_Sale_Start_Date2) == 8 and len(base_Sale_Start_Time2) == 4:
                                    sale_start2 = base_Sale_Start_Date2[0:4] + "-" + base_Sale_Start_Date2[4:6] + "-" + base_Sale_Start_Date2[6:8] + " " + base_Sale_Start_Time2[0:2] + ":" + base_Sale_Start_Time2[2:4] + ":00"
                                if len(base_Sale_End_Date2) == 8 and len(base_Sale_End_Time2) == 4:
                                    sale_end2 = base_Sale_End_Date2[0:4] + "-" + base_Sale_End_Date2[4:6] + "-" + base_Sale_End_Date2[6:8] + " " + base_Sale_End_Time2[0:2] + ":" + base_Sale_End_Time2[2:4] + ":00"

                                if len(base_Sale_Start_Date3) == 8 and len(base_Sale_Start_Time3) == 4:
                                    sale_start3 = base_Sale_Start_Date3[0:4] + "-" + base_Sale_Start_Date3[4:6] + "-" + base_Sale_Start_Date3[6:8] + " " + base_Sale_Start_Time3[0:2] + ":" + base_Sale_Start_Time3[2:4] + ":00"
                                if len(base_Sale_End_Date3) == 8 and len(base_Sale_End_Time3) == 4:
                                    sale_end3 = base_Sale_End_Date3[0:4] + "-" + base_Sale_End_Date3[4:6] + "-" + base_Sale_End_Date3[6:8] + " " + base_Sale_End_Time3[0:2] + ":" + base_Sale_End_Time3[2:4] + ":00"

                                if len(base_Sale_Start_Date4) == 8 and len(base_Sale_Start_Time4) == 4:
                                    sale_start4 = base_Sale_Start_Date4[0:4] + "-" + base_Sale_Start_Date4[4:6] + "-" + base_Sale_Start_Date4[6:8] + " " + base_Sale_Start_Time4[0:2] + ":" + base_Sale_Start_Time4[2:4] + ":00"
                                if len(base_Sale_End_Date4) == 8 and len(base_Sale_End_Time4) == 4:
                                    sale_end4 = base_Sale_End_Date4[0:4] + "-" + base_Sale_End_Date4[4:6] + "-" + base_Sale_End_Date4[6:8] + " " + base_Sale_End_Time4[0:2] + ":" + base_Sale_End_Time4[2:4] + ":00"

                                logger.debug("Sale Start 1: {}, Sale End 1: {}, Sale Start 2: {}, Sale End 2: {}, Sale Start 3: {}, Sale End 3: {}, Sale Start 4: {}, Sale End 4: {}.".format(sale_start1, sale_end1, sale_start2, sale_end2, sale_start3, sale_end3, sale_start4, sale_end4 ))

                                base_Sale_Price = 0
                                save_amount = 0
                                itemid = base_ISBN
                                regular_price = base_New_Price
                                ITEMIPF = "Regular"

                                if isinstance(regular_price, (int, float)):
                                    regular_price = "%.2f" % (float(regular_price) / 1)

                                if float(base_Sale_Price1) > 0 and int(sale_start1) > '1900-01-01 00:00:00':

                                    if s_stamp >= sale_start1 and s_stamp <= sale_end1:
                                        save_amount = regular_price - base_Sale_Price1
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price1) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start1
                                        target_date_end = sale_end1

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)


                                    elif sale_start1 >= s_stamp and sale_end1 >= s_stamp:
                                        save_amount = regular_price - base_Sale_Price1
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price1) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start1
                                        target_date_end = sale_end1
                                        target_delay = sale_start1

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                        ### Sale END
                                        ITEMIPF = "Regular"
                                        save_amount = 0
                                        target_delay = sale_end1

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                if float(base_Sale_Price2) > 0 and int(sale_start2) > '1900-01-01 00:00:00':

                                    if s_stamp >= sale_start2 and s_stamp <= sale_end2:
                                        save_amount = regular_price - base_Sale_Price2
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price2) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start2
                                        target_date_end = sale_end2

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)


                                    elif sale_start1 >= s_stamp and sale_end1 >= s_stamp:
                                        save_amount = regular_price - base_Sale_Price2
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price2) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start2
                                        target_date_end = sale_end2
                                        target_delay = sale_start2

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                        ### Sale END
                                        ITEMIPF = "Regular"
                                        save_amount = 0
                                        target_delay = sale_end2

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                if float(base_Sale_Price3) > 0 and int(sale_start3) > '1900-01-01 00:00:00':

                                    if s_stamp >= sale_start3 and s_stamp <= sale_end3:
                                        save_amount = regular_price - base_Sale_Price3
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price3) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start3
                                        target_date_end = sale_end3

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)


                                    elif sale_start3 >= s_stamp and sale_end3 >= s_stamp:
                                        save_amount = regular_price - base_Sale_Price3
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price3) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start3
                                        target_date_end = sale_end3
                                        target_delay = sale_start3

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                        ### Sale END
                                        ITEMIPF = "Regular"
                                        save_amount = 0
                                        target_delay = sale_end3

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                if float(base_Sale_Price4) > 0 and int(sale_start4) > '1900-01-01 00:00:00':

                                    if s_stamp >= sale_start4 and s_stamp <= sale_end4:
                                        save_amount = regular_price - base_Sale_Price4
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price4) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start4
                                        target_date_end = sale_end4

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)


                                    elif sale_start4 >= s_stamp and sale_end1 >= s_stamp:
                                        save_amount = regular_price - base_Sale_Price4
                                        ITEMIPF = "Sale"

                                        base_Sale_Price = "%.2f" % (float(base_Sale_Price4) / 1)
                                        save_amount = "%.2f" % (float(save_amount) / 1)
                                        target_date_start = sale_start4
                                        target_date_end = sale_end4
                                        target_delay = sale_start4

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                        ### Sale END
                                        ITEMIPF = "Regular"
                                        save_amount = 0
                                        target_delay = sale_end4

                                        if use_pfi:
                                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                               sec_dept,
                                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                if use_pfi:
                                    OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                       base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                       sec_dept,
                                                       sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                            if base_Category.lower() != "gm" and base_Category.lower() != "t":
                                logger.warn("Row {} with a ISBN record {} in {} failed Category decision with {}, no record output.".format(row_counter, base_ISBN, file_name, base_Category))
                        else:
                            logger.warn("Row {} dropped, invalid ISBN/SKU.".format(row_counter))

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

                            course_hold = sec_dept + "-" + sec_course + "-" + sec_section + "-" + base_Term

                            logger.debug("Course hold: {}.".format(course_hold))
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

                            ##### set course list and dept course selection
                            sec_course_list = []
                            sec_dept_course_section = []

                            sec_dept_course_section.append(sec_dept + ', ' + sec_course + ', ' + usage)
                            sec_course_list.append(sec_dept + ', ' + sec_course + ', ' + sec_section + ', ' + usage)
                            # trim to 1024 max length
                            sec_dept_course_section = sec_dept_course_section[0:1024]
                            sec_course_list = sec_course_list[0:1024]

                            logger.debug("sec_dept_course_section: {}.".format(sec_dept_course_section))
                            logger.debug("sec_course_list: {}.".format(sec_course_list))
                        # check for digital edition
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
                        # it is a digital edition
                        else:
                            # base 1 price IPF =701, 2 price = 702, 3 price = 703, 4 price = 704, 5 price = 705
                            if int(sec_ebook1_price_2) > 0 or int(sec_ebook2_price_2) > 0:
                                ITEMIPF = ITEMIPF + 1
                            if int(sec_ebook1_price_3) > 0 or int(sec_ebook2_price_3) > 0:
                                ITEMIPF = ITEMIPF + 1
                            if int(sec_ebook1_price_4) > 0 or int(sec_ebook2_price_4) > 0:
                                ITEMIPF = ITEMIPF + 1
                            if int(sec_ebook1_price_5) > 0 or int(sec_ebook2_price_5) > 0:
                                ITEMIPF = ITEMIPF + 1

                            # check for 2 vendors, bump to IPF 71x series
                            if sec_ebook2_vendor != "":
                                ITEMIPF = ITEMIPF + 10

                        if use_pfi:
                            OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                               base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage, sec_dept,
                                               sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                            if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                itemid = base_Used_ISBN

                                if use_pfi:
                                    OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                       base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                       sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                                itemid = base_ISBN

                        # check for multi-term, create additional records as needed.
                        if base_FormatFlag.lower() == "a" and term_multi:
                            logger.debug("processing flag format 'A' and term multi 'True'.")


                            if str(base_ISBN_HR).lower() == "dgt":

                                # base 1 price IPF =701, 2 price = 702, 3 price = 703, 4 price = 704, 5 price = 705
                                if float(sec_ebook1_price_2) > 0 or float(sec_ebook2_price_2) > 0:
                                    ITEMIPF = ITEMIPF + 1
                                if float(sec_ebook1_price_3) > 0 or float(sec_ebook2_price_3) > 0:
                                    ITEMIPF = ITEMIPF + 1
                                if float(sec_ebook1_price_4) > 0 or float(sec_ebook2_price_4) > 0:
                                    ITEMIPF = ITEMIPF + 1
                                if float(sec_ebook1_price_5) > 0 or float(sec_ebook2_price_5) > 0:
                                    ITEMIPF = ITEMIPF + 1
                                logger.debug("ISBN_HR is DGT, ITEMIPF has been incremented. ITEMIPF: {}.".format(ITEMIPF))
                                # check for 2 vendors, bump to IPF 71x series
                                if sec_ebook2_vendor != "":
                                    ITEMIPF = ITEMIPF + 10
                                    logger.debug("IIEMIPF has been incremented for second ebook vendor. ITEMIPF: {}.".format(ITEMIPF))

                                itemid = base_ISBN + '-' + course_hold
                                itemid = itemid.replace(" ", section_commas)

                            if itemid not in all_itemids and itemid[0:2] != " -":
                                db_sel = db.cursor()
                                db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(base_Department), str(sec_course), str(sec_section), str(base_Term)])
                                db.commit()
                                db_sel.close()
                                logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                            if use_pfi:
                               OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                            if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                itemid = base_Used_ISBN

                                if itemid not in all_itemids and itemid[0:2] != " -":
                                    db_sel = db.cursor()
                                    db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(base_Department), str(sec_course), str(sec_section), str(base_Term)])
                                    db.commit()
                                    db_sel.close()
                                    logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                if use_pfi:
                                    OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                       base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                       sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                        if base_FormatFlag.lower() == "d":
                            logger.debug("processing flag format 'D'")
                            ebook_count = 0

                            if section_commas != "":
                                logger.debug("Cleaning up section")
                                sec_section = sec_section.replace(",", section_commas)
                                sec_section = sec_section.replace("'", section_commas)
                                sec_section = sec_section.replace(" ", section_commas)
                                sec_instructor = sec_instructor.replace("'", "")

                            course_hold = sec_dept + '-' + sec_course + '-' + sec_section + '-' + sec_Term
                            itemid = base_ISBN + '-' + course_hold
                            itemid = itemid.replace(" ", section_commas)
                            ITEMIPF_hold = ITEMIPF

                            if sec_ebook_adopted.lower() != "y" and float(base_Ebook_Price) > 0 and str(ITEMIPF)[2:3] == 0 and ebook_count == 0:
                                logger.debug("sec_ebook_adopted: {}, base_Ebook_Price: {}, ITEMIPF: {}, ebook_count: {}.".format(sec_ebook_adopted, base_Ebook_Price, str(ITEMIPF)[2:3], ebook_count))
                                price_count = len(item_ipf_dict)
                                logger.debug("price count: {}".format(price_count))
                                for i, elem in enumerate(out_text):
                                    if elem == base_Ebook_Price_Text:
                                        price_loc = out_text[i]

                                    if price_count == price_loc:
                                        logger.debug("price count: {}, price loc: {}.".format(price_count, price_loc))
                                        out_text.pop(i)
                                        out_price.pop(i)

                                    else:
                                        if price_loc == 1:
                                            logger.debug(out_text)                  ##### NEEDS LOOKING AT #####
                                            logger.debug(out_price)                 ##### NEEDS LOOKING AT #####
                                        elif price_loc == 2:                        ##### NEEDS LOOKING AT #####
                                            logger.debug(out_text)                  ##### NEEDS LOOKING AT #####
                                            logger.debug(out_price)                 ##### NEEDS LOOKING AT #####
                                        elif price_loc == 3:                        ##### NEEDS LOOKING AT #####
                                            logger.debug(out_text)                  ##### NEEDS LOOKING AT #####
                                            logger.debug(out_price)                 ##### NEEDS LOOKING AT #####
                                        elif price_loc == 4:                        ##### NEEDS LOOKING AT #####
                                            logger.debug(out_text)                  ##### NEEDS LOOKING AT #####
                                            logger.debug(out_price)                 ##### NEEDS LOOKING AT #####
                                        elif price_loc == 5:                        ##### NEEDS LOOKING AT #####
                                            logger.debug(out_text)                  ##### NEEDS LOOKING AT #####
                                            logger.debug(out_price)                 ##### NEEDS LOOKING AT #####

                                    ITEMIPF = ITEMIPF - 100
                                    ebook_count = 1
                                    logger.debug("sec_ebook_adopted.lower() != 'y' and float(base_Ebook_Price) > 0 and ITEMIPF[2:3] == 0 and ebook_count == 0 and have set ebook count to {} and itemipf to {}.".format(ebook_count, ITEMIPF))

                            elif sec_ebook_adopted.lower() == "y":
                                logger.debug("out_price: {}".format(out_price))
                                logger.debug("out_text: {}".format(out_text))

                                if len(out_price) and len(out_text) == 1:
                                    out_price_1 = out_price[0]
                                    out_text_1 = out_text[0]
                                if len(out_price) and len(out_text) == 2:
                                    out_price_2 = out_price[1]
                                    out_text_2 = out_text[1]
                                if len(out_price) and len(out_text) == 3:
                                    out_price_3 = out_price[2]
                                    out_text_3 = out_text[2]
                                if len(out_price) and len(out_text) == 4:
                                    out_price_4 = out_price[3]
                                    out_text_4 = out_text[3]
                                if len(out_price) and len(out_text) == 5:
                                    out_price_5 = out_price[4]
                                    out_text_5 = out_text[4]

                                ITEMIPF = ITEMIPF_hold
                                ebook_count = 0
                                logger.debug("out price 1: {}, out text 1: {}, out price 2: {}, out text 2: {}, out price 3: {}, out text 3: {}, out price 4: {}, out text 4: {}, out price 5: {}, out text 5: {}.".format(out_price_1, out_text_1, out_price_2, out_text_2, out_price_3, out_text_3, out_price_4, out_text_4, out_price_5, out_text_5))
                            if itemid[1:2] != "-":
                                if base_ISBN_HR.lower() == "dgt":
                                    # base 1 price IPF =701, 2 price = 702, 3 price = 703, 4 price = 704, 5 price = 705
                                    if int(sec_ebook1_price_2) > 0 or int(sec_ebook2_price_2) > 0:
                                        ITEMIPF = ITEMIPF + 1
                                    if int(sec_ebook1_price_3) > 0 or int(sec_ebook2_price_3) > 0:
                                        ITEMIPF = ITEMIPF + 1
                                    if int(sec_ebook1_price_4) > 0 or int(sec_ebook2_price_4) > 0:
                                        ITEMIPF = ITEMIPF + 1
                                    if int(sec_ebook1_price_5) > 0 or int(sec_ebook2_price_5) > 0:
                                        ITEMIPF = ITEMIPF + 1

                                    # check for 2 vendors, bump to IPF 71x series
                                    if sec_ebook2_vendor != "":
                                        ITEMIPF = ITEMIPF + 10

                            if itemid not in all_itemids and itemid[0:2] != " -":
                                db_sel = db.cursor()
                                db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(base_Department), str(sec_course), str(sec_section), str(base_Term)])
                                db.commit()
                                db_sel.close()
                                logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                            if use_pfi:
                                OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                   base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                   sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)

                            if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                itemid = base_Used_ISBN + '-' + course_hold
                                itemid = itemid.replace(" ", section_commas)

                                if itemid not in all_itemids and itemid[0:2] != " -":
                                    db_sel = db.cursor()
                                    db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(base_Department), str(sec_course), str(sec_section), str(base_Term)])
                                    db.commit()
                                    db_sel.close()
                                    logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                if use_pfi:
                                    OUTFILE, page_line_count = send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description,
                                                       base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage,
                                                       sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                    if len(file_row) == 0:
                        logger.warn("Row {} in file {}, dropped no data.".format(row_counter, file_name))
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

def send_pfi(OUTFILE, page_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5):

    #determine where to send output

    if license_status == 1 or (license_status != 1 and page_line_count < 50):
        page_line_count += 1

        new_line = "0001 " + str(itemid) + \
                   " 7 0 |" + str(base_Author) + " " + str(base_Title) + \
                   "| 23 0 |" + str(regular_price) + \
                   "| 121 0 |" + str(ITEMIPF) + \
                   "| 9800 0 |" + str(base_Author) + \
                   "| 9801 0 |" + str(base_Title) + \
                   "| 9802 0 |" + str(base_ISBN) + \
                   "| 9803 0 |" + str(base_ISBN_HR) + \
                   "| 9804 0 |" + str(base_Vendor_Style) + \
                   "| 9805 0 |" + str(base_Edition_Status) + \
                   "| 9806 0 |" + str(base_Imprint) + \
                   "| 9807 0 |" + str(base_Edition) + \
                   "| 9808 0 |" + str(base_New_Price) + \
                   "| 9809 0 |" + str(base_New_Price_Text) + \
                   "| 9810 0 |" + str(base_Used_Price) + \
                   "| 9811 0 |" + str(base_Used_Price_Text) + \
                   "| 9812 0 |" + str(base_New_Rental_Price) + \
                   "| 9813 0 |" + str(base_New_Rental_Price_Text) + \
                   "| 9814 0 |" + str(base_Ebook_Price) + \
                   "| 9815 0 |" + str(base_Ebook_Price_Text) + \
                   "| 9816 0 |" + str(base_Used_Rental_Price) + \
                   "| 9817 0 |" + str(base_Used_Rental_Price_Text) + \
                   "| 9818 0 |" + str(base_Sale_Price) + \
                   "| 9819 0 |" + str(target_date_start) + \
                   "| 9820 0 |" + str(target_date_end) + \
                   "| 9821 0 |" + str(base_Term) + \
                   "| 9822 0 |" + str(base_Term_Description) + \
                   "| 9823 0 |" + str(base_Requested_Qty) + \
                   "| 9824 0 |" + str(base_Class_Capacity_Qty) + \
                   "| 9825 0 |" + str(base_Actual_Enrollment_Qty) + \
                   "| 9826 0 |" + str(base_Est_Sales_Qty) + \
                   "| 9827 0 |" + str(base_Category) + \
                   "| 9828 0 |" + str(base_Division) + \
                   "| 9829 0 |" + str(base_Department) + \
                   "| 9830 0 |" + str(base_Class) + \
                   "| 9831 0 |" + str(base_New_Store_Qty) + \
                   "| 9832 0 |" + str(base_New_Warehouse_Qty) + \
                   "| 9833 0 |" + str(base_Used_Store_Qty) + \
                   "| 9834 0 |" + str(base_Used_Warehouse_Qty) + \
                   "| 9835 0 |" + str(base_On_Order_PO1) + \
                   "| 9836 0 |" + str(base_On_Order_Qty1) + \
                   "| 9837 0 |" + str(base_On_Order_Date1) + \
                   "| 9838 0 |" + str(base_On_Order_PO2) + \
                   "| 9839 0 |" + str(base_On_Order_Qty2) + \
                   "| 9840 0 |" + str(base_On_Order_Date2) + \
                   "| 9841 0 |" + str(base_On_Order_PO3) + \
                   "| 9842 0 |" + str(base_On_Order_Qty3) + \
                   "| 9843 0 |" + str(base_On_Order_Date3) + \
                   "| 9844 0 |" + str(base_Total_PO_Qty) + \
                   "| 9845 0 |" + str(sec_loc_code) + \
                   "| 9846 0 |" + str(sec_bookxofy) + \
                   "| 9847 0 |" + str(sec_course_id) + \
                   "| 9848 0 |" + str(sec_course_list) + \
                   "| 9849 0 |" + str(sec_ebook_adopted) + \
                   "| 9850 0 |" + str(sec_class_cap) + \
                   "| 9851 0 |" + str(sec_prof_requested) + \
                   "| 9852 0 |" + str(sec_estimated_sales) + \
                   "| 9853 0 |" + str(base_New_Pending_Return_Qty) + \
                   "| 9854 0 |" + str(base_Used_Pending_Return_Qty) + \
                   "| 9855 0 |" + str(base_New_insite_Pending_Order) + \
                   "| 9856 0 |" + str(base_Used_insite_Pending_Order) + \
                   "| 9857 0 |" + str(base_New_Rental_insite_Pending_Order) + \
                   "| 9858 0 |" + str(base_Used_Rental_insite_Pending_Order) + \
                   "| 9859 0 |" + str(base_On_Order_PO1_Vendor) + \
                   "| 9860 0 |" + str(base_On_Order_PO2_Vendor) + \
                   "| 9861 0 |" + str(base_On_Order_PO3_Vendor) + \
                   "| 9862 0 |" + str(base_On_Order_Qty1_Used) + \
                   "| 9863 0 |" + str(base_On_Order_Qty2_Used) + \
                   "| 9864 0 |" + str(base_On_Order_Qty3_Used) + \
                   "| 9865 0 |" + str(base_Used_ISBN) + \
                   "| 9885 0 |" + str(save_amount) + \
                   "| 9886 0 |" + str(out_price_1) + \
                   "| 9887 0 |" + str(out_text_1) + \
                   "| 9888 0 |" + str(out_price_2) + \
                   "| 9889 0 |" + str(out_text_2) + \
                   "| 9890 0 |" + str(out_price_3) + \
                   "| 9891 0 |" + str(out_text_3) + \
                   "| 9892 0 |" + str(out_price_4) + \
                   "| 9893 0 |" + str(out_text_4) + \
                   "| 9894 0 |" + str(out_price_5) + \
                   "| 9895 0 |" + str(base_Edition_Status) + \
                   "| 9896 0 |" + str(out_text_5) + \
                   "| 9897 0 |" + str(sec_course) + \
                   "| 9898 0 |" + str(sec_dept) + \
                   "| 9899 0 |" + str(sec_section) + \
                   "| 9900 0 |" + str(sec_instructor) + \
                   "| 9902 0 |" + str(sec_delete_flag) + \
                   "| 9903 0 |" + str(base_FormatFlag) + \
                   "| 9914 0 |" + str(usage) + \
                   "| 9915 0 |" + str(sec_dept_course_section) + \
                   "| 9916 0 |" + str(base_Sale_Price1) + \
                   "| 9917 0 |" + str(base_Sale_Start_Date1) + \
                   "| 9918 0 |" + str(base_Sale_End_Date1) + \
                   "| 9919 0 |" + str(base_Sale_Start_Time1) + \
                   "| 9920 0 |" + str(base_Sale_End_Time1) + \
                   "| 9921 0 |" + str(base_Sale_Price2) + \
                   "| 9922 0 |" + str(base_Sale_Start_Date2) + \
                   "| 9923 0 |" + str(base_Sale_End_Date2) + \
                   "| 9924 0 |" + str(base_Sale_Start_Time2) + \
                   "| 9925 0 |" + str(base_Sale_End_Time2) + \
                   "| 9926 0 |" + str(base_Sale_Price3) + \
                   "| 9927 0 |" + str(base_Sale_Start_Date3) + \
                   "| 9928 0 |" + str(base_Sale_End_Date3) + \
                   "| 9929 0 |" + str(base_Sale_Start_Time3) + \
                   "| 9930 0 |" + str(base_Sale_End_Time3) + \
                   "| 9931 0 |" + str(base_Sale_Price4) + \
                   "| 9932 0 |" + str(base_Sale_Start_Date4) + \
                   "| 9933 0 |" + str(base_Sale_End_Date4) + \
                   "| 9934 0 |" + str(base_Sale_Start_Time4) + \
                   "| 9935 0 |" + str(base_Sale_End_Time4) + \
                   "| 9936 0 |" + str(base_image_name) + \
                   "| 9937 0 |" + str(sec_ebook1_vendor) + \
                   "| 9938 0 |" + str(sec_ebook1_period_1) + \
                   "| 9939 0 |" + str(sec_ebook1_price_1) + \
                   "| 9940 0 |" + str(sec_ebook1_period_2) + \
                   "| 9941 0 |" + str(sec_ebook1_price_2) + \
                   "| 9942 0 |" + str(sec_ebook1_period_3) + \
                   "| 9943 0 |" + str(sec_ebook1_price_3) + \
                   "| 9944 0 |" + str(sec_ebook1_period_4) + \
                   "| 9945 0 |" + str(sec_ebook1_price_4) + \
                   "| 9946 0 |" + str(sec_ebook1_period_5) + \
                   "| 9947 0 |" + str(sec_ebook1_price_5) + \
                   "| 9948 0 |" + str(sec_ebook2_vendor) + \
                   "| 9949 0 |" + str(sec_ebook2_period_1) + \
                   "| 9950 0 |" + str(sec_ebook2_price_1) + \
                   "| 9951 0 |" + str(sec_ebook2_period_2) + \
                   "| 9952 0 |" + str(sec_ebook2_price_2) + \
                   "| 9953 0 |" + str(sec_ebook2_period_3) + \
                   "| 9954 0 |" + str(sec_ebook2_price_3) + \
                   "| 9955 0 |" + str(sec_ebook2_period_4) + \
                   "| 9956 0 |" + str(sec_ebook2_price_4) + \
                   "| 9957 0 |" + str(sec_ebook2_period_5) + \
                   "| 9958 0 |" + str(sec_ebook2_price_5)

        if target_delay != "":
            new_line = new_line + "| 9500 0 |" + str(target_delay)

        new_line = new_line + "|,\n"

        OUTFILE.write(new_line)

        logger.debug("PFI file {} created.".format(OUTFILE))

    else:
        logger.info(" skipped due to license status.")
    return OUTFILE, page_line_count

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
