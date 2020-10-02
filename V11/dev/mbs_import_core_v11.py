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

        global file_name, drive_letter, use_pricer_level, log_level, log_delete_after, log_max, log_name, log_path, log_file, use_pricer_db, local_db, data_in_save, data_input_delete_after, data_path, input_data_path, wait_time, sort_order, none_100, usage_convert, IPF100, term_multi, date_format, ipf1x5_threshold, display_original, section_commas, New_On_Hand, Used_On_Hand, New_Addl, Used_Addl, New_Pend_Ret, Used_Pend_Ret, New_insite_Pend_Ord, Used_insite_Pend_Ord, New_Rental_insite_Pend_Ord, Used_Rental_insite_Pend_Ord, use_pfi, use_api, use_soap, i1_path, m1_path, r7_path, api_page_count, soap_api_ip, soap_token, soap_user, rest_api_url, rest_token, api_responses
        global requests, db, client, config_path, license_status, pricer_api_alive, logger, origin_time, output_format, page_line_count, total_api_req_counter
        global out_price, out_text, json_outstring, soap_update_str

        origin_time = time.time()

        key = 'yjhtlsic64,9w6H1'

        requests = requests.Session()
        retry = Retry(total=10, backoff_factor=1, status_forcelist=[404])
        adapter = HTTPAdapter(retry)
        requests.mount('http', adapter)

        pricer_api_alive = False

        api_req_counter = 0
        page_line_count = 0
        total_api_req_counter = 0
        api_out_page_count = 0
        api_out_line_count = 0
        out_count = 0


        api_responses = []
        pricer_response = []
        json_outstring = []
        soap_update_str = []

        command_id = 0

        regular_price = 0
        save_amount = 0

        target_delay = ""
        target_date_start = ""
        target_date_end = ""

        ##get file name without extension
        file_name_raw = __file__
        file_name_start = file_name_raw.rfind('/') + 1
        file_name = file_name_raw[file_name_start: -3]

        config_path = ""

        dev_mode = 'PYCHARM_HOSTED' in os.environ

        if not dev_mode:
            type_id = 5
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
            type_id = 0
            if os.name == 'nt':
                config_path = "c:\\pricer\\import\\configuration\\mbs_import.mms"
            else:
                config_path = "/pricer/import/configuration/mbs_import.mms"

        try:
            with open(config_path) as config_source:
                content = config_source.read()
            config = BeautifulSoup(content, 'xml')

            ###drive
            drive_letter = config.MMImport.get('Drive')
            pricer_folder = config.MMImport.get('Pricer_Folder')
            r3server_folder = config.MMImport.get('R3Server_Folder')
            pricer_level_xml = config.MMImport.get('Pricer_Level_File')

            if os.name == 'nt':
                drive_letter = config.MMImport.get('Drive') + ':\\'
            else:
                drive_letter = '/'

            # Get Paths for price log level
            for dirpath, subdirs, files in os.walk(os.path.join(drive_letter, pricer_folder, r3server_folder)):
                for x in files:
                    if x.endswith(pricer_level_xml):
                        pricer_conf_path = os.path.join(dirpath, x)

            ###logging
            if (config.Logging.get('Use_Pricer_Level')).lower() == "true":
                use_pricer_level = True
            else:
                use_pricer_level = False

            log_level = config.Logging.get('Level').lower()
            log_delete_after = config.Logging.get('Delete_After')
            log_max = config.Logging.get('Log_Max_MB')
            log_name = config.Logging.get('Log_Name')
            log_path = config.Logging.get('Path')

            log_file = os.path.join(drive_letter, log_path, log_name)

            if os.name != 'nt':
                log_file = log_file.replace('\\', '/')
                log_path = log_path.replace('\\', '/')

            # set up logging

            if use_pricer_level:

                with open(pricer_conf_path, "r") as pricer_content_file:
                    pricer_content = pricer_content_file.read()
                    config = BeautifulSoup(pricer_content, 'xml')
                    pricer_level = config.Loggers.Root.get('level')

                if pricer_level.lower() == 'info':
                    log_level = 'info'
                elif pricer_level.lower() == 'error':
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

            if not os.path.isdir(os.path.join(drive_letter, log_path)):
                os.makedirs(os.path.join(drive_letter, log_path), mode=0o777)
                logger.debug('Successfully created {} input folder for archiving.'.format(os.path.join(drive_letter, log_path)))

            logger.info('{} successfully started with logging at {}.'.format(file_name, log_level.upper()))
            if year_stamp != '2020':
                logger.info('©Copyright 2020-{} MarginMate Corporation. All Rights Reserved.'.format(year_stamp))
            else:
                logger.info('©Copyright 2020 MarginMate Corporation. All Rights Reserved.')
            #

            if os.name == 'nt':
                EventLogger.event_log_writer(file_name, "info", "Pricer", 100, log_path)
            else:
                EventLogger.event_log_writer(file_name_raw, "info", "Pricer", 100, log_path)

            ##data local
            if (config.DataLocal.get('UsePricer')).lower() == "true":
                use_pricer_db = True
            else:
                use_pricer_db = False

            local_db = str(triple_des(key).decrypt(base64.b64decode(config.DataLocal.get('LocalDB')), padmode=2), 'utf-8')

            # Datain
            if (config.DataIn.get('DataInputSave')).lower() == "true":
                data_in_save = True
            else:
                data_in_save = False

            data_input_delete_after = config.DataIn.get('DeleteAfter')
            data_path = os.path.join(drive_letter, config.DataIn.get('Path'))
            input_data_path = os.path.join(drive_letter, config.DataIn.get('InputHoldPath'))
            wait_time = config.DataIn.get('FileWaitTime')

            # Sort
            sort_order = str(config.Sort.get('Order')).lower()

            # Price
            if (config.Price.get('None_100')).lower() == "true":
                none_100 = True
            else:
                none_100 = False

            # Usage
            if (config.Usage.get('Convert')).lower() == "true":
                usage_convert = True
            else:
                usage_convert = False

            # Zero_Zero
            if (config.Zero_Zero.get('IPF100')).lower() == "true":
                IPF100 = True
            else:
                IPF100 = False

            # Term
            if (config.Term.get('Multi')).lower() == "true":
                term_multi = True
            else:
                term_multi = False

            # Date formate
            date_format = config.Date.get('Format')

            # IPF1X5
            ipf1x5_threshold = config.IPF1x5.get('Threshold')

            # Replace
            if (config.Replace_Section_Commas.get('DisplayOriginal')).lower() == "true":
                display_original = True
            else:
                display_original = False

            section_commas = config.Replace_Section_Commas.get('Char')

            # QOH CALC

            New_On_Hand = config.QOH_Calc.get('New_On_Hand')
            Used_On_Hand = config.QOH_Calc.get('Used_On_Hand')
            New_Addl = config.QOH_Calc.get('New_Addl')
            Used_Addl = config.QOH_Calc.get('Used_Addl')
            New_Pend_Ret = config.QOH_Calc.get('New_Pend_Ret')
            Used_Pend_Ret = config.QOH_Calc.get('Used_Pend_Ret')
            New_insite_Pend_Ord = config.QOH_Calc.get('New_insite_Pend_Ord')
            Used_insite_Pend_Ord = config.QOH_Calc.get('Used_insite_Pend_Ord')
            New_Rental_insite_Pend_Ord = config.QOH_Calc.get('New_Rental_insite_Pend_Ord')
            Used_Rental_insite_Pend_Ord = config.QOH_Calc.get('Used_Rental_insite_Pend_Ord')

            # Dataout
            if (config.DataOut.get('UsePFI')).lower() == "true":
                use_pfi = True
                output_format = "PFI FILE"
            else:
                use_pfi = False

            if (config.DataOut.get('UseAPI')).lower() == "true":
                use_api = True
                output_format = "REST API"
            else:
                use_api = False

            if (config.DataOut.get('UseSoap')).lower() == "true":
                use_soap = True
                output_format = "SOAP API"
            else:
                use_soap = False

            i1_path = config.DataOut.get('i1_Path')
            m1_path = config.DataOut.get('m1_Path')
            r7_path = config.DataOut.get('r7_Path')
            api_page_count = config.DataOut.get('APIPageCount')
            soap_api_ip = str(triple_des(key).decrypt(base64.b64decode(config.DataOut.get('SoapIP')), padmode=2), 'utf-8')
            soap_token = str(triple_des(key).decrypt(base64.b64decode(config.DataOut.get('SoapToken')), padmode=2), 'utf-8')
            rest_api_url = str(triple_des(key).decrypt(base64.b64decode(config.DataOut.get('RestURL')), padmode=2), 'utf-8')
            pricer_user = str(triple_des(key).decrypt(base64.b64decode(config.DataOut.get('PricerUser')), padmode=2), 'utf-8')
            pricer_password = str(triple_des(key).decrypt(base64.b64decode(config.DataOut.get('PricerPassword')), padmode=2), 'utf-8')
            rest_token = "Basic {}".format(base64.b64encode((pricer_user + ":" + pricer_password).encode("utf-8")).decode("utf-8"))



        except IOError as e:
            EventLogger.event_log_writer(file_name + " failed to find configuration file " + config_path + " I/O error({0}): {1}".format(e.errno, e.strerror), "error", "Pricer", 101, log_path)
            sys.exit(99)

        # setup outfiles for pfi use

        if use_pfi:
            data_file = 'MBS_' + file_time
            i1_file = os.path.join(drive_letter, i1_path, data_file + ".i1")
            m1_file = os.path.join(drive_letter, m1_path, data_file + ".m1")
            r7_file = os.path.join(drive_letter, r7_path, data_file + ".r7")
            temp_m1_file = os.path.join(drive_letter, i1_path, data_file + ".m1")

            logger.debug("i1 file: {}".format(i1_file))

            OUTFILE = open(i1_file, "w", encoding="utf-8")

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

        ##connect pricer db
        try:
            db = DBhandler.pricer_db(drive_letter, log_file, log_path, log_level, config_path, file_name, 'True', '')
        except:
            logger.critical("Critical error has occured see event viewer/log for more information.")
            e = traceback.format_exc(limit=None, chain=True)
            AlertManager.alert_manager(config_path, log_path, 'fatal', "Failed to connect to database with error " + str(e))
            sys.exit(99)

        # clean up input directory
        if data_in_save and int(data_input_delete_after) > 0:
            DirectoryCleaner.clean_directory(int(data_input_delete_after), input_data_path, log_file, log_level, config_path)

        ### Main File Content Processing ###

        file_list = {}
        file_list_new = []

        for ext_file_name in os.listdir(data_path):
            if ext_file_name.lower().endswith("tx1") or ext_file_name.lower().endswith("txt"):
                file_list.update({ext_file_name: str(os.stat(data_path + ext_file_name).st_mtime)})

        for current_file in sorted(file_list, key=file_list.get, reverse=False):
            file_list_new.append(current_file)

        file_txt_list = {}
        file_tx1_list = {}
        file_list_new = []

        for ext_file_name in os.listdir(data_path):
            if ext_file_name.lower().endswith("txt"):
                file_txt_list.update({ext_file_name: str(os.stat(data_path + ext_file_name).st_mtime)})
            if ext_file_name.lower().endswith("tx1"):
                file_tx1_list.update({ext_file_name: str(os.stat(data_path + ext_file_name).st_mtime)})

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

        # get all itemids in t_links
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
                                secondary_file_dict.update({"ebook1_price_4": ebook1_price_4})
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
                        if license_status == 1 or (license_status != 1 and row_counter < 50):
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
                                    base_GenKey, base_FormatFlag, base_Author, base_Title, base_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text,
                                    base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price1,
                                    base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3,
                                    base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                    base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category,
                                    base_Division,
                                    base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order,
                                    base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                    base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                    base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name))

                            ## Trim author and title to correct lenghts

                            base_Author = base_Author[0:254]
                            base_Title = base_Title[0:254]

                            ###format PO Dates

                            year_part1 = ''
                            year_part2 = ''
                            year_part3 = ''
                            month_part1 = ''
                            month_part2 = ''
                            month_part3 = ''
                            day_part1 = ''
                            day_part2 = ''
                            day_part3 = ''

                            if date_format != "YYYYMMDD":

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

                            # process if valid ISBN/SKU
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

                                logger.debug(
                                    "base_ISBN: {}, base_New_Price: {}, base_Used_Price: {}, base_New_Rental_Price: {}, base_Ebook_Price: {}, base_Used_Rental_Price: {}, base_Requested_Qty: {}, base_New_Store_Qty: {}, base_New_Warehouse_Qty: {}, base_Used_Store_Qty: {}, base_Used_Warehouse_Qty: {}, base_On_Order_Qty1: {}, base_On_Order_Qty2: {}, base_On_Order_Qty3: {}, base_Total_PO_Qty: {}, base_Class_Capacity_Qty: {}, base_Actual_Enrollment_Qty: {}, base_Est_Sales_Qty: {}, base_Sale_Price: {}, base_New_Pending_Return_Qty: {}, base_Used_Pending_Return_Qty: {}, base_New_insite_Pending_Order: {}, base_Used_insite_Pending_Order: {}, base_On_Order_Qty1_Used: {}, base_On_Order_Qty2_Used: {}, base_On_Order_Qty3_Used: {}.".format(
                                        base_ISBN, base_New_Price, base_Used_Price, base_New_Rental_Price, base_Ebook_Price, base_Used_Rental_Price, base_Requested_Qty, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty,
                                        base_Used_Warehouse_Qty, base_On_Order_Qty1, base_On_Order_Qty2,
                                        base_On_Order_Qty3, base_Total_PO_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Sale_Price, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                        base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                        base_On_Order_Qty1_Used, base_On_Order_Qty2_Used, base_On_Order_Qty3_Used))
                                # Start book record###

                                if base_Category.lower() == "t":
                                    # Determine base item IPF
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
                                        item_ipf_dict.append(Book(ITEMIPF, base_Used_Rental_Price, base_Used_Rental_Price_Text))

                                    if float(base_Ebook_Price) > 0:
                                        ITEMIPF += 1
                                        item_ipf_dict.append(Book(ITEMIPF, base_Ebook_Price, base_Ebook_Price_Text))

                                    logger.debug(
                                        "base_Category : {}, ITEMIPF : {}, base_New_Price: {}, base_Used_Price: {}, base_Used_Rental_Price: {}, base_Used_Rental_Price: {}, base_Ebook_Price: {}, item_ipf_dict : {}.".format(base_Category, ITEMIPF,
                                                                                                                                                                                                                              base_New_Price,
                                                                                                                                                                                                                              base_Used_Price,
                                                                                                                                                                                                                              base_New_Rental_Price,
                                                                                                                                                                                                                              base_Used_Rental_Price,
                                                                                                                                                                                                                              base_Ebook_Price,
                                                                                                                                                                                                                              item_ipf_dict))

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

                                    # Ascending order output

                                    if sort_order == "ascending":
                                        if len(item_ipf_dict) > 0:
                                            ITEMIPF = len(item_ipf_dict) * 100

                                            sorted_item_ipf = sorted(item_ipf_dict, key=lambda book: book.condition)

                                            for item_ipf_key in sorted_item_ipf:
                                                out_price.append(item_ipf_key.price)
                                                out_text.append(item_ipf_key.condition)

                                            logger.debug("Sorted IPF List: {}.".format(sorted_item_ipf))

                                    # Natural order output

                                    if sort_order == "natural":
                                        if len(item_ipf_dict) > 0:
                                            ITEMIPF = len(item_ipf_dict) * 100

                                            sorted_item_ipf = sorted(item_ipf_dict, key=lambda book: book.ipf)

                                            for item_ipf_key in sorted_item_ipf:
                                                out_price.append(item_ipf_key.price)
                                                out_text.append(item_ipf_key.condition)

                                            logger.debug("Sorted IPF List: {}.".format(sorted_item_ipf))

                                    # Price order output

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

                                    # set out price values from out price list

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

                                    # set out text values from out text list

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

                                    # format all price fields if not none

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

                                    # Format Quantities

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

                                    # Calculate Total QOH

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

                                    # handler for missing all prices

                                    if ITEMIPF == 0 and none_100:
                                        ITEMIPF = 100
                                        logger.debug("record on line {},with ISBN of {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

                                    elif ITEMIPF == 0 and not none_100 and str(base_ISBN).lower() != "ntr":
                                        ITEMIPF = 104
                                        logger.debug("record on line {}, with ISBN of {}, in {} did not generate valid IPF, default IPF{} assigned".format(row_counter, base_ISBN, new_file, ITEMIPF))

                                    # Check ipf1x5 threshold exceeded, if so assign IPF1X5
                                    logger.debug("Out Prices 1-5, {}. IPF1X5 Threshold {}.".format(out_price, ipf1x5_threshold))
                                    for i in out_price:
                                        if float(i) > float(ipf1x5_threshold) > 0:
                                            ITEMIPF = 105
                                            logger.debug("This ISBN {}, has Prices {} greater than IPF threshold {}, setting ITEMIPF to {}.".format(base_ISBN, out_price, ipf1x5_threshold, ITEMIPF))

                                    # ipf for none on hand but some on order

                                    if Total_QOH <= 0 and int(base_Total_PO_Qty) > 0 and ITEMIPF != 104:
                                        ITEMIPF = 101
                                        logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                                    # ipf for none on hand and none on order

                                    if Total_QOH <= 0 and int(base_Total_PO_Qty) <= 0 and ITEMIPF != 104:
                                        ITEMIPF = 102
                                        logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                                    # ipf for none on hand and none on order, but prefix 281 selected via IPF100 parameter

                                    if Total_QOH <= 0 and int(base_Total_PO_Qty) <= 0 and IPF100 and base_ISBN[0:3] == 281:
                                        ITEMIPF = 100
                                        logger.debug("This ISBN {}, has {} quantity in stock and {} on order, setting ITEMIPF to {}.".format(base_ISBN, Total_QOH, base_Total_PO_Qty, ITEMIPF))

                                    # ipf for ISBN = "NTR"

                                    if str(base_ISBN).lower() == "ntr":
                                        ITEMIPF = 106
                                        logger.debug("This ISBN {}, is flagged as NTR setting ITEMIPF to {}.".format(base_ISBN, ITEMIPF))

                                    # go to IPF x5x series for store format "D" (organized by Dept/Course/Section) vs "A", which is alpha by author/title

                                    if str(base_FormatFlag).lower() == "d":
                                        ITEMIPF = ITEMIPF + 50
                                    logger.debug("This ISBN {}, is flagged for formating with {}. Incrementing ITEMIPF by 50".format(base_ISBN, base_FormatFlag))
                                    # check for digital edition, set base IPF at 700

                                    if str(base_ISBN_HR).lower() == "dgt":
                                        ITEMIPF = 700
                                        logger.debug("This ISBN {}, is only digital setting ITEMIPF to {}.".format(base_ISBN, ITEMIPF))
                                    # Always create base ISBN record
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

                                    logger.debug(
                                        "Sale Start 1: {}, Sale End 1: {}, Sale Start 2: {}, Sale End 2: {}, Sale Start 3: {}, Sale End 3: {}, Sale Start 4: {}, Sale End 4: {}.".format(sale_start1, sale_end1, sale_start2, sale_end2, sale_start3,
                                                                                                                                                                                         sale_end3, sale_start4, sale_end4))

                                    base_Sale_Price = 0
                                    save_amount = 0
                                    itemid = base_ISBN
                                    regular_price = base_New_Price
                                    ITEMIPF = "Regular"

                                    if isinstance(regular_price, (int, float)):
                                        regular_price = "%.2f" % (float(regular_price) / 1)

                                    if float(base_Sale_Price1) > 0 and int(sale_start1) > '1900-01-01 00:00:00':

                                        if s_stamp >= datetime.datetime(sale_start1) and s_stamp <= datetime.datetime(sale_end1):
                                            save_amount = float(regular_price) - float(base_Sale_Price1)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price1) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start1
                                            target_date_end = sale_end1

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                        elif datetime.datetime(sale_start1) >= s_stamp and datetime.datetime(sale_end1) >= s_stamp:
                                            save_amount = float(regular_price) - float(base_Sale_Price1)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price1) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start1
                                            target_date_end = sale_end1
                                            target_delay = sale_start1

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                            # Sale END
                                            ITEMIPF = "Regular"
                                            save_amount = 0
                                            target_delay = sale_end1

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                    if float(base_Sale_Price2) > 0 and int(sale_start2) > '1900-01-01 00:00:00':

                                        if s_stamp >= datetime.datetime(sale_start2) and s_stamp <= datetime.datetime(sale_end2):
                                            save_amount = float(regular_price) - float(base_Sale_Price2)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price2) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start2
                                            target_date_end = sale_end2

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                        elif datetime.datetime(sale_start2) >= s_stamp and datetime.datetime(sale_end2) >= s_stamp:
                                            save_amount = float(regular_price) - float(base_Sale_Price2)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price2) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start2
                                            target_date_end = sale_end2
                                            target_delay = sale_start2

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                            # Sale END
                                            ITEMIPF = "Regular"
                                            save_amount = 0
                                            target_delay = sale_end2

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                    if float(base_Sale_Price3) > 0 and int(sale_start3) > '1900-01-01 00:00:00':

                                        if s_stamp >= datetime.datetime(sale_start3) and s_stamp <= datetime.datetime(sale_end3):
                                            save_amount = float(regular_price) - float(base_Sale_Price3)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price3) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start3
                                            target_date_end = sale_end3

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                        elif datetime.datetime(sale_start3) >= s_stamp and datetime.datetime(sale_end3) >= s_stamp:
                                            save_amount = float(regular_price) - float(base_Sale_Price3)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price3) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start3
                                            target_date_end = sale_end3
                                            target_delay = sale_start3

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                            # Sale END
                                            ITEMIPF = "Regular"
                                            save_amount = 0
                                            target_delay = sale_end3

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                    if float(base_Sale_Price4) > 0 and int(sale_start4) > '1900-01-01 00:00:00':

                                        if s_stamp >= datetime.datetime(sale_start4) and s_stamp <= datetime.datetime(sale_end4):
                                            save_amount = float(regular_price) - float(base_Sale_Price4)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price4) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start4
                                            target_date_end = sale_end4

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                        elif datetime.datetime(sale_start4) >= s_stamp and datetime.datetime(sale_end4) >= s_stamp:
                                            save_amount = float(regular_price) - float(base_Sale_Price4)
                                            ITEMIPF = "Sale"

                                            base_Sale_Price = "%.2f" % (float(base_Sale_Price4) / 1)
                                            save_amount = "%.2f" % (float(save_amount) / 1)
                                            target_date_start = sale_start4
                                            target_date_end = sale_end4
                                            target_delay = sale_start4

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                            # Sale END
                                            ITEMIPF = "Regular"
                                            save_amount = 0
                                            target_delay = sale_end4

                                            if use_pfi:
                                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                   base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                   base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
                                                                   base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                   base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                   base_Sale_End_Date4, base_Sale_Start_Time4,
                                                                   base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                                   base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                   base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                   base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor,
                                                                   base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                   out_text_2, out_text_3, out_text_4, out_text_5,
                                                                   usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                                   sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                   sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                   sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                                   sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                            if use_api:
                                               json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url,
                                                                                                                                                                                                soap_update_str, api_req_counter, command_id,
                                                                                                                                                                                                api_out_line_count, itemid, regular_price, ITEMIPF,
                                                                                                                                                                                                target_delay, base_FormatFlag,
                                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN,
                                                                                                                                                                                                base_ISBN_HR, base_Vendor_Style, base_Publisher,
                                                                                                                                                                                                base_Imprint, base_Edition, base_Edition_Status,
                                                                                                                                                                                                base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                                base_Ebook_Price, base_Ebook_Price_Text,
                                                                                                                                                                                                base_Used_Rental_Price, base_Used_Rental_Price_Text,
                                                                                                                                                                                                base_Sale_Price, target_date_start, target_date_end,
                                                                                                                                                                                                base_Sale_Price1,
                                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1,
                                                                                                                                                                                                base_Sale_Start_Time1, base_Sale_End_Time1,
                                                                                                                                                                                                base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2,
                                                                                                                                                                                                base_Sale_End_Time2, base_Sale_Price3,
                                                                                                                                                                                                base_Sale_Start_Date3,
                                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4,
                                                                                                                                                                                                base_Sale_Start_Date4, base_Sale_End_Date4,
                                                                                                                                                                                                base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                                                                                                                                                                base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                                base_Est_Sales_Qty, base_Category, base_Division,
                                                                                                                                                                                                base_Department, base_Class, base_New_Store_Qty,
                                                                                                                                                                                                base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                                base_New_insite_Pending_Order,
                                                                                                                                                                                                base_Used_insite_Pending_Order,
                                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                                base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                                                                                                                                                                base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used,
                                                                                                                                                                                                base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                                base_On_Order_Qty3_Used, base_On_Order_Date3,
                                                                                                                                                                                                base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5,
                                                                                                                                                                                                out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course,
                                                                                                                                                                                                sec_section, sec_loc_code, sec_bookxofy,
                                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor,
                                                                                                                                                                                                save_amount, sec_delete_flag, sec_dept_course_section,
                                                                                                                                                                                                sec_ebook_adopted, sec_class_cap, sec_prof_requested,
                                                                                                                                                                                                sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2,
                                                                                                                                                                                                sec_ebook1_period_3, sec_ebook1_price_3,
                                                                                                                                                                                                sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5,
                                                                                                                                                                                                sec_ebook2_vendor, sec_ebook2_period_1,
                                                                                                                                                                                                sec_ebook2_price_1,
                                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2,
                                                                                                                                                                                                sec_ebook2_period_3, sec_ebook2_price_3,
                                                                                                                                                                                                sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                    if use_pfi:
                                        OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                           base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                           base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                           base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                           base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                           base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                           base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                           base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                           base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                           base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                           base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                           out_text_4, out_text_5, usage,
                                                           sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                           sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                           sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                           sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                           sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                    if use_api:
                                       json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                        api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                        api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                        regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                        base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                        base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                        base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                        base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                        base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                        base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                        base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                        target_date_end, base_Sale_Price1,
                                                                                                                                                                                        base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                        base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                        base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                        base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                        base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                        base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                        base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                        base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                        base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                        base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                        base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                        base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                        base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                        base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                        base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                        base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                        base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                        base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2,
                                                                                                                                                                                        base_On_Order_PO2_Vendor, base_On_Order_Qty2,
                                                                                                                                                                                        base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                        base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                        base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty,
                                                                                                                                                                                        base_image_name, out_price_1, out_price_2, out_price_3,
                                                                                                                                                                                        out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                        out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                        sec_loc_code, sec_bookxofy,
                                                                                                                                                                                        sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                        sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                        sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                        sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                        sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                        sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                        sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                        sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                        sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                        sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                        sec_ebook2_period_5, sec_ebook2_price_5)

                                if base_Category.lower() != "gm" and base_Category.lower() != "t":
                                    logger.warn("Row {} with a ISBN record {} in {} failed Category decision with {}, no record output.".format(row_counter, base_ISBN, file_name, base_Category))
                            else:
                                logger.warn("Row {} dropped, invalid ISBN/SKU.".format(row_counter))

                            # get secondary info from secondary_data_file_list

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
                                logger.debug(
                                    "sec_GenKey {}, sec_csn {}, sec_seq_no {}, sec_ISBN {}, sec_Term {}, sec_Term_Description {}, sec_dept {}, sec_course {}, sec_section {}, sec_loc_code {}, sec_bookxofy {}, sec_course_id {}, sec_instructor {}, sec_course_code {}, sec_delete_flag {}, sec_ebook_adopted {}, sec_class_cap {}, sec_prof_requested {}, sec_estimated_sales {}, sec_ebook1_vendor {}, sec_ebook1_period_1 {}, sec_ebook1_price_1 {}, sec_ebook1_period_2 {}, sec_ebook1_price_2 {}, sec_ebook1_period_3 {}, sec_ebook1_price_3 {}, sec_ebook1_period_4 {}, sec_ebook1_price_4 {}, sec_ebook1_period_5 {}, sec_ebook1_price_5 {}, sec_ebook2_vendor {}, sec_ebook2_period_1 {}, sec_ebook2_price_1 {}, sec_ebook2_period_2 {}, sec_ebook2_price_2 {}, sec_ebook2_period_3 {}, sec_ebook2_price_3 {}, sec_ebook2_period_4 {}, sec_ebook2_price_4 {}, sec_ebook2_period_5 {}, sec_ebook2_price_5 {}.".format(
                                        sec_GenKey, sec_csn, sec_seq_no, sec_ISBN, sec_Term, sec_Term_Description, sec_dept, sec_course,
                                        sec_section,
                                        sec_loc_code, sec_bookxofy, sec_course_id, sec_instructor, sec_course_code, sec_delete_flag,
                                        sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor,
                                        sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                        sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5,
                                        sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2,
                                        sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5,
                                        sec_ebook2_price_5))
                                # if DisplayOriginal False
                                if not display_original:
                                    sec_section = sec_section.replace(",", section_commas)
                                    sec_section = sec_section.replace("'", section_commas)
                                    sec_section = sec_section.replace(" ", section_commas)

                                    logger.debug("Cleaning up section field.")

                                # Clean up Usages
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

                                # set course list and dept course selection
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

                            if itemid not in all_itemids and itemid[0:2] != " -":
                                all_itemids.append(itemid)
                                db_sel = db.cursor()
                                db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                db.commit()
                                db_sel.close()
                                logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                            if use_pfi:
                                OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                   base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                   base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                   base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                   base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term,
                                                   base_Term_Description, base_Requested_Qty, base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty,
                                                   base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                   base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                   base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                   base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4,
                                                   out_text_5, usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy, sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section,
                                                   sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                   sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2,
                                                   sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                            if use_api:
                                json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                api_req_counter, command_id, api_out_line_count, itemid, regular_price,
                                                                                                                                                                                ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price,
                                                                                                                                                                                base_Used_Price_Text,
                                                                                                                                                                                base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                                                                                                                                                base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                target_date_end, base_Sale_Price1,
                                                                                                                                                                                base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty,
                                                                                                                                                                                base_Category, base_Division, base_Department, base_Class,
                                                                                                                                                                                base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty,
                                                                                                                                                                                base_Used_Warehouse_Qty,
                                                                                                                                                                                base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                                                                                                                                base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2,
                                                                                                                                                                                base_On_Order_PO3,
                                                                                                                                                                                base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used,
                                                                                                                                                                                base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                                                                                                                                out_text_2, out_text_3,
                                                                                                                                                                                out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                sec_loc_code, sec_bookxofy,
                                                                                                                                                                                sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor,
                                                                                                                                                                                sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                sec_ebook2_period_5, sec_ebook2_price_5)

                                if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                    itemid = base_Used_ISBN

                                    if itemid not in all_itemids and itemid[0:2] != " -":
                                        all_itemids.append(itemid)
                                        db_sel = db.cursor()
                                        db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                        db.commit()
                                        db_sel.close()
                                        logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                    if use_pfi:
                                        OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                           base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                           base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                           base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                           base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                           base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                           base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                           base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                           base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                           base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                           base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                           out_text_4, out_text_5, usage,
                                                           sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                           sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                           sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                           sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                           sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                    if use_api:
                                       json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                        api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                        api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                        regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                        base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                        base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                        base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                        base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                        base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                        base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                        base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                        target_date_end, base_Sale_Price1,
                                                                                                                                                                                        base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                        base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                        base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                        base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                        base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                        base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                        base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                        base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                        base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                        base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                        base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                        base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                        base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                        base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                        base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                        base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                        base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                        base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2,
                                                                                                                                                                                        base_On_Order_PO2_Vendor, base_On_Order_Qty2,
                                                                                                                                                                                        base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                        base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                        base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty,
                                                                                                                                                                                        base_image_name, out_price_1, out_price_2, out_price_3,
                                                                                                                                                                                        out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                        out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                        sec_loc_code, sec_bookxofy,
                                                                                                                                                                                        sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                        sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                        sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                        sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                        sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                        sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                        sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                        sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                        sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                        sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                        sec_ebook2_period_5, sec_ebook2_price_5)

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
                                    all_itemids.append(itemid)
                                    db_sel = db.cursor()
                                    db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                    db.commit()
                                    db_sel.close()
                                    logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                if use_pfi:
                                    OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                       base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                       base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                       base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                       base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                       base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                       base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                       base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                       base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                       base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                       base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4,
                                                       out_text_5, usage, sec_dept,
                                                       sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                       sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                       sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                       sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                       sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                if use_api:
                                   json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                    api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                    api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                    regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                    base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                    base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                    base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                    base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                    base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                                                                                                                                                    base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                    base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                    target_date_end, base_Sale_Price1,
                                                                                                                                                                                    base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                    base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                    base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                    base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                    base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                    base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                    base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                    base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                    base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                    base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                    base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                    base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                    base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                    base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                    base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                    base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                    base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                                                                                                                                    base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                    base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2,
                                                                                                                                                                                    base_On_Order_PO3,
                                                                                                                                                                                    base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used,
                                                                                                                                                                                    base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                    out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                                                                                                                                    out_text_2, out_text_3,
                                                                                                                                                                                    out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                    sec_loc_code, sec_bookxofy,
                                                                                                                                                                                    sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                    sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                    sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                    sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                    sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                    sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                    sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                    sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                    sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                    sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                    sec_ebook2_period_5, sec_ebook2_price_5)

                                if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                    itemid = base_Used_ISBN

                                    if itemid not in all_itemids and itemid[0:2] != " -":
                                        all_itemids.append(itemid)
                                        db_sel = db.cursor()
                                        db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                        db.commit()
                                        db_sel.close()
                                        logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                    if use_pfi:
                                        OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                           base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                           base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                           base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                           base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                           base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                           base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                           base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                           base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                           base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                           base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                           out_text_4, out_text_5, usage,
                                                           sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                           sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                           sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                           sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                           sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                    if use_api:
                                       json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                        api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                        api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                        regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                        base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                        base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                        base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                        base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                        base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                        base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                        base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                        target_date_end, base_Sale_Price1,
                                                                                                                                                                                        base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                        base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                        base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                        base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                        base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                        base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                        base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                        base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                        base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                        base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                        base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                        base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                        base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                        base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                        base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                        base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                        base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                        base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2,
                                                                                                                                                                                        base_On_Order_PO2_Vendor, base_On_Order_Qty2,
                                                                                                                                                                                        base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                        base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                        base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty,
                                                                                                                                                                                        base_image_name, out_price_1, out_price_2, out_price_3,
                                                                                                                                                                                        out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                        out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                        sec_loc_code, sec_bookxofy,
                                                                                                                                                                                        sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                        sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                        sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                        sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                        sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                        sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                        sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                        sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                        sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                        sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                        sec_ebook2_period_5, sec_ebook2_price_5)

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
                                    # find out haw many prices
                                    price_count = len(item_ipf_dict) - 1
                                    price_loc = ''
                                    logger.debug("price count: {}".format(price_count))

                                    # check position of ebook in sequence
                                    for i, elem in enumerate(out_text):
                                        if elem == base_Ebook_Price_Text:
                                            price_loc = i

                                        logger.debug("price count: {}, price loc: {}.".format(price_count, price_loc))
                                        out_text.pop(i)
                                        out_price.pop(i)

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
                                    logger.debug("out price 1: {}, out text 1: {}, out price 2: {}, out text 2: {}, out price 3: {}, out text 3: {}, out price 4: {}, out text 4: {}, out price 5: {}, out text 5: {}.".format(out_price_1, out_text_1,
                                                                                                                                                                                                                               out_price_2, out_text_2,
                                                                                                                                                                                                                               out_price_3, out_text_3,
                                                                                                                                                                                                                               out_price_4, out_text_4,
                                                                                                                                                                                                                               out_price_5, out_text_5))
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
                                    all_itemids.append(itemid)
                                    db_sel = db.cursor()
                                    db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                    db.commit()
                                    db_sel.close()
                                    logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                if use_pfi:
                                    OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                       base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                       base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                       base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                       base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                       base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                       base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                       base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                       base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                       base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                       base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3, out_text_4,
                                                       out_text_5, usage, sec_dept,
                                                       sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                       sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                       sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                       sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                       sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                if use_api:
                                    json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                    api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                    api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                    regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                    base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                    base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                    base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                    base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                    base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                                                                                                                                                    base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                    base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                    target_date_end, base_Sale_Price1,
                                                                                                                                                                                    base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                    base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                    base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                    base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                    base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                    base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                    base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                    base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                    base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                    base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                    base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                    base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                    base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                    base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                    base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                    base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                    base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used,
                                                                                                                                                                                    base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor,
                                                                                                                                                                                    base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2,
                                                                                                                                                                                    base_On_Order_PO3,
                                                                                                                                                                                    base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used,
                                                                                                                                                                                    base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1,
                                                                                                                                                                                    out_price_2, out_price_3, out_price_4, out_price_5, out_text_1,
                                                                                                                                                                                    out_text_2, out_text_3,
                                                                                                                                                                                    out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                    sec_loc_code, sec_bookxofy,
                                                                                                                                                                                    sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                    sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                    sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                    sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                    sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                    sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                    sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                    sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                    sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                    sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                    sec_ebook2_period_5, sec_ebook2_price_5)

                                if base_Used_ISBN != "" and base_Used_ISBN != " " and base_ISBN != " " and str(base_Used_ISBN[0:3]).lower() != "ntr" and base_ISBN != "" and base_Used_ISBN != base_ISBN:
                                    itemid = base_Used_ISBN + '-' + course_hold
                                    itemid = itemid.replace(" ", section_commas)

                                    if itemid not in all_itemids and itemid[0:2] != " -":
                                        all_itemids.append(itemid)
                                        db_sel = db.cursor()
                                        db_sel.callproc("sp_insert_mbs_itemid", [str(itemid), str(base_ISBN), str(sec_dept), str(sec_course), str(sec_section), str(base_Term)])
                                        db.commit()
                                        db_sel.close()
                                        logger.debug("Item ID not found in t_links, updating t_links with, ItemID: {}, ISBN: {}, Department: {}, Course: {} and Term: {}.".format(itemid, base_ISBN, base_Department, sec_course, sec_section, base_Term))

                                    if use_pfi:
                                        OUTFILE = send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint,
                                                           base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price,
                                                           base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1,
                                                           base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
                                                           base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4,
                                                           base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
                                                           base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                           base_Used_Store_Qty, base_Used_Warehouse_Qty, base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                           base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1, base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1,
                                                           base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3, base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                           base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                           out_text_4, out_text_5, usage,
                                                           sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
                                                           sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                           sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1, sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                           sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1, sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3,
                                                           sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5)
                                    if use_api:
                                        json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count = send_api(use_soap, json_outstring, api_responses, page_line_count,
                                                                                                                                                                                        api_out_page_count, patch_header, rest_api_url, soap_update_str,
                                                                                                                                                                                        api_req_counter, command_id, api_out_line_count, itemid,
                                                                                                                                                                                        regular_price, ITEMIPF, target_delay, base_FormatFlag,
                                                                                                                                                                                        base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR,
                                                                                                                                                                                        base_Vendor_Style, base_Publisher, base_Imprint, base_Edition,
                                                                                                                                                                                        base_Edition_Status, base_New_Price, base_New_Price_Text,
                                                                                                                                                                                        base_Used_Price, base_Used_Price_Text,
                                                                                                                                                                                        base_New_Rental_Price, base_New_Rental_Price_Text,
                                                                                                                                                                                        base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price,
                                                                                                                                                                                        base_Used_Rental_Price_Text, base_Sale_Price, target_date_start,
                                                                                                                                                                                        target_date_end, base_Sale_Price1,
                                                                                                                                                                                        base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1,
                                                                                                                                                                                        base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2,
                                                                                                                                                                                        base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2,
                                                                                                                                                                                        base_Sale_Price3, base_Sale_Start_Date3,
                                                                                                                                                                                        base_Sale_End_Date3, base_Sale_Start_Time3,
                                                                                                                                                                                        base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4,
                                                                                                                                                                                        base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
                                                                                                                                                                                        base_Term, base_Term_Description, base_Requested_Qty,
                                                                                                                                                                                        base_Class_Capacity_Qty, base_Actual_Enrollment_Qty,
                                                                                                                                                                                        base_Est_Sales_Qty, base_Category, base_Division, base_Department,
                                                                                                                                                                                        base_Class, base_New_Store_Qty, base_New_Warehouse_Qty,
                                                                                                                                                                                        base_Used_Store_Qty, base_Used_Warehouse_Qty,
                                                                                                                                                                                        base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty,
                                                                                                                                                                                        base_New_insite_Pending_Order, base_Used_insite_Pending_Order,
                                                                                                                                                                                        base_New_Rental_insite_Pending_Order,
                                                                                                                                                                                        base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
                                                                                                                                                                                        base_On_Order_PO1_Vendor, base_On_Order_Qty1,
                                                                                                                                                                                        base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2,
                                                                                                                                                                                        base_On_Order_PO2_Vendor, base_On_Order_Qty2,
                                                                                                                                                                                        base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
                                                                                                                                                                                        base_On_Order_PO3_Vendor, base_On_Order_Qty3,
                                                                                                                                                                                        base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty,
                                                                                                                                                                                        base_image_name, out_price_1, out_price_2, out_price_3,
                                                                                                                                                                                        out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
                                                                                                                                                                                        out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section,
                                                                                                                                                                                        sec_loc_code, sec_bookxofy,
                                                                                                                                                                                        sec_course_id, sec_course_list, sec_instructor, save_amount,
                                                                                                                                                                                        sec_delete_flag, sec_dept_course_section, sec_ebook_adopted,
                                                                                                                                                                                        sec_class_cap, sec_prof_requested, sec_estimated_sales,
                                                                                                                                                                                        sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
                                                                                                                                                                                        sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3,
                                                                                                                                                                                        sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4,
                                                                                                                                                                                        sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor,
                                                                                                                                                                                        sec_ebook2_period_1, sec_ebook2_price_1,
                                                                                                                                                                                        sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3,
                                                                                                                                                                                        sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4,
                                                                                                                                                                                        sec_ebook2_period_5, sec_ebook2_price_5)

                        else:
                            logger.info("Skipped due to license status")
                    if len(file_row) == 0:
                        logger.warn("Row {} in file {}, dropped no data.".format(row_counter, file_name))
                logger.debug(os.path.join(data_path, new_file))
                file_close_handler(file_content)
                # move used data file to input
                if data_in_save:
                    if not os.path.isdir(input_data_path):
                        os.makedirs(input_data_path, mode=0o777)
                        logger.debug('Successfully created {} input folder for archiving.'.format(input_data_path))
                    if new_file.endswith(".txt"):
                        shutil.move((os.path.join(data_path, new_file)), (os.path.join(input_data_path, new_file)))
                        shutil.move((os.path.join(data_path, new_file[:-4] + ".tx1")), (os.path.join(input_data_path, new_file[:-4] + ".tx1")))
                        logger.debug('Successfully moved {} data set to input folder for archiving.'.format(new_file[:-4]))
                else:
                    os.remove(os.path.join(data_path, new_file))
                    logger.info(str(os.path.join(data_path, new_file)) + ' deleted.')

            # wrap up pfi files
            if use_pfi:
                OUTFILE.close()

                logger.info("Item data file " + i1_file + " closed.")

                # process m1 file
                M1FILE = open(temp_m1_file, "a")
                logger.info(temp_m1_file + " opened for message record write.")
                M1FILE.write("UPDATE,0001,," + i1_file + "," + r7_file)
                M1FILE.close()
                logger.info("Temp message file " + temp_m1_file + " closed.")

                shutil.move(temp_m1_file, m1_file)

                logger.info(temp_m1_file + " file moved to " + m1_file + " to start PFI processing by PricerServer.")
                logger.info("Finished processing items.")
            # Clean up < 1000 records for API
            if use_api:
                if len(json_outstring) > int(api_page_count):
                    logger.warning("Length of data is to great. {}.".format(len(json_outstring)))
                if len(soap_update_str) > int(api_page_count):
                    logger.warning("Length of data is to great. {}.".format(len(soap_update_str)))
                if not use_soap:
                    if len(json_outstring) in range(0, int(api_page_count)):

                        # execute rest request and store responseid in list
                        api_responses, api_req_counter, json_outstring, api_out_page_count = send_rest_api(api_req_counter, api_out_page_count, rest_api_url, json_outstring, patch_header, api_responses)
                    else:
                        logger.debug('REST API had no data to send.')

                else:
                    if len(soap_update_str) in range(0, int(api_page_count)):
                        api_req_counter, api_responses, command_id, api_out_line_count = soap_api_updateitem(soap_update_str, api_responses, api_req_counter, api_out_line_count, api_out_page_count)
                        # get results and process

                        logger.debug('Command ID for last SOAP API request sent: ' + str(command_id))

        else:
            logger.info("No New Files to Process")
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


# ---------------------------------------------------- end main -----------------------------------------------------------------------

# ---------------------------------------------------- start subroutines --------------------------------------------------------------
def file_handler(data_file):
    file_path = data_path + data_file

    # check file for stability
    current_size = 0
    new_size = os.path.getsize(file_path)
    while new_size > current_size:
        time.sleep(0.25)
        current_size = new_size
        new_size = os.path.getsize(file_path)

    file_content = open(file_path, "r", encoding='utf-8')

    return file_content


def file_close_handler(file_content):
    file_content.close()


def send_pfi(OUTFILE, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag, base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price,
             base_New_Price_Text, base_Used_Price, base_Used_Price_Text, base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price,
             target_date_start, target_date_end, base_Sale_Price1, base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2,
             base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3, base_Sale_End_Date3, base_Sale_Start_Time3, base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4,
             base_Term, base_Term_Description, base_Requested_Qty,
             base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty,
             base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
             base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
             base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
             out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
             sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
             sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1,
             sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5):
    # determine where to send output
    new_line = "0001 " + str(itemid) + \
               " 7 0 |" + str(base_Author) + " " + str(base_Title) + \
               "| 23 0 |" + str(regular_price) + \
               "| 121 0 |" + str(ITEMIPF) + \
               "| 9800 0 |" + str(base_Author) + \
               "| 9801 0 |" + str(base_Title) + \
               "| 9802 0 |" + str(base_ISBN) + \
               "| 9803 0 |" + str(base_ISBN_HR) + \
               "| 9804 0 |" + str(base_Vendor_Style) + \
               "| 9805 0 |" + str(base_Publisher) + \
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

    return OUTFILE


def send_api(use_soap, json_outstring, api_responses, page_line_count, api_out_page_count, patch_header, rest_api_url, soap_update_str, api_req_counter, command_id, api_out_line_count, itemid, regular_price, ITEMIPF, target_delay, base_FormatFlag,
             base_Author, base_Title, base_ISBN, base_Used_ISBN, base_ISBN_HR, base_Vendor_Style, base_Publisher, base_Imprint, base_Edition, base_Edition_Status, base_New_Price, base_New_Price_Text, base_Used_Price, base_Used_Price_Text,
             base_New_Rental_Price, base_New_Rental_Price_Text, base_Ebook_Price, base_Ebook_Price_Text, base_Used_Rental_Price, base_Used_Rental_Price_Text, base_Sale_Price, target_date_start, target_date_end, base_Sale_Price1,
             base_Sale_Start_Date1, base_Sale_End_Date1, base_Sale_Start_Time1, base_Sale_End_Time1, base_Sale_Price2, base_Sale_Start_Date2, base_Sale_End_Date2, base_Sale_Start_Time2, base_Sale_End_Time2, base_Sale_Price3, base_Sale_Start_Date3,
             base_Sale_End_Date3, base_Sale_Start_Time3,
             base_Sale_End_Time3, base_Sale_Price4, base_Sale_Start_Date4, base_Sale_End_Date4, base_Sale_Start_Time4, base_Sale_End_Time4, base_Term, base_Term_Description, base_Requested_Qty,
             base_Class_Capacity_Qty, base_Actual_Enrollment_Qty, base_Est_Sales_Qty, base_Category, base_Division, base_Department, base_Class, base_New_Store_Qty, base_New_Warehouse_Qty, base_Used_Store_Qty, base_Used_Warehouse_Qty,
             base_New_Pending_Return_Qty, base_Used_Pending_Return_Qty, base_New_insite_Pending_Order, base_Used_insite_Pending_Order, base_New_Rental_insite_Pending_Order, base_Used_Rental_insite_Pending_Order, base_On_Order_PO1,
             base_On_Order_PO1_Vendor, base_On_Order_Qty1, base_On_Order_Qty1_Used, base_On_Order_Date1, base_On_Order_PO2, base_On_Order_PO2_Vendor, base_On_Order_Qty2, base_On_Order_Qty2_Used, base_On_Order_Date2, base_On_Order_PO3,
             base_On_Order_PO3_Vendor, base_On_Order_Qty3, base_On_Order_Qty3_Used, base_On_Order_Date3, base_Total_PO_Qty, base_image_name, out_price_1, out_price_2, out_price_3, out_price_4, out_price_5, out_text_1, out_text_2, out_text_3,
             out_text_4, out_text_5, usage, sec_dept, sec_course, sec_section, sec_loc_code, sec_bookxofy,
             sec_course_id, sec_course_list, sec_instructor, save_amount, sec_delete_flag, sec_dept_course_section, sec_ebook_adopted, sec_class_cap, sec_prof_requested, sec_estimated_sales, sec_ebook1_vendor, sec_ebook1_period_1, sec_ebook1_price_1,
             sec_ebook1_period_2, sec_ebook1_price_2, sec_ebook1_period_3, sec_ebook1_price_3, sec_ebook1_period_4, sec_ebook1_price_4, sec_ebook1_period_5, sec_ebook1_price_5, sec_ebook2_vendor, sec_ebook2_period_1, sec_ebook2_price_1,
             sec_ebook2_period_2, sec_ebook2_price_2, sec_ebook2_period_3, sec_ebook2_price_3, sec_ebook2_period_4, sec_ebook2_price_4, sec_ebook2_period_5, sec_ebook2_price_5):
    """
    :type api_responses: list
    :type soap_update_str: list
    """
    if not use_soap:
        # REST API
        item_out_json = {}

        item_out_json.update({"AUTHOR": str(str(base_Author)),
                              "TITLE": str(str(base_Title)),
                              "ISBN": str(base_ISBN),
                              "ISBN_HR": str(base_ISBN_HR),
                              "VENDOR_STYLE": str(base_Vendor_Style),
                              "PUBLISHER": str(base_Publisher),
                              "IMPRINT": str(base_Imprint),
                              "EDITION": str(base_Edition),
                              "NEW_PRICE": str(base_New_Price),
                              "NEW_PRICE_TEXT": str(base_New_Price_Text),
                              "USED_PRICE": str(base_Used_Price),
                              "USED_PRICE_TEXT": str(base_Used_Price_Text),
                              "NEW_RENTAL_PRICE": str(base_New_Rental_Price),
                              "NEW_RENTAL_PRICE_TEXT": str(base_New_Rental_Price_Text),
                              "EBOOK_PRICE": str(base_Ebook_Price),
                              "EBOOK_PRICE_TEXT": str(base_Ebook_Price_Text),
                              "USED_RENTAL_PRICE": str(base_Used_Rental_Price),
                              "USED_RENTAL_PRICE_TEXT": str(base_Used_Rental_Price_Text),
                              "SALE_PRICE": str(base_Sale_Price),
                              "SALE_START_DATE": str(target_date_start),
                              "SALE_END_DATE": str(target_date_end),
                              "TERM": str(base_Term),
                              "TERM_DESCRIPTION": str(base_Term_Description),
                              "REQUESTED_QTY": str(base_Requested_Qty),
                              "CLASS_CAPACITY_QTY": str(base_Class_Capacity_Qty),
                              "ACTUAL_ENROLLMENT_QTY": str(base_Actual_Enrollment_Qty),
                              "EST_SALES_QTY": str(base_Est_Sales_Qty),
                              "CATEGORY": str(base_Category),
                              "DIVISION": str(base_Division),
                              "DEPARTMENT": str(base_Department),
                              "CLASS": str(base_Class),
                              "NEW_STORE_QTY": str(base_New_Store_Qty),
                              "NEW_WAREHOUSE_QTY": str(base_New_Warehouse_Qty),
                              "USED_STORE_QTY": str(base_Used_Store_Qty),
                              "USED_WAREHOUSE_QTY": str(base_Used_Warehouse_Qty),
                              "ON_ORDER_PO1": str(base_On_Order_PO1),
                              "ON_ORDER_QTY1": str(base_On_Order_Qty1),
                              "ON_ORDER_DATE1": str(base_On_Order_Date1),
                              "ON_ORDER_PO2": str(base_On_Order_PO2),
                              "ON_ORDER_QTY2": str(base_On_Order_Qty2),
                              "ON_ORDER_DATE2": str(base_On_Order_Date2),
                              "ON_ORDER_PO3": str(base_On_Order_PO3),
                              "ON_ORDER_QTY3": str(base_On_Order_Qty3),
                              "ON_ORDER_DATE3": str(base_On_Order_Date3),
                              "TOTAL_PO_QTY": str(base_Total_PO_Qty),
                              "LOCATION_CODE": str(sec_loc_code),
                              "BOOK_X_OF_Y": str(sec_bookxofy),
                              "COURSE_ID": str(sec_course_id),
                              "COURSE_LIST": str(sec_course_list),
                              "EBOOK_ADOPTED": str(sec_ebook_adopted),
                              "CLASS_CAPACITY": str(sec_class_cap),
                              "PROF_REQUESTED": str(sec_prof_requested),
                              "ESTIMATED_SALES": str(sec_estimated_sales),
                              "NEW_PENDING_RETURN": str(base_New_Pending_Return_Qty),
                              "USED_PENDING_RETURN": str(base_Used_Pending_Return_Qty),
                              "NEW_INSITE_PENDING_ORDER": str(base_New_insite_Pending_Order),
                              "USED_INSITE_PENDING_ORDER": str(base_Used_insite_Pending_Order),
                              "NEW_RENTAL_INSITE_PENDING_ORDER": str(base_New_Rental_insite_Pending_Order),
                              "USED_RENTAL_INSITE_PENDING_ORDER": str(base_Used_Rental_insite_Pending_Order),
                              "PO_1_VENDOR": str(base_On_Order_PO1_Vendor),
                              "PO_2_VENDOR": str(base_On_Order_PO2_Vendor),
                              "PO_3_VENDOR": str(base_On_Order_PO3_Vendor),
                              "PO_1_QTY_USED": str(base_On_Order_Qty1_Used),
                              "PO_2_QTY_USED": str(base_On_Order_Qty2_Used),
                              "PO_3_QTY_USED": str(base_On_Order_Qty3_Used),
                              "USED_ISBN": str(base_Used_ISBN),
                              "SAVE_AMOUNT": str(save_amount),
                              "OUT_PRICE_1": str(out_price_1),
                              "OUT_TEXT_1": str(out_text_1),
                              "OUT_PRICE_2": str(out_price_2),
                              "OUT_TEXT_2": str(out_text_2),
                              "OUT_PRICE_3": str(out_price_3),
                              "OUT_TEXT_3": str(out_text_3),
                              "OUT_PRICE_4": str(out_price_4),
                              "OUT_TEXT_4": str(out_text_4),
                              "OUT_PRICE_5": str(out_price_5),
                              "OUT_TEXT_5": str(out_text_5),
                              "EDITION_STATUS": str(base_Edition_Status),
                              "COURSE": str(sec_course),
                              "DEPT": str(sec_dept),
                              "SECTION": str(sec_section),
                              "INSTRUCTOR": str(sec_instructor),
                              "DELETE_FLAG": str(sec_delete_flag),
                              "FORMAT_FLAG": str(base_FormatFlag),
                              "COURSE_USAGE": str(usage),
                              "DEPT_COURSE_SECTION": str(sec_dept_course_section),
                              "SALE_PRICE_1": str(base_Sale_Price1),
                              "SALE_START_DATE_1": str(base_Sale_Start_Date1),
                              "SALE_END_DATE_1": str(base_Sale_End_Date1),
                              "SALE_START_TIME_1": str(base_Sale_Start_Time1),
                              "SALE_END_TIME_1": str(base_Sale_End_Time1),
                              "SALE_PRICE_2": str(base_Sale_Price2),
                              "SALE_START_DATE_2": str(base_Sale_Start_Date2),
                              "SALE_END_DATE_2": str(base_Sale_End_Date2),
                              "SALE_START_TIME_2": str(base_Sale_Start_Time2),
                              "SALE_END_TIME_2": str(base_Sale_End_Time2),
                              "SALE_PRICE_3": str(base_Sale_Price3),
                              "SALE_START_DATE_3": str(base_Sale_Start_Date3),
                              "SALE_END_DATE_3": str(base_Sale_End_Date3),
                              "SALE_START_TIME_3": str(base_Sale_Start_Time3),
                              "SALE_END_TIME_3": str(base_Sale_End_Time3),
                              "SALE_PRICE_4": str(base_Sale_Price4),
                              "SALE_START_DATE_4": str(base_Sale_Start_Date4),
                              "SALE_END_DATE_4": str(base_Sale_End_Date4),
                              "SALE_START_TIME_4": str(base_Sale_Start_Time4),
                              "SALE_END_TIME_4": str(base_Sale_End_Time4),
                              "IMAGE_NAME": str(base_image_name),
                              "EBOOK1_VENDOR": str(sec_ebook1_vendor),
                              "EBOOK1_PERIOD1": str(sec_ebook1_period_1),
                              "EBOOK1_PRICE1": str(sec_ebook1_price_1),
                              "EBOOK1_PERIOD2": str(sec_ebook1_period_2),
                              "EBOOK1_PRICE2": str(sec_ebook1_price_2),
                              "EBOOK1_PERIOD3": str(sec_ebook1_period_3),
                              "EBOOK1_PRICE3": str(sec_ebook1_price_3),
                              "EBOOK1_PERIOD4": str(sec_ebook1_period_4),
                              "EBOOK1_PRICE4": str(sec_ebook1_price_4),
                              "EBOOK1_PERIOD5": str(sec_ebook1_period_5),
                              "EBOOK1_PRICE5": str(sec_ebook1_price_5),
                              "EBOOK2_VENDOR": str(sec_ebook2_vendor),
                              "EBOOK2_PERIOD1": str(sec_ebook2_period_1),
                              "EBOOK2_PRICE1": str(sec_ebook2_price_1),
                              "EBOOK2_PERIOD2": str(sec_ebook2_period_2),
                              "EBOOK2_PRICE2": str(sec_ebook2_price_2),
                              "EBOOK2_PERIOD3": str(sec_ebook2_period_3),
                              "EBOOK2_PRICE3": str(sec_ebook2_price_3),
                              "EBOOK2_PERIOD4": str(sec_ebook2_period_4),
                              "EBOOK2_PRICE4": str(sec_ebook2_price_4),
                              "EBOOK2_PERIOD5": str(sec_ebook2_period_5),
                              "EBOOK2_PRICE5": str(sec_ebook2_price_5)})

        json_outstring.append({"itemId": str(itemid),
                               "itemName": str(base_Author) + " " + str(base_Title),
                               "presentation": str(ITEMIPF),
                               "price": str(regular_price),
                               "properties": item_out_json})

        if target_delay != "":
            json_outstring.append({"validFrom": str(target_delay)})

        page_line_count += 1
        api_out_line_count += 1
        logger.debug("page_line_count: {}".format(page_line_count))
        if int(page_line_count) == int(api_page_count):
            if len(json_outstring) > 0:
                logger.debug("Length of Data to be sent: {}.".format(len(json_outstring)))

                api_out_page_count += 1

                api_responses, api_req_counter, json_outstring, api_out_page_count = send_rest_api(api_req_counter, api_out_page_count, rest_api_url, json_outstring, patch_header, api_responses)
                logger.info('Send page ' + str(api_out_page_count) + ' data to API')
            else:
                logger.info('REST API had no data to send.')

            if float(api_out_line_count) != float(api_page_count):
                logger.critical("REST API failed to send expected " + str(api_page_count) + " records, " + str(api_out_line_count) + " sent.")

            page_line_count = 0
            json_outstring = []
            api_out_line_count = 0

    else:
        # SOAP API

        itemproperties_str = []

        itemproperties_str.apped({'id': {'number': 7}, 'value': str(base_Author) + "-" + str(base_Title)}
            , {'id': {'number': 23}, 'value': str(regular_price)}
            , {'id': {'number': 121}, 'value': str(ITEMIPF)}
            , {'id': {'number': 9800}, 'value': str(base_Author)}
            , {'id': {'number': 9801}, 'value': str(base_Title)}
            , {'id': {'number': 9802}, 'value': str(base_ISBN)}
            , {'id': {'number': 9803}, 'value': str(base_ISBN_HR)}
            , {'id': {'number': 9804}, 'value': str(base_Vendor_Style)}
            , {'id': {'number': 9805}, 'value': str(base_Publisher)}
            , {'id': {'number': 9806}, 'value': str(base_Imprint)}
            , {'id': {'number': 9807}, 'value': str(base_Edition)}
            , {'id': {'number': 9808}, 'value': str(base_New_Price)}
            , {'id': {'number': 9809}, 'value': str(base_New_Price_Text)}
            , {'id': {'number': 9810}, 'value': str(base_Used_Price)}
            , {'id': {'number': 9811}, 'value': str(base_Used_Price_Text)}
            , {'id': {'number': 9812}, 'value': str(base_New_Rental_Price)}
            , {'id': {'number': 9813}, 'value': str(base_New_Rental_Price_Text)}
            , {'id': {'number': 9814}, 'value': str(base_Ebook_Price)}
            , {'id': {'number': 9815}, 'value': str(base_Ebook_Price_Text)}
            , {'id': {'number': 9816}, 'value': str(base_Used_Rental_Price)}
            , {'id': {'number': 9817}, 'value': str(base_Used_Rental_Price_Text)}
            , {'id': {'number': 9818}, 'value': str(base_Sale_Price)}
            , {'id': {'number': 9819}, 'value': str(target_date_start)}
            , {'id': {'number': 9820}, 'value': str(target_date_end)}
            , {'id': {'number': 9821}, 'value': str(base_Term)}
            , {'id': {'number': 9822}, 'value': str(base_Term_Description)}
            , {'id': {'number': 9823}, 'value': str(base_Requested_Qty)}
            , {'id': {'number': 9824}, 'value': str(base_Class_Capacity_Qty)}
            , {'id': {'number': 9825}, 'value': str(base_Actual_Enrollment_Qty)}
            , {'id': {'number': 9826}, 'value': str(base_Est_Sales_Qty)}
            , {'id': {'number': 9827}, 'value': str(base_Category)}
            , {'id': {'number': 9828}, 'value': str(base_Division)}
            , {'id': {'number': 9829}, 'value': str(base_Department)}
            , {'id': {'number': 9830}, 'value': str(base_Class)}
            , {'id': {'number': 9831}, 'value': str(base_New_Store_Qty)}
            , {'id': {'number': 9832}, 'value': str(base_New_Warehouse_Qty)}
            , {'id': {'number': 9833}, 'value': str(base_Used_Store_Qty)}
            , {'id': {'number': 9834}, 'value': str(base_Used_Warehouse_Qty)}
            , {'id': {'number': 9835}, 'value': str(base_On_Order_PO1)}
            , {'id': {'number': 9836}, 'value': str(base_On_Order_Qty1)}
            , {'id': {'number': 9837}, 'value': str(base_On_Order_Date1)}
            , {'id': {'number': 9838}, 'value': str(base_On_Order_PO2)}
            , {'id': {'number': 9839}, 'value': str(base_On_Order_Qty2)}
            , {'id': {'number': 9840}, 'value': str(base_On_Order_Date2)}
            , {'id': {'number': 9841}, 'value': str(base_On_Order_PO3)}
            , {'id': {'number': 9842}, 'value': str(base_On_Order_Qty3)}
            , {'id': {'number': 9843}, 'value': str(base_On_Order_Date3)}
            , {'id': {'number': 9844}, 'value': str(base_Total_PO_Qty)}
            , {'id': {'number': 9845}, 'value': str(sec_loc_code)}
            , {'id': {'number': 9846}, 'value': str(sec_bookxofy)}
            , {'id': {'number': 9847}, 'value': str(sec_course_id)}
            , {'id': {'number': 9848}, 'value': str(sec_course_list)}
            , {'id': {'number': 9849}, 'value': str(sec_ebook_adopted)}
            , {'id': {'number': 9850}, 'value': str(sec_class_cap)}
            , {'id': {'number': 9851}, 'value': str(sec_prof_requested)}
            , {'id': {'number': 9852}, 'value': str(sec_estimated_sales)}
            , {'id': {'number': 9853}, 'value': str(base_New_Pending_Return_Qty)}
            , {'id': {'number': 9854}, 'value': str(base_Used_Pending_Return_Qty)}
            , {'id': {'number': 9855}, 'value': str(base_New_insite_Pending_Order)}
            , {'id': {'number': 9856}, 'value': str(base_Used_insite_Pending_Order)}
            , {'id': {'number': 9857}, 'value': str(base_New_Rental_insite_Pending_Order)}
            , {'id': {'number': 9858}, 'value': str(base_Used_Rental_insite_Pending_Order)}
            , {'id': {'number': 9859}, 'value': str(base_On_Order_PO1_Vendor)}
            , {'id': {'number': 9860}, 'value': str(base_On_Order_PO2_Vendor)}
            , {'id': {'number': 9861}, 'value': str(base_On_Order_PO3_Vendor)}
            , {'id': {'number': 9862}, 'value': str(base_On_Order_Qty1_Used)}
            , {'id': {'number': 9863}, 'value': str(base_On_Order_Qty2_Used)}
            , {'id': {'number': 9864}, 'value': str(base_On_Order_Qty3_Used)}
            , {'id': {'number': 9865}, 'value': str(base_Used_ISBN)}
            , {'id': {'number': 9885}, 'value': str(save_amount)}
            , {'id': {'number': 9886}, 'value': str(out_price_1)}
            , {'id': {'number': 9887}, 'value': str(out_text_1)}
            , {'id': {'number': 9888}, 'value': str(out_price_2)}
            , {'id': {'number': 9889}, 'value': str(out_text_2)}
            , {'id': {'number': 9890}, 'value': str(out_price_3)}
            , {'id': {'number': 9891}, 'value': str(out_text_3)}
            , {'id': {'number': 9892}, 'value': str(out_price_4)}
            , {'id': {'number': 9893}, 'value': str(out_text_4)}
            , {'id': {'number': 9894}, 'value': str(out_price_5)}
            , {'id': {'number': 9895}, 'value': str(base_Edition_Status)}
            , {'id': {'number': 9896}, 'value': str(out_text_5)}
            , {'id': {'number': 9897}, 'value': str(sec_course)}
            , {'id': {'number': 9898}, 'value': str(sec_dept)}
            , {'id': {'number': 9899}, 'value': str(sec_section)}
            , {'id': {'number': 9900}, 'value': str(sec_instructor)}
            , {'id': {'number': 9902}, 'value': str(sec_delete_flag)}
            , {'id': {'number': 9903}, 'value': str(base_FormatFlag)}
            , {'id': {'number': 9914}, 'value': str(usage)}
            , {'id': {'number': 9915}, 'value': str(sec_dept_course_section)}
            , {'id': {'number': 9916}, 'value': str(base_Sale_Price1)}
            , {'id': {'number': 9917}, 'value': str(base_Sale_Start_Date1)}
            , {'id': {'number': 9918}, 'value': str(base_Sale_End_Date1)}
            , {'id': {'number': 9919}, 'value': str(base_Sale_Start_Time1)}
            , {'id': {'number': 9920}, 'value': str(base_Sale_End_Time1)}
            , {'id': {'number': 9921}, 'value': str(base_Sale_Price2)}
            , {'id': {'number': 9922}, 'value': str(base_Sale_Start_Date2)}
            , {'id': {'number': 9923}, 'value': str(base_Sale_End_Date2)}
            , {'id': {'number': 9924}, 'value': str(base_Sale_Start_Time2)}
            , {'id': {'number': 9925}, 'value': str(base_Sale_End_Time2)}
            , {'id': {'number': 9926}, 'value': str(base_Sale_Price3)}
            , {'id': {'number': 9927}, 'value': str(base_Sale_Start_Date3)}
            , {'id': {'number': 9928}, 'value': str(base_Sale_End_Date3)}
            , {'id': {'number': 9929}, 'value': str(base_Sale_Start_Time3)}
            , {'id': {'number': 9930}, 'value': str(base_Sale_End_Time3)}
            , {'id': {'number': 9931}, 'value': str(base_Sale_Price4)}
            , {'id': {'number': 9932}, 'value': str(base_Sale_Start_Date4)}
            , {'id': {'number': 9933}, 'value': str(base_Sale_End_Date4)}
            , {'id': {'number': 9934}, 'value': str(base_Sale_Start_Time4)}
            , {'id': {'number': 9935}, 'value': str(base_Sale_End_Time4)}
            , {'id': {'number': 9936}, 'value': str(base_image_name)}
            , {'id': {'number': 9937}, 'value': str(sec_ebook1_vendor)}
            , {'id': {'number': 9938}, 'value': str(sec_ebook1_period_1)}
            , {'id': {'number': 9939}, 'value': str(sec_ebook1_price_1)}
            , {'id': {'number': 9940}, 'value': str(sec_ebook1_period_2)}
            , {'id': {'number': 9941}, 'value': str(sec_ebook1_price_2)}
            , {'id': {'number': 9942}, 'value': str(sec_ebook1_period_3)}
            , {'id': {'number': 9943}, 'value': str(sec_ebook1_price_3)}
            , {'id': {'number': 9944}, 'value': str(sec_ebook1_period_4)}
            , {'id': {'number': 9945}, 'value': str(sec_ebook1_price_4)}
            , {'id': {'number': 9946}, 'value': str(sec_ebook1_period_5)}
            , {'id': {'number': 9947}, 'value': str(sec_ebook1_price_5)}
            , {'id': {'number': 9948}, 'value': str(sec_ebook2_vendor)}
            , {'id': {'number': 9949}, 'value': str(sec_ebook2_period_1)}
            , {'id': {'number': 9950}, 'value': str(sec_ebook2_price_1)}
            , {'id': {'number': 9951}, 'value': str(sec_ebook2_period_2)}
            , {'id': {'number': 9952}, 'value': str(sec_ebook2_price_2)}
            , {'id': {'number': 9953}, 'value': str(sec_ebook2_period_3)}
            , {'id': {'number': 9954}, 'value': str(sec_ebook2_price_3)}
            , {'id': {'number': 9955}, 'value': str(sec_ebook2_period_4)}
            , {'id': {'number': 9956}, 'value': str(sec_ebook2_price_4)}
            , {'id': {'number': 9957}, 'value': str(sec_ebook2_period_5)}
            , {'id': {'number': 9958}, 'value': str(sec_ebook2_price_5)})

        if target_delay != "":
            itemproperties_str.append({'id': {'number': 9500}, 'value': str(target_delay)})

        soap_update_str.append({'itemId': itemid, 'itemProperties': itemproperties_str})

        page_line_count += 1
        api_out_line_count += 1
        logger.debug("page_line_count: {}".format(page_line_count))
        if int(page_line_count) == int(api_page_count):
            logger.debug("Length of Data to be sent: {}.".format(len(soap_update_str)))
            api_out_page_count += 1

            api_req_counter, api_responses, command_id, api_out_line_count = soap_api_updateitem(soap_update_str, api_responses, api_req_counter, api_out_line_count, api_out_page_count)

            logger.info('Command ID for last SOAP API request sent: ' + str(command_id))

            if float(api_out_line_count) != float(api_page_count):
                logger.critical("SOAP API failed to send expected " + str(api_page_count) + " records, " + str(api_out_line_count) + " sent.")

            page_line_count = 0
            soap_update_str = []
            api_out_line_count = 0

    return json_outstring, soap_update_str, api_req_counter, api_out_page_count, api_responses, command_id, page_line_count, api_out_line_count


def soap_api_updateitem(soap_update_str, api_responses, api_req_counter, api_out_line_count, api_out_page_count):
    """

    :type api_responses: list
    """
    good_content = 0

    if len(soap_update_str) > 0:
        if api_out_page_count == 1:
            logger.info('Starting to send data to Pricer API.')
        logger.debug("soap_update_str: {}.".format(soap_update_str))
        logger.debug("soap ip: {}.".format('http://' + soap_api_ip + '/pricer_5_0?wsdl'))
        logger.debug("soap user: {}.".format(soap_user))
        logger.debug("soap token: {}.".format(soap_token))
        command_id = client.service.updateItems(soap_update_str)
        api_req_counter += 1
        good_content = 1

        if command_id != None:
            try:
                api_responses.append(int(command_id))

            except ValueError:
                logger.critical("Last Update failed to return valid command id.")
        else:
            logger.critical("Last Update failed to return valid command id.")

    if good_content == 0:
        logger.info("No data to write to SOAP API.")

    return api_req_counter, api_responses, command_id, api_out_line_count


def send_rest_api(api_req_counter, api_out_page_count, rest_api_url, json_outstring, patch_header, api_responses):
    """

    :type api_responses: list
    """
    # execute rest request and store responseid in list
    if api_out_page_count == 1:
        logger.info('Starting to send data to Pricer API.')
    logger.debug("Json outstring: {}.".format(json_outstring))
    logger.debug("Patch Header: {}.".format(patch_header))
    logger.debug("REST API url: {}.".format(rest_api_url))
    response_id = (json.loads(requests.patch(rest_api_url, json=json_outstring, headers=patch_header).content).get('requestId'))
    api_responses.append(response_id)
    # logger.debug("ResponseId for last REST API request sent: " + str(response_id) + ".")

    api_req_counter += 1

    return api_responses, api_req_counter, json_outstring, api_out_page_count


# -----------------------------------------------------------------------end subroutines ---------------------------------------------------------------

if __name__ == '__main__':
    main()
