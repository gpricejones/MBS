import AlertManager
import base64
from datetime import datetime
import DBhandler
import DirectoryCleaner
import EventLogger
import hashlib
import json
import LicenseValidator
import LogCleaner
import Logger
import os
from pyDes import *
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import shutil
from StdTime import time, file_time, s_stamp, year_stamp, simple_date, simple_hours, simple_minutes
import sys
import time
import traceback
from bs4 import BeautifulSoup

def main():

    try:

        origin_time = time.time()

        global file_name, drive_letter, log_level, log_file, log_path, log_delete_after, log_max, use_pricer_level, logger, data_path, data_delete, data_input_delete_after, input_data_path, page_size, page_delay, use_checksum, token, item_url, promo_url, use_api, use_soap, api_page_count, rest_api_url, rest_token, soap_api_ip, soap_token, soap_user, total_api_req_counter
        global requests, db, client, config_path, license_status, pricer_api_alive, origin_time

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
                config_path = "c:\pricer\import\configuration\\mbs_import.mms"
            else:
                config_path = "/pricer/import/configuration/mbs_import.mms"

        try:
            with open(config_path) as config_source:
                content = config_source.read()
            config = BeautifulSoup(content, 'html.parser')
            ###drive
            drive_letter = config.mmimport.get('drive')
            ###logging
            log_level = config.logging.get('level').lower()
            log_delete_after = config.logging.get('delete_after')
            log_max = config.logging.get('log_max_mb')
            log_name = config.logging.get('log_name')
            log_path = config.logging.get('path')

            log_file = drive_letter + log_path + config.logging.get('log_name')

            if os.name != 'nt':
                log_file = log_file.replace('\\', '/')
                log_path = log_path.replace('\\', '/')

            ##########set up logging
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
            input_data_path = config.datain.get('path')


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
            if current_file[:-4] + '.tx1' in sorted(file_tx1_list, key=file_tx1_list.get, reverse=False):
                file_list_new.append(current_file)

        for current_file in sorted(file_tx1_list, key=file_tx1_list.get, reverse=False):
            if current_file[:-4] + '.txt' in sorted(file_txt_list, key=file_txt_list.get, reverse=False):
                file_list_new.append(current_file)

        if len(file_list) > 0:

            api_out_page_count = 0
            api_out_line_count = 0
            page_line_count = 0
            out_count = 0
            row_counter = 0

            json_outstring = []
            soap_update_str = []

            # process each file

            for new_file in file_list_new:
                file_content = file_handler(new_file)
                logger.info("Start processing output from {} to API".format(new_file))

                if new_file.lower().endswith("txt"):

                    for file_row in file_content:
                        row_counter += 1
                        file_row = file_row.rstrip(os.linesep)
                        row_fields = file_row.split('|')

                if new_file.lower().endswith("tx1"):

                    for file_row in file_content:
                        row_counter += 1
                        file_row = file_row.rstrip(os.linesep)
                        row_fields = file_row.split('|')


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
    file_path = data_path + data_file

    # check file for stability
    current_size = 0
    new_size = os.path.getsize(file_path)
    while new_size > current_size:
        time.sleep(0.25)
        current_size = new_size
        new_size = os.path.getsize(file_path)

    file_content = open(data_path + data_file, "r", encoding='utf-8')

    return file_content
#################################################end subroutines#############################################################

if __name__ == '__main__':
    main()