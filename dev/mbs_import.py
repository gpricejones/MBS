import os
import sys
import time
import socket
import winreg
import subprocess
import win32event
import EventLogger
import win32service
import servicemanager
import win32serviceutil

##get file name without extension
drive_letter, tail = os.path.splitdrive(__file__)
path, file = os.path.split(tail)
file_name = file[:-3]

# read registry
try:
    reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, "Software\MarginMate", 0, winreg.KEY_READ | winreg.KEY_WOW64_32KEY)
    license_key = winreg.QueryValueEx(reg_key, "MM_MBS_LicenseKey")[0]
    period = winreg.QueryValueEx(reg_key, "MM_MBS_Value")[0]
    time_out_value = winreg.QueryValueEx(reg_key, "MM_MBS_TimeOut")[0]
    command = winreg.QueryValueEx(reg_key, "MM_MBS_Command")[0]
    arguments = winreg.QueryValueEx(reg_key, "MM_MBS_Parameters")[0]
    execution_time_out_value = winreg.QueryValueEx(reg_key, "MM_MBS_Execution_TimeOut")[0]

except FileNotFoundError:
    # this print, prints directly to cmd when installing the service.
    print("{} failed to read required values from 32bit registry.".format(file_name))
    try:
        reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, "SOFTWARE\MarginMate", 0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY)
        license_key = winreg.QueryValueEx(reg_key, "MM_MBS_LicenseKey")[0]
        period = winreg.QueryValueEx(reg_key, "MM_MBS_Value")[0]
        time_out_value = winreg.QueryValueEx(reg_key, "MM_MBS_TimeOut")[0]
        command = winreg.QueryValueEx(reg_key, "MM_MBS_Command")[0]
        arguments = winreg.QueryValueEx(reg_key, "MM_MBS_Parameters")[0]
        execution_time_out_value = winreg.QueryValueEx(reg_key, "MM_MBS_Execution_TimeOut")[0]

    except FileNotFoundError:
        # this print, prints directly to cmd when installing the service.
        print("{} failed to read required registry values from 64bit registry.".format(file_name))
        EventLogger.event_log_writer("{} failed to read required registry values from 32bit and 64bit registry.".format(file_name), "error", "Pricer", 101, '')
        sys.exit(99)

except Exception as e:
    EventLogger.event_log_writer("{} failed to read required registry values from 32bit and 64bit registry with error: {}.".format(file_name, sys.exc_info()[0]), "error", "Pricer", 101, '')
    print("Uncatagorised error occurred: {}".format(sys.exc_info()[0]))

period = int(period)


class Scheduler(win32serviceutil.ServiceFramework):
    _svc_name_ = "MarginMate MBS Import Processor"
    _svc_display_name_ = "MarginMate MBS Import Processor"
    _svc_description_ = "Executes MarginMate MBS Import Processor Core Components on scheduled interval."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED, (self._svc_name_, ''))
        EventLogger.event_log_writer(file_name, "info", "Pricer", 100, '')
        self.main()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def main(self):

        if license_key == "ov1IXz17JSOrvOTPhyX5":
            period_timer = None

            while period_timer != win32event.WAIT_OBJECT_0:
                if arguments != "":

                    retry_count = 1
                    service_not_running_list = []

                    pricerserver = 'PricerServer'
                    pricermysql = 'PricerMysql'

                    running = False

                    while not running and retry_count <= int(time_out_value):
                        try:

                            pricer_server = win32serviceutil.QueryServiceStatus(pricerserver)
                            pricer_mysql = win32serviceutil.QueryServiceStatus(pricermysql)
                        except:
                            EventLogger.event_log_writer("{} failed to find a service like that installed.".format(file_name), "error", "Pricer", 101, '')
                            sys.exit(99)
                        else:
                            if str(pricer_server[1]) == '4' and str(pricer_mysql[1]) == '4':
                                running = True
                                break
                            else:
                                if str(pricer_server[1]) != '4':
                                    if pricerserver not in service_not_running_list:
                                        service_not_running_list.append(pricerserver)
                                if str(pricer_mysql[1]) != '4':
                                    if pricermysql not in service_not_running_list:
                                        service_not_running_list.append(pricermysql)
                                retry_count += 1
                                time.sleep(1)
                                if retry_count >= int(time_out_value):
                                    EventLogger.event_log_writer("{} failed to start, {} required services are not running.".format(file_name, service_not_running_list), "error", "Pricer", 101, '')
                                    sys.exit(99)

                        try:
                            EventLogger.event_log_writer("{} executing {} with parameters {} on schedule.".format(file_name, command, arguments), "info", "Pricer", 100, '')
                            subprocess.Popen([command, arguments]).communicate(timeout=int(execution_time_out_value))
                        except subprocess.TimeoutExpired:
                            EventLogger.event_log_writer("{} failed to complete in a timely manner, exiting execution. Error: {}".format([command, arguments], e), "error", "Pricer", 101, '')
                        except Exception as e:
                            EventLogger.event_log_writer("{} failed to execute. Error: {}".format([command, arguments], e), "error", "Pricer", 101, '')
                    else:
                        os.system(command)
                        EventLogger.event_log_writer("{} executing {} on schedule.".format(file_name, command), "error", "Pricer", 101, '')

                    period_timer = int(win32event.WaitForSingleObject(self.hWaitStop, period))

            else:
                EventLogger.event_log_writer(file_name + " failed license check.", "error", "Pricer", 101, '')
                sys.exit(99)


if __name__ == '__main__':

    _svc_display_name_ = "MarginMate MBS Import Processor"

    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(Scheduler)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(Scheduler)
        if "install" in sys.argv:
            EventLogger.event_log_writer("{} successfully installed {}.".format(file_name, _svc_display_name_), "info", "Pricer", 104, '')
        if "remove" in sys.argv:
            EventLogger.event_log_writer("{} successfully removed {}.".format(file_name, _svc_display_name_), "info", "Pricer", 104, '')