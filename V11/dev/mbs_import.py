import winreg
import os
import EventLogger
import sys
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket

# get file name without extension
file_name_raw = __file__
file_name_start = file_name_raw.rfind('/') + 1
file_name = file_name_raw[file_name_start: -3]

# read registry
try:
    reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, "Software\MarginMate", 0, winreg.KEY_READ)
    license_key, regtype = winreg.QueryValueEx(reg_key, "MM_NFM_LicenseKey")
    period, regtype = winreg.QueryValueEx(reg_key, "MM_NFM_Value")
    command, regtype = winreg.QueryValueEx(reg_key, "MM_NFM_Command")
    arguments, regtype = winreg.QueryValueEx(reg_key, "MM_NFM_Parameters")

except FileNotFoundError:
    EventLogger.event_log_writer(file_name + " failed to read registry values.", "error", "Pricer", 101, '')
    sys.exit(99)

period = int(period)

class Scheduler(win32serviceutil.ServiceFramework):
    _svc_name_ = "MarginMate MBS Import Processor"
    _svc_display_name_ = "MarginMate MBS Import Processor"
    _svc_description_ = "Executes MarginMate NFM Import Processor Core Components on scheduled interval."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED, (self._svc_name_,''))
        EventLogger.event_log_writer(file_name, "info", "Pricer", 100, '')
        self.main()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def main(self):

        if license_key == "ov1IXz17JSOrvOTPhyX5":
            period_timer = None

            while period_timer != win32event.WAIT_OBJECT_0:
                try:
                    if arguments != "":
                        EventLogger.event_log_writer(file_name + " executing " + command + " with parameters " + arguments + " on schedule.", "info", "Pricer", 100, '')
                        os.system(command + " " + arguments)
                    else:
                        EventLogger.event_log_writer(file_name + " executing " + command + " on schedule.", "error", "Pricer", 100, '')
                        os.system(command)

                except IOError as e:
                    EventLogger.event_log_writer(command + " failed to execute. I/O error({0}): {1}".format(e.errno, e.strerror), "info", "Pricer", 101, '')
                    sys.exit(99)

                period_timer = int(win32event.WaitForSingleObject(self.hWaitStop, period))

        else:
            EventLogger.event_log_writer(file_name + " failed license check.", "error", "Pricer", 101, '')
            sys.exit(99)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(Scheduler)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(Scheduler)
