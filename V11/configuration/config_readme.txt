MBS import config guide

this will hopfully guide most people through the setup of the mbs_import.mms file

lets start with <MMImport/>

Drive: pretty simple just the drive letter, eg: c or e .
Pricer_Folder: location of  where pricer was installed so if pricer was installed like so (c:\pricer) then just put "pricer" or if it was installed else where eg (c:\folder\pricer) then put "folder\pricer"
R3Server_Folder: this should never change, but in the off chance that it does then change to the revised folder name default = "r3server"
Pricer_Level_File: this should never change, but in the off chance that it does then change to the revised folder name default = "log4j2.xml"

<Logging/>
Level: logging level defualt shouild be "Info", "debug" should only be used when problems arise since logs will grow expedentially 
Use_Pricer_Level: allows logging based on the value of the pricer software logging if set to "True" logging levels are adjusted in the pricer software defualt = "False" 
Path: location of folder for the logs file to be saved. Uses the drive set as drive letter "pricer\R3Server\logs\"
Log_Name: allows for the log file to be renamed as seen fit, default = "data_import.log"
Delete_After: deletes archived log files after set amount of days.
Log_Max_MB: sets a maximum file size before zipping log file for achiving default = "100"

<DataLocal/>
UsePricer: defualt set to "True" unless a plaza instance set up then "False"
LocalDB: must be encrypted using built in encrypter. encrypted version of '{"ip":"ip address", "port":"port", "dbName":"database name", "user":"user", "pw":"password"}' must have all values assigned if usepricerdb set to "false"

<DataIn/>
DataInputSave: set to "True" if you want data saved for x amount of days after processing, if set to "False" data gets removed after processing
DeleteAfter: sets the x amount of days after data processed, for clean up if set  DataInputSave to False value should be "0"
Path: folder for the data to be dropped to for processing "Pricer\import\data\"
InputHoldPath: folder for the data to be moved to if DataInputSave set to "True" default should be set as "Pricer\Import\data\input\"
FileWaitTim: time to wait in seconds for file transfer to be complete "60"

<Sort/>
Order: defines output order of price1-price5. "Ascending" is alphanumeric sorted, "Normal" is as the input order, and "Price" is lowest price to highest.

<Price/>
None_100: defines whether all zeros in price fields defaults to IPF100 "True" or IPF104 "False"

<Usage/>
Convert: "True" changes usage "Required" to "RQ", "Recommended" to "RC" and "Optional" "OP"

<Zero_Zero/>
IPF100: "True" allows selection of IPF100 when ISBN starts with 281

<Term/>
Multi: "True" 

<Date/>
Format="MM/DD/YYYY" format allows alteration of standard date format (yyyymmdd) to preferred format.
Allowable date formats are: YYYYMMDD, MM/DD/YYYY, DD/MM/YYYY, MM-DD-YYYY, DD-MM-YYYY, MM/DD/YY, DD/MM/YY, MM-DD-YY, DD-MM-YY, YYYY-MM-DD, YYYY/MM/DD

<IPF1x5/>
Threshold="0"

<Replace_Section_Commas/>
Char="-"
DisplayOriginal="True"

<DataOut/>

UsePFI="False"
UseAPI="False"
UseSoap="False"
Depending on output used if ,PFI only set UsePFI to "True", if REST only set UseAPI to "True" if SOAP set both UseAPI and UseSoap to "True" 
i1_Path="pricer\PFIFiles\DataFiles\"
m1_Path="pricer\PFIFiles\MessageFiles\"
r7_Path="pricer\PFIFiles\ResultFiles\"
Default locations for the PFI created files
APIPageCount="1000" MAX is 1000 dont change
SoapIP=must be encrypted using built in encrypter. "ipaddress:11097" 
SoapToken=must be encrypted using built in encrypter. API token generated from within the pricer system once set dont change 
RestURL=must be encrypted using built in encrypter. pricer system url once set dont change
PricerUser=must be encrypted using built in encrypter. pricer system user, if using soap and user is changed soap token must be revised
PricerPassword=must be encrypted using built in encrypter. pricer system password


<Alerts/>
WELLogging="True" (Windows Event Log Logging) watches the WindowsEvent Log and can notify via email, the critical errors reported and send .zip file of the logs to recipent for debugging
WELLogName="Pricer"
Email="False" set to "True" to allow emails to be sent to users in the to_list, cc_list, bss_list.
EmailHost="tQVB53TbUIM="
EmailHello="tQVB53TbUIM="
EmailPort="tQVB53TbUIM="
EmailUser="tQVB53TbUIM="
EmailUPW="tQVB53TbUIM="
EmailTo_List="tQVB53TbUIM="
Emailcc_List="tQVB53TbUIM="
Emailbcc_List="tQVB53TbUIM="
EmailUID="tQVB53TbUIM="
EmailFrom="tQVB53TbUIM="
