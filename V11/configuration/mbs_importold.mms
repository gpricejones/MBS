<?xml version="1.0" encoding="utf-8"?>
<!--
Logging controls whether log file is written - False = no log, True = Writes log file file to path defined in Path. Delete_after defines old log file lifespan in days (zero to never delete)
Interactive controls whether "Press Enter to Continue" is displayed onscreen at end of execution.  False = No, True = Yes.
Verbose defines logged detail: Yes = Full detail, No = summary only.
LocalConn defines MySQL connection parameters for Pricer database.
Data Path defines directory to serach for input files. Delete defines whether to delete input file once processed (True) or to move it to processed directory (False).
Sort Order defines output order of price1-price5.  Natural is input order, Ascending is smallest price first then in ascending order
Price None_100 defines whether all zeros in price fields defaults to IPF100 (True) or IPF104 (False)
Usage Convert changes usage "Required" to "RQ" and "Recommended" to "RC"
Zero_Zero allows selection of IPF100 when ISBN starts with 281
Date format allows alteration of standard date format (yyyymmdd) to preferred format.
Allowable date formats are: YYYYMMDD, MM/DD/YYYY, DD/MM/YYYY, MM-DD-YYYY, DD-MM-YYYY, MM/DD/YY, DD/MM/YY, MM-DD-YY, DD-MM-YY, YYYY-MM-DD, YYYY/MM/DD
-->

<MMImport Logging="True" Interactive="False" Verbose="True" Path="C:\pricer\import\logs\" Delete_After="0">
  <LocalConn Server="localhost" 
  DB="Pricer" 
  Uid="CZKDW4MD8fL7PaLtF+b4SsyRigzUEPRgslPC7VHWbYScTyAuClixKQ==" 
  Port="wPTWkLXAL/2zAlU9FtQIVlbJA/jnQC4FjEEvYx9nb1MHmTNBk+aNQA==" 
  Pass="RECOWeuNuxikaDYoogzbz9b2bBnSYOwP6A55jHtYqO4=" 
  pb="RECOWeuNuxikaDYoogzbz9b2bBnSYOwP6A55jHtYqO4=" 
  pp="x3RkC1hEU2B8gVIKZQqEZx9HtbG6uFUhlKm/tlGL+r5MCebLw8aVzA==" 
  ur="CZKDW4MD8fL7PaLtF+b4SsyRigzUEPRgslPC7VHWbYScTyAuClixKQ==" 
  up="x3RkC1hEU2B8gVIKZQqEZx9HtbG6uFUhlKm/tlGL+r5MCebLw8aVzA==" />
  <Data Path="C:\Pricer\Import\data\" Delete="False" Input_Delete_After="0" />
  <Sort Order="Natural"/>
  <Price None_100="False"/>
  <Usage Convert="True"/>
  <Zero_Zero IPF100="True"/>
  <Term Multi="True"/>
  <Date Format="MM/DD/YYYY"/>
  <IPF1x5 Threshold="0" />
  <Replace_Section_Commas Char="-" DisplayOriginal="True" />
  <PFI i1_path="c:\pricer\pfifiles\datafiles\" m1_path="c:\pricer\pfifiles\messagefiles\" r7_path="c:\pricer\pfifiles\resultfiles\" PFI_Delete_After="0" />
  <QOH_Calc New_On_Hand="Add" Used_On_Hand="Add" New_Addl="Add" Used_Addl="Add" New_Pend_Ret="Add" Used_Pend_Ret="Add" New_insite_Pend_Ord="Add" Used_insite_Pend_Ord="Add" New_Rental_insite_Pend_Ord="Add" Used_Rental_insite_Pend_Ord="Add" />
  <Pricer Version="R4" />
</MMImport>