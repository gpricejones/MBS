<?xml version="1.0" encoding="utf-8"?>
<!--
Time is set to specific local time for once-per-day execution, "24:00:00" indicates more frequent execution.  Time set must be 2 digits each for hh,mm and ss (add leading zeros if required)
Value is execution interval in milliseconds when Time="24:00:00"  Value="0" indicates once-per-day execution at Time.
Command Path points to program to execute.
Argumants contains all switches and configuration file information.
LocalLog Path defines logging path, file name created by code.
-->

<MMSchedule Time="24:00:00" Value="5000" StoreID="1">
  <Command Path="C:\Pricer\import\bin\mbs_import_v4.exe" Arguments="c:\Pricer\import\configuration\mbs_import.mms" Pause="0" Step="True" Name="Data Update">
  </Command>
  <LocalLog Path="C:\Pricer\import\logs\" Logging="True" Verbose="True"/>
</MMSchedule>