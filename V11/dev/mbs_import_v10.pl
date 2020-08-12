use strict;

#use warnings::unused;
#use warnings 'once'; 

use Crypt::Simple passphrase=>'urefhgaorehgberfhbf';
use DateTime;
use DBI;
use File::stat;
use File::Copy;

use MM_CodeBase::DirectoryClean ('directory_cleaner');
use MM_CodeBase::EventLogWriter;
use MM_CodeBase::GetStdTime;
use MM_CodeBase::LicenseValidate ('license_validator');
use MM_CodeBase::LogWriter ('log_writer');

use String::Util 'trim';
use Time::localtime;
use Win32::EventLog;
use XML::Simple;

sub write_line;
sub write_line_delayed;

my $exe_file = substr(__FILE__,0, length(__FILE__)-3);
my $GenKey = "";
my $FormatFlag = "";
my $Author = "";
my $Title = "";
my $ISBN = "";
my $ISBN_HR = "";
my $Used_ISBN = "";
my $Vendor_Style = "";
my $Publisher = "";
my $Imprint = "";
my $Edition = "";
my $Edition_Status = "";
my $New_Price = "";
my $New_Price_Text = "";
my $Used_Price = "";
my $Used_Price_Text = "";
my $New_Rental_Price = "";
my $New_Rental_Price_Text = "";
my $Ebook_Price = "";
my $Ebook_Price_Text = "";
my $Used_Rental_Price = "";
my $Used_Rental_Price_Text = "";
my $Regular_Price = "";
my $Sale_Price1 = "";
my $Sale_Start_Date1 = "";
my $Sale_End_Date1 = "";
my $Sale_Start_Time1 = "";
my $Sale_End_Time1 = "";
my $Sale_Price2 = "";
my $Sale_Start_Date2 = "";
my $Sale_End_Date2 = "";
my $Sale_Start_Time2 = "";
my $Sale_End_Time2 = "";
my $Sale_Price3 = "";
my $Sale_Start_Date3 = "";
my $Sale_End_Date3 = "";
my $Sale_Start_Time3 = "";
my $Sale_End_Time3 = "";
my $Sale_Price4 = "";
my $Sale_Start_Date4 = "";
my $Sale_End_Date4 = "";
my $Sale_Start_Time4 = "";
my $Sale_End_Time4 = "";
my $Term = "";
my $Term_Description = "";
my $Requested_Qty = "";
my $Class_Capacity_Qty = "";
my $Actual_Enrollment_Qty = "";
my $Est_Sales_Qty = "";
my $Category = "";
my $Division = "";
my $Department = "";
my $Class = "";
my $New_Store_Qty = "";
my $New_Warehouse_Qty = "";
my $Used_Store_Qty = "";
my $Used_Warehouse_Qty = "";
my $On_Order_PO1 = "";
my $On_Order_Qty1 = "";
my $On_Order_Date1 = "";
my $On_Order_PO2 = "";
my $On_Order_Qty2 = "";
my $On_Order_Date2 = "";
my $On_Order_PO3 = "";
my $On_Order_Qty3 = "";
my $On_Order_Date3 = "";
my $Total_PO_Qty = "";
my $New_Pending_Return_Qty = "";
my $Used_Pending_Return_Qty = "";
my $New_insite_Pending_Order = "";
my $Used_insite_Pending_Order = "";
my $New_Rental_insite_Pending_Order = "";
my $Used_Rental_insite_Pending_Order = "";
my $On_Order_PO1_Vendor = "";
my $On_Order_PO2_Vendor = "";
my $On_Order_PO3_Vendor = "";
my $On_Order_Qty1_Used = "";
my $On_Order_Qty2_Used = "";
my $On_Order_Qty3_Used = "";

my $dept_course_section = "";
my $image_name = "";
my $display_original = "";
my $original_section = ""; 

my $ITEMIPF = "";
my $new_line = "";
my $out_price_1 = 0;
my $out_text_1 = "";
my $out_price_2 = 0;
my $out_text_2 = "";
my $out_price_3 = 0;
my $out_text_3 = "";
my $out_price_4 = 0;
my $out_text_4 = "";
my $out_price_5 = 0;
my $out_text_5 = "";

my $out_price_1_hold = 0;
my $out_text_1_hold = "";
my $out_price_2_hold = 0;
my $out_text_2_hold = "";
my $out_price_3_hold = 0;
my $out_text_3_hold = "";
my $out_price_4_hold = 0;
my $out_text_4_hold = "";
my $out_price_5_hold = 0;
my $out_text_5_hold = "";
my $ITEMIPF_hold = "";

my %out_text = ();
my %out_price = ();
my @file_list = ();
my $file_name = "";
my $file = "";
my $in_file = "";
my $out_file = "";
my $out_count = "";
my $temp_file = "";
my $line = "";
my $in_count = "";
my $xml = "";
my $config_file = "";
my $data = "";
my $data_path = "";
my $Logging = "";
my $Interactive = "";
my $verbose = "";
my $Log_Path = "";
my $LocalConn_Server = "";
my $LocalConn_DB = "";
my $LocalConn_Pass = "";
my $LocalConn_Port = "";
my $LocalConn_Uid = "";
my $input_delete_after = "";
my $LogFile = "";
my $resp = "";
my @course = ();
my $Total_OH = 0;
my $sort_order = "";
my @sort = ();
my $sort_counter = 0;
my %sorted_price = ();
my %sorted_text = ();
my $log_short = "";
my $log_file = "";
my $csec = "";
my $cmin = "";
my $chour = "";
my $cmday = "";
my $cmon = "";
my $cyear = "";
my $now = "";
my $short_now = "";
my $short_year = "";
my $save_amount = "";
my $itemid = "";
my $course_hold = "";
my $delete_flag = "0";

my $find = " ";
my $replace = "-";
my $replace1 = "QzQ";
my $s_find = '-';
my $s_replace = '&';

my $dsn = "";
my $dbh = "";
my $select_str = "";
my $db_str = ""; 
my $ex_db = "";
my $selectdb = "";
my $selectdb1 = "";
my $row = "";
my $row1 = "";
my $insertdb = "";
my $v_dept = "";
my $v_course = "";
my $v_section = "";
my $v_instructor = "";
my $v_usage = "";
my $v_delete = "";

my $last_size = 0;
my $current_size = 1;
my $i1_file = "";
my $m1_file = "";
my $r7_file = "";
my $temp_m1_file = "";
my $hold_m1_file = "";
my $processed_path = "";
my $processed_path_1 = "";
my $i1_path = "";
my $m1_path = "";
my $r7_path = "";
my $pfi_delete_after = "";
my $sale_start1 = "";
my $sale_end1 = "";
my $sale_start2 = "";
my $sale_end2 = ""; 
my $sale_start3 = "";
my $sale_end3 = ""; 
my $sale_start4 = "";
my $sale_end4 = ""; 
my $long_now = "";
my $Sale_Price = "";
my $Sale_Start_Date = "";
my $Sale_End_Date = "";

my $target_date_start = "";
my $target_date_end = "";
my $target_delay = "";

my $none_100 = "";
my $usage_convert = "";
my $IPF100 = "";
my $term_multi = "";
my $date_format = "";

my $delete_after = "";
my $data_delete = "";
my $file_modify = "";
my $insert_str = "";

my $year_part1 = "";
my $month_part1 = "";
my $day_part1 = "";
my $year_part2 = "";
my $month_part2 = "";
my $day_part2 = "";
my $year_part3 = "";
my $month_part3 = "";
my $day_part3 = "";

my $inst_hold = "";
my $usage_hold = "";

my $ipf1x5_threshold = '';

my $license_status = "";
my $type = "5";

my $file_root = "";
my $secondary_file = "";
my $file_exists_counter = 0;
my $ins_file = "";

my $csn = "";
my $seq_no = "";
my $dept = "";
my $course = "";
my $section = "";
my $loc_code = "";
my $bookxofy = "";
my $course_id = "";
my $instructor = "";
my $course_code = "";
my $delete_flag = "";
my $v_loc_code = "";
my $ebook_adopted = "";
my $class_cap = "";
my $prof_requested = "";
my $estimated_sales = "";
my $ebook1_vendor = "";
my $ebook1_period_1 = "";
my $ebook1_price_1 = "";
my $ebook1_period_2 = "";
my $ebook1_price_2 = "";
my $ebook1_period_3 = "";
my $ebook1_price_3 = "";
my $ebook1_period_4 = "";
my $ebook1_price_4 = "";
my $ebook1_period_5 = "";
my $ebook1_price_5 = "";
my $ebook2_vendor = "";
my $ebook2_period_1 = "";
my $ebook2_price_1 = "";
my $ebook2_period_2 = "";
my $ebook2_price_2 = "";
my $ebook2_period_3 = "";
my $ebook2_price_3 = "";
my $ebook2_period_4 = "";
my $ebook2_price_4 = "";
my $ebook2_period_5 = "";
my $ebook2_price_5 = "";

my $v_bookxofy = "";
my $v_courseid = "";
my $v_course_list = "";
my $s_line = "";
my $v_ebook_adopted = "";
my $v_class_cap = "";
my $v_prof_requested = "";
my $v_estimated_sales = "";
my $v_ebook1_vendor = "";
my $v_ebook1_period_1 = "";
my $v_ebook1_price_1 = "";
my $v_ebook1_period_2 = "";
my $v_ebook1_price_2 = "";
my $v_ebook1_period_3 = "";
my $v_ebook1_price_3 = "";
my $v_ebook1_period_4 = "";
my $v_ebook1_price_4 = "";
my $v_ebook1_period_5 = "";
my $v_ebook1_price_5 = "";
my $v_ebook2_vendor = "";
my $v_ebook2_period_1 = "";
my $v_ebook2_price_1 = "";
my $v_ebook2_period_2 = "";
my $v_ebook2_price_2 = "";
my $v_ebook2_period_3 = "";
my $v_ebook2_price_3 = "";
my $v_ebook2_period_4 = "";
my $v_ebook2_price_4 = "";
my $v_ebook2_period_5 = "";
my $v_ebook2_price_5 = "";

my $first_file = "";
my $second_file = "";

my $New_On_Hand = "";
my $Used_On_Hand = "";
my $New_Addl = "";
my $Used_Addl = "";
my $New_Pend_Ret = "";
my $Used_Pend_Ret = "";
my $New_insite_Pend_Ord = "";
my $Used_insite_Pend_Ord = "";
my $New_Rental_insite_Pend_Ord = "";
my $Used_Rental_insite_Pend_Ord = "";

my $price_count = "";
my $price_loc = "";

my $section_commas = "";
my $v_section_hold = "";

my $ebook_count = 0;

my $pricer_version = "";

my $used_itemid = "";

#Write Startup to Event Log
MM_CodeBase::EventLogWriter::eventlog_write("$exe_file started",'EVENTLOG_ERROR_TYPE','Program Start');

$config_file = $ARGV[0];

if ($config_file eq '')
{
	$config_file = 'c:\pricer\import\configuration\mbs_import.mms';
}
		
# create object
$xml = new XML::Simple;

# read XML file
$data = $xml->XMLin($config_file);

$Logging = lc($data->{Logging});
$Interactive = lc($data->{Interactive});
$verbose = lc($data->{Verbose});
$Log_Path = $data->{Path};
$delete_after = $data->{Delete_After};
$LocalConn_Server = $data->{LocalConn}->{Server};
$LocalConn_DB = $data->{LocalConn}->{DB};
$LocalConn_Pass = decrypt($data->{LocalConn}->{Pass});
$LocalConn_Port = decrypt($data->{LocalConn}->{Port});
$LocalConn_Uid = decrypt($data->{LocalConn}->{Uid});
$data_path = $data->{Data}->{Path};
$data_delete = lc($data->{Data}->{Delete});
$input_delete_after = lc($data->{Data}->{Input_Delete_After});
$sort_order = lc($data->{Sort}->{Order});
$i1_path = $data->{PFI}->{i1_path};
$m1_path = $data->{PFI}->{m1_path};
$r7_path = $data->{PFI}->{r7_path};
$pfi_delete_after = $data->{PFI}->{PFI_Delete_After};
$none_100 = $data->{Price}->{None_100};
$usage_convert = $data->{Usage}->{Convert};
$IPF100 = $data->{Zero_Zero}->{IPF100}; 
$term_multi = lc($data->{Term}->{Multi});
$date_format = $data->{Date}->{Format};
$ipf1x5_threshold = $data->{IPF1x5}->{Threshold};
$New_On_Hand = lc($data->{QOH_Calc}->{Used_On_Hand});
$Used_On_Hand = lc($data->{QOH_Calc}->{New_On_Hand});
$New_Addl = lc($data->{QOH_Calc}->{New_Addl});
$Used_Addl = lc($data->{QOH_Calc}->{Used_Addl});
$New_Pend_Ret = lc($data->{QOH_Calc}->{New_Pend_Ret});
$Used_Pend_Ret = lc($data->{QOH_Calc}->{Used_Pend_Ret});
$New_insite_Pend_Ord = lc($data->{QOH_Calc}->{New_insite_Pend_Ord});
$Used_insite_Pend_Ord = lc($data->{QOH_Calc}->{Used_insite_Pend_Ord});
$New_Rental_insite_Pend_Ord = lc($data->{QOH_Calc}->{New_Rental_insite_Pend_Ord});
$Used_Rental_insite_Pend_Ord = lc($data->{QOH_Calc}->{Used_Rental_insite_Pend_Ord});
$section_commas = ($data->{Replace_Section_Commas}->{Char});
$display_original = lc($data->{Replace_Section_Commas}->{DisplayOriginal});
$pricer_version = ($data->{Pricer}->{Version});

##############  get today date in correct format ##############
($csec, $cmin, $chour, $cmday, $cmon, $cyear) = MM_CodeBase::GetStdTime::get_time();
$now = "$cmon-$cmday-$cyear";
$short_year = substr($cyear, -2);
$short_now = "$cmon/$cmday/$short_year";
$long_now = "$cyear-$cmon-$cmday $chour:$cmin:00";

############## prepare logging ###################
if ($Logging eq "true")
{
	$log_short = "$cyear-$cmon-$cmday";
	$log_file = '>>'.$Log_Path.$exe_file.'_'.$log_short.'.log';
	$LogFile = $Log_Path.$exe_file.'_'.$log_short.'.log';

	if ($Logging eq "true")
	{
		log_writer("$exe_file Successfully Started\n© Copyright 2013-$cyear MarginMate Corporation.  All Rights Reserved.  Used under license.\nSee http://www.marginmate.com/software/license/eula.php for license agreement\n", $log_file);
	}
}

############## check license ###################
if ($pricer_version ne "R4")
{
	$license_status = license_validator ($dsn, $LocalConn_DB, $LocalConn_Server, $LocalConn_Port, $LocalConn_Uid, $LocalConn_Pass, $type);
	
		# license status = 0 signifies unlicensed installation, first execution
		# license status = 1 signifies license validated
		# license status = 2 signifies license expired
	
	if ($Logging eq "true")
	{
		if ($license_status == 0)
		{
			log_writer("This software is unlicensed and will process only 50 records for testing.\nContact license\@marginmate.com to obtain licensed copy.", $log_file);
		}
		elsif ($license_status == 1)
		{
			log_writer("Software license validated.", $log_file);
		}
		elsif ($license_status == 2)
		{
			log_writer("Software license expired, it will process only 50 records until license renewed.\nContact license\@marginmate.com to update license", $log_file);
		}
		else
		{
			log_writer("License Validation Failed", $log_file);
		}
	}
}
else
{
	$license_status = 1;
}


#################### perform  file cleanup ###################
#log cleanup
if ($delete_after > 0)
{
	log_writer("Start Log directory cleanup.", $log_file);
	directory_cleaner($delete_after, $Log_Path, $Logging, $log_file);
	log_writer("Completed Log directory cleanup.", $log_file);
}


if ($input_delete_after > 0)
{
	#processed folder
	if ($Logging eq "true")
	{
		log_writer("Start Processed directory cleanup.", $log_file);
	}
	directory_cleaner($input_delete_after, $data_path."processed\\", $Logging, $log_file);
	#results folder
	if ($Logging eq "true")
	{
		log_writer("Complete Processed directory cleanup.", $log_file);
	}
}

#pfi file cleanup
if ($pfi_delete_after > 0)
{
	#data folder
	if ($Logging eq "true")
	{
		log_writer("Start PFI Data directory cleanup.", $log_file);
	}
	directory_cleaner($pfi_delete_after, $i1_path, $Logging, $log_file);
	#results folder
	if ($Logging eq "true")
	{
		log_writer("Complete PFI Data directory cleanup.", $log_file);
	}
	
	#result folder
	if ($Logging eq "true")
	{
		log_writer("Start PFI Result directory cleanup.", $log_file);
	}
	directory_cleaner($pfi_delete_after, $r7_path, $Logging, $log_file);
	#results folder
	if ($Logging eq "true")
	{
		log_writer("Complete PFI Result directory cleanup.", $log_file);
	}
}

############## Connect DB ##################

$dsn = "DBI:mysql:database=$LocalConn_DB;host=$LocalConn_Server;port=$LocalConn_Port";
$dbh = DBI->connect($dsn, $LocalConn_Uid, $LocalConn_Pass);

############## lookup files ################
opendir(IMD, $data_path)
	or do
	{ 
		if (($verbose eq "true") && ($Logging eq "true"))
		{
			log_writer("Failed to open $data_path terminating execution\n", $log_file);
			log_writer("\n***********Unexpected Program Termination***********\n", $log_file);
		}
		goto END;
	};

@file_list = ();
@file_list = readdir(IMD);

#find earliest modify date
my $data_file = "";
my $earliest_date = "Z";

foreach $file_name (@file_list)
{
	unless (($file_name eq ".") || ($file_name eq ".."))
	{
		#find first file
		if ((lc(substr($file_name, -3)) eq "tx1") || (lc(substr($file_name, -3)) eq "txt"))
		{
			$file = $data_path.$file_name;
			$file_modify = ctime(stat($file)->mtime);
			if ($file_modify lt $earliest_date)
			{
				$earliest_date = $file_modify;
				$first_file = $file_name;
				
				#find matching second file
				if (lc(substr($first_file, -3)) eq "tx1")
				{
					$second_file = substr($file_name, 0, -4).".txt";
				}
				else
				{
					$second_file = substr($file_name, 0, -4).".tx1";
				}
				$file_exists_counter = 0;
				 #see if it exists, wait up to 60 seconds if not
				while (!-e $data_path.$second_file)
				{
					sleep 1.0;
					$file_exists_counter++;
					
					#terminate if main file missing after 60 seconds
					if ($file_exists_counter > 60)
					{
						log_writer("No input file $second_file found to match $first_file file.  $first_file deleted.  Terminating execution\n", $log_file);
						unlink $file;
						goto END;
					}
				}
			}
		}
		
		if (lc(substr($first_file, -3)) eq "tx1")
		{
			$data_file = $second_file;
			$secondary_file = $first_file;
		}
		else
		{
			$data_file = $first_file;
			$secondary_file = $second_file;
		}
	}
}


if ($data_file eq "")
{
	if (($verbose eq "true") && ($Logging eq "true"))
	{
		log_writer("No input file found at $data_path terminating execution\n", $log_file);
		goto END;
	}
}

closedir(IMD);

##############  process files ##############

$i1_file = $i1_path.substr($data_file, 0, length($data_file) - 4).".i1";
$m1_file = $m1_path.substr($data_file, 0, length($data_file) - 4).".m1";
$r7_file = $r7_path.substr($data_file, 0, length($data_file) - 4).".r7";
$temp_m1_file = $m1_path.substr($data_file, 0, length($data_file) - 4).".hold";
$processed_path = $data_path."processed\\".$data_file;
$processed_path_1 = $data_path."processed\\".$secondary_file;

$in_file = $data_path.$data_file;
$ins_file = $data_path.$secondary_file;
$out_file = '>'.$i1_file;
$temp_file = $data_path.'output.hold';
$hold_m1_file = '>'.$temp_m1_file;

$in_count = 0;
$out_count = 0;

## check for main file size stability
if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("Checking size of $in_file");
}
while ($last_size < $current_size)
{
	$current_size = -s $in_file;
	
	if ($last_size < $current_size)
	{
		$last_size = $current_size;
		$current_size = $current_size + 1;
	}
	
	sleep 0.25;
}

if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("$in_file size stable at $current_size bytes, continuing processing", $log_file);
}

## check for secondary file size stability
$last_size = 0;
$current_size = 1;
if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("Checking size of $ins_file");
}
while ($last_size < $current_size)
{
	$current_size = -s $ins_file;
	
	if ($last_size < $current_size)
	{
		$last_size = $current_size;
		$current_size = $current_size + 1;
	}
	
	sleep 0.25;
}
if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("$ins_file size stable at $current_size bytes, continuing processing", $log_file);
}

#start processing secondary file
#empty temp table
$db_str = "DELETE FROM t_secondary_file;";
$ex_db = $dbh->prepare($db_str);
$ex_db->execute();
$ex_db->finish();

#process new rows
open (INSFILE, $ins_file)
	or do
	{ 
		if (($verbose eq "true") && ($Logging eq "true"))
		{
			log_writer("Failed to open $ins_file terminating execution\n", $log_file);
			log_writer("\n***********Unexpected Program Termination***********\n", $log_file);
		}
		goto END;
	};

while ($s_line = <INSFILE>)
{
	chomp $s_line;
	(
	$GenKey, 			#1
	$csn,	 			#2
	$seq_no,			#3
	$ISBN, 				#4
	$Term, 				#5
	$Term_Description, 	#6
	$dept,				#7
	$course,			#8
	$section,			#9
	$loc_code,			#10
	$bookxofy,			#11
	$course_id,			#12
	$instructor,		#13
	$course_code,		#14
	$delete_flag,		#15
	$ebook_adopted,		#16
	$class_cap,		#17
	$prof_requested,	#18
	$estimated_sales,	#19
	$ebook1_vendor,		#20
	$ebook1_period_1,	#21
	$ebook1_price_1,	#22
	$ebook1_period_2,	#23
	$ebook1_price_2,	#24
	$ebook1_period_3,	#25
	$ebook1_price_3,	#26
	$ebook1_period_4,	#27
	$ebook1_price_4,	#28
	$ebook1_period_5,	#29
	$ebook1_price_5,	#30
	$ebook2_vendor,		#31
	$ebook2_period_1,	#32
	$ebook2_price_1,	#33
	$ebook2_period_2,	#34
	$ebook2_price_2,	#35
	$ebook2_period_3,	#36
	$ebook2_price_3,	#37
	$ebook2_period_4,	#38
	$ebook2_price_4,	#39
	$ebook2_period_5,	#40
	$ebook2_price_5		#41
	
	) = split /\|/, $s_line;
	
	#clean up numerics
	if (($class_cap eq "") || (!defined($class_cap)))
	{
		$class_cap = 0;
	}
	
	if (($estimated_sales eq "") || (!defined($estimated_sales)))
	{
		$estimated_sales = 0;
	}
	
	if (($ebook1_price_1 eq "") || (!defined($ebook1_price_1)))
	{
		$ebook1_price_1 = 0;
	}
	if (($ebook1_price_2 eq "") || (!defined($ebook1_price_2)))
	{
		$ebook1_price_2 = 0;
	}
	if (($ebook1_price_3 eq "") || (!defined($ebook1_price_3)))
	{
		$ebook1_price_3 = 0;
	}
	if (($ebook1_price_4 eq "") || (!defined($ebook1_price_4)))
	{
		$ebook1_price_4 = 0;
	}
	if (($ebook1_price_5 eq "") || (!defined($ebook1_price_5)))
	{
		$ebook1_price_5 = 0;
	}
	if (($ebook2_price_1 eq "") || (!defined($ebook2_price_1)))
	{
		$ebook2_price_1 = 0;
	}
	if (($ebook2_price_2 eq "") || (!defined($ebook2_price_2)))
	{
		$ebook2_price_2 = 0;
	}
	if (($ebook2_price_3 eq "") || (!defined($ebook2_price_3)))
	{
		$ebook2_price_3 = 0;
	}
	if (($ebook2_price_4 eq "") || (!defined($ebook2_price_4)))
	{
		$ebook2_price_4 = 0;
	}
	if (($ebook2_price_5 eq "") || (!defined($ebook2_price_5)))
	{
		$ebook2_price_5 = 0;
	}
	
	
	if (($ebook1_period_1 eq "") || (!defined($ebook1_period_1)))
	{
		$ebook1_period_1 = 0;
	}
	if (($ebook1_period_2 eq "") || (!defined($ebook1_period_2)))
	{
		$ebook1_period_2 = 0;
	}
	if (($ebook1_period_3 eq "") || (!defined($ebook1_period_3)))
	{
		$ebook1_period_3 = 0;
	}
	if (($ebook1_period_4 eq "") || (!defined($ebook1_period_4)))
	{
		$ebook1_period_4 = 0;
	}
	if (($ebook1_period_5 eq "") || (!defined($ebook1_period_5)))
	{
		$ebook1_period_5 = 0;
	}
	if (($ebook2_period_1 eq "") || (!defined($ebook2_period_1)))
	{
		$ebook2_period_1 = 0;
	}
	if (($ebook2_period_2 eq "") || (!defined($ebook2_period_2)))
	{
		$ebook2_period_2 = 0;
	}
	if (($ebook2_period_3 eq "") || (!defined($ebook2_period_3)))
	{
		$ebook2_period_3 = 0;
	}
	if (($ebook2_period_4 eq "") || (!defined($ebook2_period_4)))
	{
		$ebook2_period_4 = 0;
	}
	if (($ebook2_period_5 eq "") || (!defined($ebook2_period_5)))
	{
		$ebook2_period_5 = 0;
	}
	
	#format prices
	$ebook1_price_1 = sprintf '%.2f', $ebook1_price_1;
	$ebook1_price_2 = sprintf '%.2f', $ebook1_price_2;
	$ebook1_price_3 = sprintf '%.2f', $ebook1_price_3;
	$ebook1_price_4 = sprintf '%.2f', $ebook1_price_4;
	$ebook1_price_5 = sprintf '%.2f', $ebook1_price_5;
	$ebook2_price_1 = sprintf '%.2f', $ebook2_price_1;
	$ebook2_price_2 = sprintf '%.2f', $ebook2_price_2;
	$ebook2_price_3 = sprintf '%.2f', $ebook2_price_3;
	$ebook2_price_4 = sprintf '%.2f', $ebook2_price_4;
	$ebook2_price_5 = sprintf '%.2f', $ebook2_price_5;
	
	#set up original section display
	$original_section = $section;
	
	#clean up commas, spaces and single quotes
	$section =~ s/\,/$section_commas/g;
	$section =~ s/ /$section_commas/g;
	$instructor =~ s/\'/ /g;
	
	$db_str = "INSERT INTO t_secondary_file 
			(ISBN, Term, Term_Description, dept, course, section, original_section, loc_code, bookxofy, course_id, instructor, course_code, delete_flag, ebook_adopted, class_cap, prof_requested, estimated_sales, ebook1_vendor, ebook1_period_1, ebook1_price_1, ebook1_period_2, ebook1_price_2, ebook1_period_3, ebook1_price_3, ebook1_period_4, ebook1_price_4, ebook1_period_5, ebook1_price_5, ebook2_vendor, ebook2_period_1, ebook2_price_1, ebook2_period_2, ebook2_price_2, ebook2_period_3, ebook2_price_3, ebook2_period_4, ebook2_price_4, ebook2_period_5, ebook2_price_5) 
			VALUES 
			(\'$ISBN\', \'$Term\', \'$Term_Description\', \'$dept\', \'$course\', \'$section\', \'$original_section\', \'$loc_code\', \'$bookxofy\', \'$course_id\', \'$instructor\', \'$course_code\', \'$delete_flag\', \'$ebook_adopted\', \'$class_cap\', \'$prof_requested\', \'$estimated_sales\', \'$ebook1_vendor\', \'$ebook1_period_1\', \'$ebook1_price_1\', \'$ebook1_period_2\', \'$ebook1_price_2\', \'$ebook1_period_3\', \'$ebook1_price_3\', \'$ebook1_period_4\', \'$ebook1_price_4\', \'$ebook1_period_5\', \'$ebook1_price_5\', \'$ebook2_vendor\', \'$ebook2_period_1\', \'$ebook2_price_1\', \'$ebook2_period_2\', \'$ebook2_price_2\', \'$ebook2_period_3\', \'$ebook2_price_3\', \'$ebook2_period_4\', \'$ebook2_price_4\', \'$ebook2_period_5\', \'$ebook2_price_5\');";
	
	#print "$db_str/n";
	
	$ex_db = $dbh->prepare($db_str);
	$ex_db->execute();
	$ex_db->finish();	
}

close (INSFILE);

#start processsing main file
open (INFILE, $in_file)
or do
	{ 
		if (($verbose eq "true") && ($Logging eq "true"))
		{
			log_writer("Failed to open $in_file terminating execution\n", $log_file);
			log_writer("\n***********Unexpected Program Termination***********\n", $log_file);
		}
		goto END;
	};

if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("$in_file opened for record processing", $log_file);
}

open (OUTFILE, $out_file)
or do
	{ 
		if (($verbose eq "true") && ($Logging eq "true"))
		{
			log_writer("Failed to open $i1_file terminating execution\n", $log_file);
			log_writer("\n***********Unexpected Program Termination***********\n", $log_file);
		}
		goto END;
	};
	
if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("$i1_file opened for processed record write", $log_file);
}

$in_count = 0;

while ($line = <INFILE>)
{
	if ((($license_status != 1) && ($in_count < 50)) || ($license_status == 1))
	{
		$in_count++;
		
		chomp $line;
		
		#check for zero length line, only process if > 0
		if (length($line) > 0)
		{
		
			#split into fields
			(
			$GenKey, 							#1
			$FormatFlag, 						#2
			$Author, 							#3
			$Title, 							#4
			$ISBN, 								#5
			$Used_ISBN,							#6
			$ISBN_HR, 							#7
			$Vendor_Style, 						#8
			$Publisher, 						#9
			$Imprint, 							#10
			$Edition, 							#11
			$Edition_Status, 					#12
			$New_Price, 						#13
			$New_Price_Text, 					#14
			$Used_Price, 						#15
			$Used_Price_Text, 					#16
			$New_Rental_Price, 					#17
			$New_Rental_Price_Text, 			#18
			$Ebook_Price, 						#19
			$Ebook_Price_Text, 					#20
			$Used_Rental_Price, 				#21
			$Used_Rental_Price_Text, 			#22
			$Sale_Price1, 						#23
			$Sale_Start_Date1, 					#24
			$Sale_End_Date1, 					#25
			$Sale_Start_Time1, 					#26
			$Sale_End_Time1, 					#27
			$Sale_Price2, 						#28
			$Sale_Start_Date2, 					#29
			$Sale_End_Date2, 					#30
			$Sale_Start_Time2, 					#31
			$Sale_End_Time2, 					#32
			$Sale_Price3, 						#33
			$Sale_Start_Date3, 					#34
			$Sale_End_Date3, 					#35
			$Sale_Start_Time3, 					#36
			$Sale_End_Time3, 					#37
			$Sale_Price4, 						#38
			$Sale_Start_Date4, 					#39
			$Sale_End_Date4, 					#40
			$Sale_Start_Time4, 					#41
			$Sale_End_Time4, 					#42
			$Term, 								#43
			$Term_Description, 					#44
			$Requested_Qty, 					#45
			$Class_Capacity_Qty, 				#46
			$Actual_Enrollment_Qty,				#47
			$Est_Sales_Qty, 					#48
			$Category, 							#49
			$Division, 							#50
			$Department, 						#51
			$Class, 							#52
			$New_Store_Qty, 					#53
			$New_Warehouse_Qty, 				#54
			$Used_Store_Qty, 					#55
			$Used_Warehouse_Qty, 				#56
			$New_Pending_Return_Qty,			#57
			$Used_Pending_Return_Qty,			#58
			$New_insite_Pending_Order,			#59
			$Used_insite_Pending_Order,			#60
			$New_Rental_insite_Pending_Order,	#61
			$Used_Rental_insite_Pending_Order,	#62
			$On_Order_PO1, 						#63
			$On_Order_PO1_Vendor,				#64
			$On_Order_Qty1, 					#65		
			$On_Order_Qty1_Used,				#66
			$On_Order_Date1, 					#67		
			$On_Order_PO2, 						#68
			$On_Order_PO2_Vendor,				#69
			$On_Order_Qty2, 					#70
			$On_Order_Qty2_Used, 				#71
			$On_Order_Date2, 					#72		
			$On_Order_PO3, 						#73
			$On_Order_PO3_Vendor,				#74
			$On_Order_Qty3, 					#75
			$On_Order_Qty3_Used, 				#76
			$On_Order_Date3, 					#77
			$Total_PO_Qty 						#78
			) = split /\|/, $line;
			
			$image_name = "$ISBN.png";

			#Format PO Dates
			if ($date_format ne "YYYYMMDD")
			{
				$year_part1 = "";
				$month_part1 = "";
				$day_part1 = "";
				$year_part2 = "";
				$month_part2 = "";
				$day_part2 = "";
				$year_part3 = "";
				$month_part3 = "";
				$day_part3 = "";
				
				if (length($On_Order_Date1) == 8)
				{
					$year_part1 = substr($On_Order_Date1, 0, 4);
					$month_part1 = substr($On_Order_Date1, 4, 2);
					$day_part1 = substr($On_Order_Date1, 6, 2);
				}
				
				if (length($On_Order_Date2) == 8)
				{
					$year_part2 = substr($On_Order_Date2, 0, 4);
					$month_part2 = substr($On_Order_Date2, 4, 2);
					$day_part2 = substr($On_Order_Date2, 6, 2);
				}
				
				if (length($On_Order_Date3) == 8)
				{
					$year_part3 = substr($On_Order_Date3, 0, 4);
					$month_part3 = substr($On_Order_Date3, 4, 2);
					$day_part3 = substr($On_Order_Date3, 6, 2);
				}
					
				if ($date_format eq "MM/DD/YYYY")
				{
					$On_Order_Date1 = "$month_part1/$day_part1/$year_part1";
					$On_Order_Date2 = "$month_part2/$day_part2/$year_part2";
					$On_Order_Date3 = "$month_part3/$day_part3/$year_part3";
				}
				
				if ($date_format eq "DD/MM/YYYY")
				{
					$On_Order_Date1 = "$day_part1/$month_part1/$year_part1";
					$On_Order_Date2 = "$day_part2/$month_part2/$year_part2";
					$On_Order_Date3 = "$day_part3/$month_part3/$year_part3";
				}
				
				if ($date_format eq "MM-DD-YYYY")
				{
					$On_Order_Date1 = "$month_part1-$day_part1-$year_part1";
					$On_Order_Date2 = "$month_part2-$day_part2-$year_part2";
					$On_Order_Date3 = "$month_part3-$day_part3-$year_part3";
				}
				
				if ($date_format eq "DD-MM-YYYY")
				{
					$On_Order_Date1 = "$day_part1-$month_part1-$year_part1";
					$On_Order_Date2 = "$day_part2-$month_part2-$year_part2";
					$On_Order_Date3 = "$day_part3-$month_part3-$year_part3";
				}
				
				if ($date_format eq "YYYY-MM-DD")
				{
					$On_Order_Date1 = "$year_part1-$month_part1-$day_part1";
					$On_Order_Date2 = "$year_part2-$month_part2-$day_part2";
					$On_Order_Date3 = "$year_part3-$month_part3-$day_part3";
				}
				
				if ($date_format eq "YYYY/MM/DD")
				{
					$On_Order_Date1 = "$year_part1/$month_part1/$day_part1";
					$On_Order_Date2 = "$year_part2/$month_part2/$day_part2";
					$On_Order_Date3 = "$year_part3/$month_part3/$day_part3";
				}
				
				if ($date_format eq "MM/DD/YY")
				{
					$year_part1 = substr($year_part1, 2, 2);
					$On_Order_Date1 = "$month_part1/$day_part1/$year_part1";
					$year_part2 = substr($year_part2, 2, 2);
					$On_Order_Date2 = "$month_part2/$day_part2/$year_part2";
					$year_part3 = substr($year_part3, 2, 2);
					$On_Order_Date3 = "$month_part3/$day_part3/$year_part3";
				}
				
				if ($date_format eq "DD/MM/YY")
				{
					$year_part1 = substr($year_part1, 2, 2);
					$On_Order_Date1 = "$day_part1/$month_part1/$year_part1";
					$year_part2 = substr($year_part2, 2, 2);
					$On_Order_Date2 = "$day_part2/$month_part2/$year_part2";
					$year_part3 = substr($year_part3, 2, 2);
					$On_Order_Date3 = "$day_part3/$month_part3/$year_part3";
				}
				
				if ($date_format eq "MM-DD-YY")
				{
					$year_part1 = substr($year_part1, 2, 2);
					$On_Order_Date1 = "$month_part1-$day_part1-$year_part1";
					$year_part2 = substr($year_part2, 2, 2);
					$On_Order_Date2 = "$month_part2-$day_part2-$year_part2";
					$year_part3 = substr($year_part3, 2, 2);
					$On_Order_Date3 = "$month_part3-$day_part3-$year_part3";
				}
				
				if ($date_format eq "DD-MM-YY")
				{
					$year_part1 = substr($year_part1, 2, 2);
					$On_Order_Date1 = "$day_part1-$month_part1-$year_part1";
					$year_part2 = substr($year_part2, 2, 2);
					$On_Order_Date2 = "$day_part2-$month_part2-$year_part2";
					$year_part3 = substr($year_part3, 2, 2);
					$On_Order_Date3 = "$day_part3-$month_part3-$year_part3";
				}
			}
					
			$ISBN = trim($ISBN);
					
			#process if valid ISBN/SKU
			if ((defined($ISBN)) && ($ISBN ne ""))
			{
				#clean up prices and quantities
				if (($New_Price eq "") || (!defined($New_Price)))
				{
					$New_Price = 0;
				}
				if (($Used_Price eq "") || (!defined($Used_Price)))
				{
					$Used_Price = 0;
				}
				if (($New_Rental_Price eq "") || (!defined($New_Rental_Price)))
				{
					$New_Rental_Price = 0;
				}
				if (($Ebook_Price eq "") || (!defined($Ebook_Price)))
				{
					$Ebook_Price = 0;
				}
				if (($Used_Rental_Price eq "") || (!defined($Used_Rental_Price)))
				{
					$Used_Rental_Price = 0;
				}	
				if (($Requested_Qty eq "") || (!defined($Requested_Qty)))
				{
					$Requested_Qty = 0;
				}
				if (($New_Store_Qty eq "") || (!defined($New_Store_Qty)))
				{
					$New_Store_Qty = 0;
				}
				if (($New_Warehouse_Qty eq "") || (!defined($New_Warehouse_Qty)))
				{
					$New_Warehouse_Qty = 0;
				}
				if (($Used_Store_Qty eq "") || (!defined($Used_Store_Qty)))
				{
					$Used_Store_Qty = 0;
				}
				if (($Used_Warehouse_Qty eq "") || (!defined($Used_Warehouse_Qty)))
				{
					$Used_Warehouse_Qty = 0;
				}
				if (($On_Order_Qty1 eq "") || (!defined($On_Order_Qty1)))
				{
					$On_Order_Qty1 = 0;
				}
				if (($On_Order_Qty2 eq "") || (!defined($On_Order_Qty2)))
				{
					$On_Order_Qty2 = 0;
				}
				if (($On_Order_Qty3 eq "") || (!defined($On_Order_Qty3)))
				{
					$On_Order_Qty3 = 0;
				}
				if (($Total_PO_Qty eq "") || (!defined($Total_PO_Qty)))
				{
					$Total_PO_Qty = 0;
				}
				if (($Class_Capacity_Qty eq "") || (!defined($Class_Capacity_Qty)))
				{
					$Class_Capacity_Qty = 0;
				}
				if (($Actual_Enrollment_Qty eq "") || (!defined($Actual_Enrollment_Qty)))
				{
					$Actual_Enrollment_Qty = 0;
				}
				if (($Est_Sales_Qty eq "") || (!defined($Est_Sales_Qty)))
				{
					$Est_Sales_Qty = 0;
				}
				if (($Sale_Price eq "") || (!defined($Sale_Price)))
				{
					$Sale_Price = 0;
				}
				
				if (($New_Pending_Return_Qty eq "") || (!defined($New_Pending_Return_Qty)))
				{
					$New_Pending_Return_Qty = 0;
				}
				if (($Used_Pending_Return_Qty eq "") || (!defined($Used_Pending_Return_Qty)))
				{
					$Used_Pending_Return_Qty = 0;
				}
				if (($New_insite_Pending_Order eq "") || (!defined($New_insite_Pending_Order)))
				{
					$New_insite_Pending_Order = 0;
				}
				if (($Used_insite_Pending_Order eq "") || (!defined($Used_insite_Pending_Order)))
				{
					$Used_insite_Pending_Order = 0;
				}
				if (($On_Order_Qty1_Used eq "") || (!defined($On_Order_Qty1_Used)))
				{
					$On_Order_Qty1_Used = 0;
				}
				if (($On_Order_Qty2_Used eq "") || (!defined($On_Order_Qty2_Used)))
				{
					$On_Order_Qty2_Used = 0;
				}
				if (($On_Order_Qty3_Used eq "") || (!defined($On_Order_Qty3_Used)))
				{
					$On_Order_Qty3_Used = 0;
				}
		
				###### Start Book Record #####
				if (lc($Category) eq "t")
				{
					#determine base item IPF
					$ITEMIPF = 0;
					%out_text = ();
					%out_price = ();
					
					if ($New_Price > 0)
					{
						$ITEMIPF++;
						$out_text{$ITEMIPF} = $New_Price_Text;
						$out_price{$ITEMIPF} = $New_Price;
					}
					
					if ($Used_Price > 0)
					{
						$ITEMIPF++;
						$out_text{$ITEMIPF} = $Used_Price_Text;
						$out_price{$ITEMIPF} = $Used_Price;
					}
					
					if ($New_Rental_Price > 0)
					{
						$ITEMIPF++;
						$out_text{$ITEMIPF} = $New_Rental_Price_Text;
						$out_price{$ITEMIPF} = $New_Rental_Price;
					}
					
					if ($Used_Rental_Price > 0)
					{
						$ITEMIPF++;
						$out_text{$ITEMIPF} = $Used_Rental_Price_Text;
						$out_price{$ITEMIPF} = $Used_Rental_Price;
					}
					
					if ($Ebook_Price > 0)
					{
						$ITEMIPF++;
						$out_text{$ITEMIPF} = $Ebook_Price_Text;
						$out_price{$ITEMIPF} = $Ebook_Price;
					}
				
					$ITEMIPF = $ITEMIPF * 100;
					
					#Ascending order output
					if (lc($sort_order) eq "ascending")
					{
						@sort = ();
						$sort_counter = 0;
						%sorted_price = ();
						%sorted_text = ();
						
						@sort = sort{$out_price{$a} <=> $out_price{$b}} keys(%out_price);
			
						foreach(@sort)
						{
							$sort_counter++;
							$sorted_price{$sort_counter} = @out_price{$_};
							$sorted_text{$sort_counter} = @out_text{$_};	
						}
						
						$out_price_1 = $sorted_price{1};
						$out_text_1 = $sorted_text{1};
						$out_price_2 = $sorted_price{2};
						$out_text_2 = $sorted_text{2};
						$out_price_3 = $sorted_price{3};
						$out_text_3 = $sorted_text{3};
						$out_price_4 = $sorted_price{4};
						$out_text_4 = $sorted_text{4};
						$out_price_5 = $sorted_price{5};
						$out_text_5 = $sorted_text{5};
					}
					
					#Natural order output
					if (lc($sort_order) eq "natural")
					{
						$out_price_1 = $out_price{1};
						$out_text_1 = $out_text{1};
						$out_price_2 = $out_price{2};
						$out_text_2 = $out_text{2};
						$out_price_3 = $out_price{3};
						$out_text_3 = $out_text{3};
						$out_price_4 = $out_price{4};
						$out_text_4 = $out_text{4};
						$out_price_5 = $out_price{5};
						$out_text_5 = $out_text{5};
					}
							
					#Format Currency	
					$New_Price = sprintf '%.2f', $New_Price;
					$Used_Price = sprintf '%.2f', $Used_Price;
					$New_Rental_Price = sprintf '%.2f', $New_Rental_Price;
					$Ebook_Price = sprintf '%.2f', $Ebook_Price;
					$Used_Rental_Price = sprintf '%.2f', $Used_Rental_Price;
					$out_price_1 = sprintf '%.2f', $out_price_1;
					$out_price_2 = sprintf '%.2f', $out_price_2;
					$out_price_3 = sprintf '%.2f', $out_price_3;
					$out_price_4 = sprintf '%.2f', $out_price_4;
					$out_price_5 = sprintf '%.2f', $out_price_5;
					$Sale_Price = sprintf '%.2f', $Sale_Price;
					
					#Format Quantities
					$New_Store_Qty = sprintf '%d', $New_Store_Qty;
					$New_Warehouse_Qty = sprintf '%d', $New_Warehouse_Qty;
					$Used_Store_Qty = sprintf '%d', $Used_Store_Qty;
					$Used_Warehouse_Qty = sprintf '%d', $Used_Warehouse_Qty;
					$On_Order_Qty1 = sprintf '%d', $On_Order_Qty1;
					$On_Order_Qty2 = sprintf '%d', $On_Order_Qty2;
					$On_Order_Qty3 = sprintf '%d', $On_Order_Qty3;
					$Total_PO_Qty = sprintf '%d', $Total_PO_Qty;
					$Requested_Qty = sprintf '%d', $Requested_Qty;
					$Class_Capacity_Qty = sprintf '%d', $Class_Capacity_Qty;
					$Actual_Enrollment_Qty = sprintf '%d', $Actual_Enrollment_Qty;
					$Est_Sales_Qty = sprintf '%d', $Est_Sales_Qty;
					$On_Order_Qty1_Used = sprintf '%d', $Est_Sales_Qty;
					$On_Order_Qty2_Used = sprintf '%d', $On_Order_Qty2_Used;
					$On_Order_Qty3_Used = sprintf '%d', $On_Order_Qty3_Used;
					$New_Pending_Return_Qty = sprintf '%d', $New_Pending_Return_Qty;
					$Used_Pending_Return_Qty = sprintf '%d', $Used_Pending_Return_Qty;
					$New_insite_Pending_Order = sprintf '%d', $New_insite_Pending_Order;
					$Used_insite_Pending_Order = sprintf '%d', $Used_insite_Pending_Order;
					$New_Rental_insite_Pending_Order = sprintf '%d', $New_Rental_insite_Pending_Order;
					$Used_Rental_insite_Pending_Order = sprintf '%d', $Used_Rental_insite_Pending_Order;
					
					#calculate $Total_OH
					$Total_OH = 0;
					
					if ($New_On_Hand eq "add")
					{
						$Total_OH = $Total_OH  + $New_Store_Qty;
					}
					elsif ($New_On_Hand eq "subtract")
					{
						$Total_OH = $Total_OH  - $New_Store_Qty;
					}
					
					if ($Used_On_Hand eq "add")
					{
						$Total_OH = $Total_OH  + $Used_Store_Qty;
					}
					elsif ($Used_On_Hand eq "subtract")
					{
						$Total_OH = $Total_OH  - $Used_Store_Qty;
					}
										
					if ($New_Addl eq "add")
					{
						$Total_OH = $Total_OH  + $New_Warehouse_Qty;
					}
					elsif ($New_Addl eq "subtract")
					{
						$Total_OH = $Total_OH  - $New_Warehouse_Qty;
					}
					
					if ($Used_Addl eq "add")
					{
						$Total_OH = $Total_OH  + $Used_Warehouse_Qty;
					}
					elsif ($Used_Addl eq "subtract")
					{
						$Total_OH = $Total_OH  - $Used_Warehouse_Qty;
					}
										
					if ($New_Pend_Ret eq "add")
					{
						$Total_OH = $Total_OH  + $New_Pending_Return_Qty;
					}
					elsif ($New_Pend_Ret eq "subtract")
					{
						$Total_OH = $Total_OH  - $New_Pending_Return_Qty;
					}
					
					if ($Used_Pend_Ret eq "add")
					{
						$Total_OH = $Total_OH  + $Used_Pending_Return_Qty;
					}
					elsif ($Used_Pend_Ret eq "subtract")
					{
						$Total_OH = $Total_OH  - $Used_Pending_Return_Qty;
					}
										
					if ($New_insite_Pend_Ord eq "add")
					{
						$Total_OH = $Total_OH  + $New_insite_Pending_Order;
					}
					elsif ($New_insite_Pend_Ord eq "subtract")
					{
						$Total_OH = $Total_OH  - $New_insite_Pending_Order;
					}
					
					if ($Used_insite_Pend_Ord eq "add")
					{
						$Total_OH = $Total_OH  + $Used_insite_Pending_Order;
					}
					elsif ($Used_insite_Pend_Ord eq "subtract")
					{
						$Total_OH = $Total_OH  - $Used_insite_Pending_Order;
					}
					
					if ($New_Rental_insite_Pend_Ord eq "add")
					{
						$Total_OH = $Total_OH  + $New_Rental_insite_Pending_Order;
					}
					elsif ($New_Rental_insite_Pend_Ord eq "subtract")
					{
						$Total_OH = $Total_OH  - $New_Rental_insite_Pending_Order;
					}
					
					if ($Used_Rental_insite_Pend_Ord eq "add")
					{
						$Total_OH = $Total_OH  + $Used_Rental_insite_Pending_Order;
					}
					elsif ($Used_Rental_insite_Pend_Ord eq "subtract")
					{
						$Total_OH = $Total_OH  - $Used_Rental_insite_Pending_Order;
					}

					#Trim author, title to correct length
					$Author = substr($Author, 0, 255);
					$Title = substr($Title, 0, 255);
					
					#handler for missing all prices
					if (($ITEMIPF == 0) && (lc($none_100) eq "true"))
					{
						$ITEMIPF = 100;	
						if ((lc($verbose) eq "true") && (lc($Logging) eq "true") && (lc($ISBN) ne 'ntr'))
						{
							log_writer("record $in_count, $ISBN, in $in_file did not generate valid IPF, default IPF100 assigned", $log_file);
						}	
					}
					elsif (($ITEMIPF == 0) && (lc($none_100) eq "false") && (lc($ISBN) ne 'ntr'))
					{
						$ITEMIPF = 104;	
						if ((lc($verbose) eq "true") && (lc($Logging) eq "true"))
						{
							log_writer("record $in_count, $ISBN, in $in_file did not generate valid IPF, default IPF104 assigned", $log_file);
						}	
					}
					
					#check if ipf1x5 threshold exceeded, if so assign IPF1X5
					if (($ipf1x5_threshold > 0) && (($out_price_1 > $ipf1x5_threshold) || ($out_price_2 > $ipf1x5_threshold) || ($out_price_3 > $ipf1x5_threshold) || ($out_price_4 > $ipf1x5_threshold) || ($out_price_5 > $ipf1x5_threshold)))
					{
						$ITEMIPF = 105;
					}
							
					#ipf for none on hand but some on order
					if (($Total_OH <= 0) && ($Total_PO_Qty > 0) && ($ITEMIPF ne "104"))
					{
						$ITEMIPF = 101;
					}
					
					#ipf for none on hand and none on order
					if (($Total_OH <= 0) && ($Total_PO_Qty <= 0) && ($ITEMIPF ne "104"))
					{
						$ITEMIPF = 102;
					}
			
					#ipf for none on hand and none on order, but prefix 281 selected via IPF100 parameter
					if (($Total_OH <= 0) && ($Total_PO_Qty <= 0) && (lc($IPF100) eq "true") && (substr($ISBN,0,3) eq "281"))
					{
						$ITEMIPF = 100;
					}
					
					#ipf for ISBN = "NTR"
					if (lc($ISBN) eq 'ntr')
					{
						$ITEMIPF = 106;
					}
					
					#go to IPF x5x series for store format "D" (organized by Dept/Course/Section) vs "A", which is alpha by author/title
					if (lc($FormatFlag) eq "d")
					{
						$ITEMIPF = $ITEMIPF + 50;
					}
					
					#check for digital edition, set base IPF at 700
					if (lc($ISBN_HR) eq 'dgt')
					{
						$ITEMIPF = 700;

					}
					
					#Always create base ISBN record

					$itemid = $ISBN;
					
					$v_course_list = "";
					
					#get secondary record
					$select_str = "SELECT ISBN, Term, Term_Description, dept, course, section, original_section, loc_code, bookxofy, course_id, instructor, course_code, delete_flag, ebook_adopted, class_cap, prof_requested, estimated_sales, ebook1_vendor, ebook1_period_1, ebook1_price_1, ebook1_period_2, ebook1_price_2, ebook1_period_3, ebook1_price_3, ebook1_period_4, ebook1_price_4, ebook1_period_5, ebook1_price_5, ebook2_vendor, ebook2_period_1, ebook2_price_1, ebook2_period_2, ebook2_price_2, ebook2_period_3, ebook2_price_3, ebook2_period_4, ebook2_price_4, ebook2_period_5, ebook2_price_5 FROM t_secondary_file 
						WHERE ISBN = \'$ISBN\' AND Term = \'$Term\' 
						ORDER BY dept, course, section;";
					$selectdb = $dbh->prepare($select_str);
					$selectdb->execute() or die "Unable to execute '$select_str'.  " . $selectdb->errstr;
					#$row = $selectdb->fetchrow_hashref();
					
					if ($display_original ne "true")
					{
						while($row = $selectdb->fetchrow_hashref())
						{
							$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'section'}.", ".$row->{'course_code'};
						}
					}
					else
					{
						while($row = $selectdb->fetchrow_hashref())
						{
							$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'original_section'}.", ".$row->{'course_code'};
						}
					}					
					
					$selectdb->finish();
					
					$v_course_list = substr($v_course_list, 2, );
					
					$v_course_list = $v_course_list." <";
					
					#trim to 1024 max length
					$v_course_list = substr($v_course_list, 0, 1024 );

					
					#Blank course-specific field values prior to write
					$v_dept = "";
					$v_course = "";
					$v_section = "";
					$v_instructor = "";
					$v_usage = "";
					$v_delete = "";
					$dept_course_section = "";
					$v_loc_code = "";
					$v_bookxofy = "";
					$v_courseid = "";
					$v_ebook_adopted = "";
					$v_class_cap = "";
					$v_prof_requested = "";
					$v_estimated_sales = "";
					
					#check for digital edition
					if (lc($ISBN_HR) ne 'dgt')
					{
						$v_ebook1_vendor = "";
						$v_ebook1_period_1 = "";
						$v_ebook1_price_1 = "";
						$v_ebook1_period_2 = "";
						$v_ebook1_price_2 = "";
						$v_ebook1_period_3 = "";
						$v_ebook1_price_3 = "";
						$v_ebook1_period_4 = "";
						$v_ebook1_price_4 = "";
						$v_ebook1_period_5 = "";
						$v_ebook1_price_5 = "";
						$v_ebook2_vendor = "";
						$v_ebook2_period_1 = "";
						$v_ebook2_price_1 = "";
						$v_ebook2_period_2 = "";
						$v_ebook2_price_2 = "";
						$v_ebook2_period_3 = "";
						$v_ebook2_price_3 = "";
						$v_ebook2_period_4 = "";
						$v_ebook2_price_4 = "";
						$v_ebook2_period_5 = "";
						$v_ebook2_price_5 = "";
					}
					#it is a digital edition
					else
					{
						#base 1 price IPF =701, 2 price = 702, 3 price = 703, 4 price = 704, 5 price = 705
						if (($v_ebook1_price_2 > 0) || ($v_ebook1_price_2 > 0))
						{
							$ITEMIPF = $ITEMIPF +1;
						}
						if (($v_ebook1_price_3 > 0) || ($v_ebook2_price_3 > 0))
						{
							$ITEMIPF = $ITEMIPF +1;
						}
						if (($v_ebook1_price_4 > 0) || ($v_ebook2_price_4 > 0))
						{
							$ITEMIPF = $ITEMIPF +1;
						}
						if (($v_ebook1_price_5 > 0) || ($v_ebook2_price_5 > 0))
						{
							$ITEMIPF = $ITEMIPF +1;
						}
						
						#check for 2 vendors, bump to IPF 71x series
						if ($v_ebook2_vendor ne "")
						{
							$ITEMIPF = $ITEMIPF +10;
						}
					#end digital edition IPF generation
					}
					
					&write_line;
					
					#Add base used ISBN record if required
					if (($Used_ISBN ne '') && ($Used_ISBN ne ' ') && ($ISBN ne ' ') && (lc(substr($ISBN, 0, 3)) ne 'ntr') && ($ISBN ne ''))
					{
						$itemid = $Used_ISBN;
						
						&write_line;
						
						$itemid = $ISBN;						
					}
					
					#check for multi-term, create additional records as needed.
					if ((lc($FormatFlag) eq "a") && ($term_multi eq "true"))
					{
						$v_course_list = "";
						
						#get secondary record, create course list
						$select_str = "SELECT ISBN, Term, Term_Description, dept, course, section, original_section, loc_code, bookxofy, course_id, instructor, course_code, delete_flag, ebook_adopted, class_cap, prof_requested, estimated_sales, ebook1_vendor, ebook1_period_1, ebook1_price_1, ebook1_period_2, ebook1_price_2, ebook1_period_3, ebook1_price_3, ebook1_period_4, ebook1_price_4, ebook1_period_5, ebook1_price_5, ebook2_vendor, ebook2_period_1, ebook2_price_1, ebook2_period_2, ebook2_price_2, ebook2_period_3, ebook2_price_3, ebook2_period_4, ebook2_price_4, ebook2_period_5, ebook2_price_5 FROM t_secondary_file 
							WHERE ISBN = \'$ISBN\' AND Term = \'$Term\'
							ORDER BY dept, course, section;";
						$selectdb = $dbh->prepare($select_str);
						$selectdb->execute();
						#$row = $selectdb->fetchrow_hashref();
						
						if ($display_original ne "true")
						{
							while($row = $selectdb->fetchrow_hashref())
							{
								$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'section'}.", ".$row->{'course_code'};
							}
						}
						else
						{
							while($row = $selectdb->fetchrow_hashref())
							{
								$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'original_section'}.", ".$row->{'course_code'};
							}
						}
						
						$selectdb->finish();
						
						$v_course_list = substr($v_course_list, 2, );
						
						$v_course_list = $v_course_list." <";
						
						#trim to 1024 max length
						$v_course_list = substr($v_course_list, 0, 1024 );
						
						#clean up usages
						if (lc($usage_convert) eq "true")
						{
							if (lc($course_code) eq "required")
								{
									$v_usage = "RQ";
								}
							if (lc($course_code) eq "recommended")
								{
									$v_usage = "RC";
								}
						}
						
						#Add field values prior to write
						$v_dept = $row->{'dept'};
						$v_course = $row->{'course'};
						if ($display_original ne "true")
						{
							$v_section = $row->{'section'};
						}
						else
						{
							$v_section = $row->{'original_section'};
						}						
						$v_instructor = $row->{'instructor'};
						$v_usage = $row->{'course_code'};
						$v_delete = $row->{'delete_flag'};
						$dept_course_section =  $row->{'dept'}." ".$row->{'course'}." ".$row->{'section'};
						$v_loc_code = $row->{'loc_code'};
						$v_bookxofy = $row->{'bookxofy'};
						$v_courseid = $row->{'course_id'};
						$v_ebook_adopted = $row->{'ebook_adopted'};
						$v_class_cap = $row->{'class_cap'};
						$v_prof_requested = $row->{'prof_requested'};
						$v_estimated_sales = $row->{'estimated_sales'};
						$v_ebook1_vendor = $row->{'ebook1_vendor'};
						$v_ebook1_period_1 = $row->{'ebook1_period_1'};
						$v_ebook1_price_1 = $row->{'ebook1_price_1'};
						$v_ebook1_period_2 = $row->{'ebook1_period_2'};
						$v_ebook1_price_2 = $row->{'ebook1_price_2'};
						$v_ebook1_period_3 = $row->{'ebook1_period_3'};
						$v_ebook1_price_3 = $row->{'ebook1_price_3'};
						$v_ebook1_period_4 = $row->{'ebook1_period_4'};
						$v_ebook1_price_4 = $row->{'ebook1_price_4'};
						$v_ebook1_period_5 = $row->{'ebook1_period_5'};
						$v_ebook1_price_5 = $row->{'ebook1_price_5'};
						$v_ebook2_vendor = $row->{'ebook2_vendor'};
						$v_ebook2_period_1 = $row->{'ebook2_period_1'};
						$v_ebook2_price_1 = $row->{'ebook2_price_1'};
						$v_ebook2_period_2 = $row->{'ebook2_period_2'};
						$v_ebook2_price_2 = $row->{'ebook2_price_2'};
						$v_ebook2_period_3 = $row->{'ebook2_period_3'};
						$v_ebook2_price_3 = $row->{'ebook2_price_3'};
						$v_ebook2_period_4 = $row->{'ebook2_period_4'};
						$v_ebook2_price_4 = $row->{'ebook2_price_4'};
						$v_ebook2_period_5 = $row->{'ebook2_period_5'};
						$v_ebook2_price_5 = $row->{'ebook2_price_5'};
						
						$course_hold = "$v_dept-$v_course-$v_section-$Term";
						
						#adjust digital edition
						if (lc($ISBN_HR) eq "dgt")
						{
							#base 1 price IPF =701, 2 price = 702, 3 price = 703, 4 price = 704, 5 price = 705
							if (($v_ebook1_price_2 > 0) || ($v_ebook1_price_2 > 0))
							{
								$ITEMIPF = $ITEMIPF +1;
							}
							if (($v_ebook1_price_3 > 0) || ($v_ebook2_price_3 > 0))
							{
								$ITEMIPF = $ITEMIPF +1;
							}
							if (($v_ebook1_price_4 > 0) || ($v_ebook2_price_4 > 0))
							{
								$ITEMIPF = $ITEMIPF +1;
							}
							if (($v_ebook1_price_5 > 0) || ($v_ebook2_price_5 > 0))
							{
								$ITEMIPF = $ITEMIPF +1;
							}
							
							#check for 2 vendors, bump to IPF 71x series
							if ($v_ebook2_vendor ne "")
							{
								$ITEMIPF = $ITEMIPF +10;
							}
						#end digital edition IPF generation
						}
						
						$itemid = $ISBN.'-'.$course_hold;
						
						&write_line;						
							
						#check if course in db table
						$select_str = "SELECT itemid FROM t_links WHERE itemid = \'$itemid\';";
						$selectdb = $dbh->prepare($select_str);
						$selectdb->execute();
						$row = $selectdb->fetchrow_hashref();
	
						#if not found execute insert
						if (($row->{'itemid'} eq "") && (substr($itemid, 0, 2) ne " -"))
						{
							$insert_str = "INSERT INTO t_links (itemid, isbn, dept, course, section, term) VALUES (\'$itemid\', \'$ISBN\', \'$v_dept\', \'$v_course\', \'$v_section\', \'$Term\');";
							#print "$insert_str\n";
							$insertdb = $dbh->prepare($insert_str);
							$insertdb->execute();
							$insertdb->finish();
						}
						$selectdb->finish();
						
						if (($Used_ISBN ne '') && ($Used_ISBN ne ' ') && ($ISBN ne ' ') && (lc(substr($ISBN, 0, 3)) ne 'ntr') && ($ISBN ne ''))
						{
							$itemid = $Used_ISBN.'-'.$course_hold;
								
							&write_line;
							
							#check if course in db table
							$select_str = "SELECT itemid FROM t_links WHERE itemid = \'$itemid\';";
							$selectdb = $dbh->prepare($select_str);
							$selectdb->execute();
							$row = $selectdb->fetchrow_hashref();
		
							#if not found execute insert
							if (($row->{'itemid'} eq "") && (substr($itemid, 0, 2) ne " -"))
							{
								$insert_str = "INSERT INTO t_links (itemid, isbn, dept, course, section, term) VALUES (\'$itemid\', \'$ISBN\', \'$v_dept\', \'$v_course\', \'$v_section\', \'$Term\');";
								#print "$insert_str\n";
								$insertdb = $dbh->prepare($insert_str);
								$insertdb->execute();
								$insertdb->finish();
							}
							$selectdb->finish();
							
							$itemid = $ISBN.'-'.$course_hold;
							
						}
					}				
				
					#add records for store format "D" (organized by Dept/Course/Section) vs "A", which is alpha by author/title
					if (lc($FormatFlag) eq "d")
					{
						$row = "";
						$v_course_list = "";
						$ebook_count = 0;
						
						#get secondary records for course list
						$select_str = "SELECT ISBN, Term, Term_Description, dept, course, section, original_section, loc_code, bookxofy, course_id, instructor, course_code, delete_flag, ebook_adopted, class_cap, prof_requested, estimated_sales, ebook1_vendor, ebook1_period_1, ebook1_price_1, ebook1_period_2, ebook1_price_2, ebook1_period_3, ebook1_price_3, ebook1_period_4, ebook1_price_4, ebook1_period_5, ebook1_price_5, ebook2_vendor, ebook2_period_1, ebook2_price_1, ebook2_period_2, ebook2_price_2, ebook2_period_3, ebook2_price_3, ebook2_period_4, ebook2_price_4, ebook2_period_5, ebook2_price_5 FROM t_secondary_file 
							WHERE ISBN = \'$ISBN\' AND Term = \'$Term\'
							ORDER BY dept, course, section;";
						$selectdb = $dbh->prepare($select_str);
						$selectdb->execute();
						#$row = $selectdb->fetchrow_hashref();
						
						if ($display_original ne "true")
						{
							while($row = $selectdb->fetchrow_hashref())
							{
								$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'section'}.", ".$row->{'course_code'};
							}
						}
						else
						{
							while($row = $selectdb->fetchrow_hashref())
							{
								$v_course_list = $v_course_list." <> ".$row->{'dept'}.", ".$row->{'course'}.", ".$row->{'originaal_section'}.", ".$row->{'course_code'};
							}
						}						
						
						$selectdb->finish();
						
						$v_course_list = substr($v_course_list, 2, );
						
						$v_course_list = $v_course_list." <";
						
						#trim to 1024 max length
						$v_course_list = substr($v_course_list, 0, 1024 );
						
						$row = "";
						
						#get secondary records for individual record writes
						$select_str = "SELECT ISBN, Term, Term_Description, dept, course, section, original_section, loc_code, bookxofy, course_id, instructor, course_code, delete_flag, ebook_adopted, class_cap, prof_requested, estimated_sales, ebook1_vendor, ebook1_period_1, ebook1_price_1, ebook1_period_2, ebook1_price_2, ebook1_period_3, ebook1_price_3, ebook1_period_4, ebook1_price_4, ebook1_period_5, ebook1_price_5, ebook2_vendor, ebook2_period_1, ebook2_price_1, ebook2_period_2, ebook2_price_2, ebook2_period_3, ebook2_price_3, ebook2_period_4, ebook2_price_4, ebook2_period_5, ebook2_price_5 FROM t_secondary_file 
							WHERE ISBN = \'$ISBN\' AND Term = \'$Term\'
							ORDER BY dept, course, section;";
						$selectdb = $dbh->prepare($select_str);
						$selectdb->execute();
						#$row = $selectdb->fetchrow_hashref();
						
						while($row = $selectdb->fetchrow_hashref())
						{
							#clean up usages
							if (lc($usage_convert) eq "true")
							{
								if (lc($course_code) eq "required")
									{
										$v_usage = "RQ";
									}
								if (lc($course_code) eq "recommended")
									{
										$v_usage = "RC";
									}
							}
							
							#Add field values prior to write
							$v_dept = $row->{'dept'};
							$v_course = $row->{'course'};
							if ($display_original ne "true")
							{
								$v_section = $row->{'section'};
							}
							else
							{
								$v_section = $row->{'original_section'};
							}
							$v_instructor = $row->{'instructor'};
							$v_usage = $row->{'course_code'};
							$v_delete = $row->{'delete_flag'};
							$dept_course_section = $row->{'dept'}." ".$row->{'course'}." ".$row->{'section'};
							$v_loc_code = $row->{'loc_code'};
							$v_bookxofy = $row->{'bookxofy'};
							$v_courseid = $row->{'course_id'};
							$v_ebook_adopted = $row->{'ebook_adopted'};
							$v_class_cap = $row->{'class_cap'};
							$v_prof_requested = $row->{'prof_requested'};
							$v_estimated_sales = $row->{'estimated_sales'};
							$v_ebook1_vendor = $row->{'ebook1_vendor'};
							$v_ebook1_period_1 = $row->{'ebook1_period_1'};
							$v_ebook1_price_1 = $row->{'ebook1_price_1'};
							$v_ebook1_period_2 = $row->{'ebook1_period_2'};
							$v_ebook1_price_2 = $row->{'ebook1_price_2'};
							$v_ebook1_period_3 = $row->{'ebook1_period_3'};
							$v_ebook1_price_3 = $row->{'ebook1_price_3'};
							$v_ebook1_period_4 = $row->{'ebook1_period_4'};
							$v_ebook1_price_4 = $row->{'ebook1_price_4'};
							$v_ebook1_period_5 = $row->{'ebook1_period_5'};
							$v_ebook1_price_5 = $row->{'ebook1_price_5'};
							$v_ebook2_vendor = $row->{'ebook2_vendor'};
							$v_ebook2_period_1 = $row->{'ebook2_period_1'};
							$v_ebook2_price_1 = $row->{'ebook2_price_1'};
							$v_ebook2_period_2 = $row->{'ebook2_period_2'};
							$v_ebook2_price_2 = $row->{'ebook2_price_2'};
							$v_ebook2_period_3 = $row->{'ebook2_period_3'};
							$v_ebook2_price_3 = $row->{'ebook2_price_3'};
							$v_ebook2_period_4 = $row->{'ebook2_period_4'};
							$v_ebook2_price_4 = $row->{'ebook2_price_4'};
							$v_ebook2_period_5 = $row->{'ebook2_period_5'};
							$v_ebook2_price_5 = $row->{'ebook2_price_5'};
							
							$v_section_hold = $v_section;
							
							if ($section_commas ne "")
							{
								
								$v_section_hold =~ s/\,/$section_commas/g;
								$v_section_hold =~ s/ /$section_commas/g;
								$v_instructor =~ s/\'/ /g;
							}
						
							$course_hold = "$v_dept-$v_course-$v_section_hold-$Term";
						
							$itemid = $ISBN.'-'.$course_hold;
							
							#clean up stray spaces
							$itemid =~ s/ /$section_commas/g;
							
							$out_price_1_hold = $out_price_1;
							$out_text_1_hold = $out_text_1;
							$out_price_2_hold = $out_price_2;
							$out_text_2_hold = $out_text_2;
							$out_price_3_hold = $out_price_3;
							$out_text_3_hold = $out_text_3;
							$out_price_4_hold = $out_price_4;
							$out_text_4_hold = $out_text_4;
							$out_price_5_hold = $out_price_5;
							$out_text_5_hold = $out_text_5;
							$ITEMIPF_hold = $ITEMIPF;
							
							#check whether ebook not adopted for this course but there is an ebook price and it is a regular price template, not special template
							if ((lc($v_ebook_adopted) ne "y") && ($Ebook_Price > 0) && (substr($ITEMIPF, 2, 1) eq "0") && ($ebook_count == 0))
							{
								#find out haw many prices
								$price_count = substr($ITEMIPF, 0, 1);
								#check position of ebook in sequence
								if ($out_text_1 eq $Ebook_Price_Text)
								{
									$price_loc = 1;
								}
								if ($out_text_2 eq $Ebook_Price_Text)
								{
									$price_loc = 2;
								}
								if ($out_text_3 eq $Ebook_Price_Text)
								{
									$price_loc = 3;
								}
								if ($out_text_4 eq $Ebook_Price_Text)
								{
									$price_loc = 4;
								}
								if ($out_text_5 eq $Ebook_Price_Text)
								{
									$price_loc = 5;
								}
								
								#knock off ebook if last one
								if ($price_count == $price_loc)
								{
									if ($price_loc == 2)
									{
										$out_text_2 = "";
										$out_price_2 = 0;
									}
									if ($price_loc == 3)
									{
										$out_text_3 = "";
										$out_price_3 = 0;
									}
									if ($price_loc == 4)
									{
										$out_text_4 = "";
										$out_price_4 = 0;
									}
									if ($price_loc == 5)
									{
										$out_text_5 = "";
										$out_price_5 = 0;
									}
								}
								else
								{
									#shuffle up!
									#ebook top
									if ($price_loc == 1)
									{
										$out_price_1 = $out_price_2;
										$out_text_1 = $out_text_2;
										$out_price_2 = $out_price_3;
										$out_text_2 = $out_text_3;
										$out_price_3 = $out_price_4;
										$out_text_3 = $out_text_4;
										$out_price_4 = $out_price_5;
										$out_text_4 = $out_text_5;
										$out_price_5 = "";
										$out_text_5 = "";
									}
									elsif ($price_loc == 2)
									{
										$out_price_2 = $out_price_3;
										$out_text_2 = $out_text_3;
										$out_price_3 = $out_price_4;
										$out_text_3 = $out_text_4;
										$out_price_4 = $out_price_5;
										$out_text_4 = $out_text_5;
										$out_price_5 = "";
										$out_text_5 = "";
									}
									elsif ($price_loc == 3)
									{
										$out_price_3 = $out_price_4;
										$out_text_3 = $out_text_4;
										$out_price_4 = $out_price_5;
										$out_text_4 = $out_text_5;
										$out_price_5 = "";
										$out_text_5 = "";
									}
									elsif ($price_loc == 4)
									{
										$out_price_4 = $out_price_5;
										$out_text_4 = $out_text_5;
										$out_price_5 = "";
										$out_text_5 = "";
									}
									elsif ($price_loc == 5)
									{
										$out_price_5 = "";
										$out_text_5 = "";
									}	
								}
								
								#drop 100 from IPF if first time through
								$ITEMIPF = $ITEMIPF - 100;
								
								$ebook_count = 1;
							}
							elsif (lc($v_ebook_adopted) eq "y")
							{
								$out_price_1 = $out_price_1_hold;
								$out_text_1 = $out_text_1_hold;
								$out_price_2 = $out_price_2_hold;
								$out_text_2 = $out_text_2_hold;
								$out_price_3 = $out_price_3_hold;
								$out_text_3 = $out_text_3_hold;
								$out_price_4 = $out_price_4_hold;
								$out_text_4 = $out_text_4_hold;
								$out_price_5 = $out_price_5_hold;
								$out_text_5 = $out_text_5_hold;
								$ITEMIPF = $ITEMIPF_hold;
								
								$ebook_count = 0;
							}
							
							if (substr($itemid, 1, 1) ne '-')
							{
								#adjust digital edition
								if (lc($ISBN_HR) eq "dgt")
								{
									#base 1 price IPF =701, 2 price = 7-2, 3 price = 703, 4 price = 704, 5 price = 705
									if (($v_ebook1_price_2 > 0) || ($v_ebook1_price_2 > 0))
									{
										$ITEMIPF = $ITEMIPF +1;
									}
									if (($v_ebook1_price_3 > 0) || ($v_ebook2_price_3 > 0))
									{
										$ITEMIPF = $ITEMIPF +1;
									}
									if (($v_ebook1_price_4 > 0) || ($v_ebook2_price_4 > 0))
									{
										$ITEMIPF = $ITEMIPF +1;
									}
									if (($v_ebook1_price_5 > 0) || ($v_ebook2_price_5 > 0))
									{
										$ITEMIPF = $ITEMIPF +1;
									}
									
									#check for 2 vendors, bump to IPF 71x series
									if ($v_ebook2_vendor ne "")
									{
										$ITEMIPF = $ITEMIPF +10;
									}
								#end digital edition IPF generation
								}
								
								&write_line;

								#check if course in db table
								$select_str = "SELECT itemid FROM t_links WHERE itemid = \'$itemid\';";
								$selectdb1 = $dbh->prepare($select_str) or die "Unable to prepare $select_str" . $dbh->errstr;
								$selectdb1->execute() or die "Unable to execute '$select_str'.  " . $selectdb1->errstr;
								$row1 = $selectdb1->fetchrow_hashref();
			
								#if not found execute insert
								if (($row1->{'itemid'} ne $itemid) && (substr($itemid, 0, 2) ne " -"))
								{
									$insert_str = "INSERT INTO t_links (itemid, isbn, dept, course, section, term) VALUES (\'$itemid\', \'$ISBN\', \'$v_dept\', \'$v_course\', \'$v_section\', \'$Term\');";
									$insertdb = $dbh->prepare($insert_str);
									$insertdb->execute();
									$insertdb->finish();
								}
								
								$selectdb1->finish();
							
							}
							
							#also check if course in db table by used isbn, add link record and write pfi record 
							if (($Used_ISBN ne '') && ($Used_ISBN ne ' ') && ($ISBN ne ' ') && (lc(substr($ISBN, 0, 3)) ne 'ntr') && ($ISBN ne ''))
							{
								
								$itemid = $Used_ISBN.'-'.$course_hold;
								
								#print "Used $itemid\n";
								
								#clean up stray spaces
								$itemid =~ s/ /$section_commas/g;
								
								if (substr($itemid, 1, 1) ne '-')
								{
									&write_line;
									
									$select_str = "SELECT itemid FROM t_links WHERE itemid = \'$itemid\';";
									$selectdb1 = $dbh->prepare($select_str) or die "Unable to prepare $select_str" . $dbh->errstr;
									$selectdb1->execute() or die "Unable to execute '$select_str'.  " . $selectdb1->errstr;
									$row1 = $selectdb1->fetchrow_hashref();
				
									#if not found execute insert
									if (($row1->{'itemid'} ne $itemid) && (substr($itemid, 0, 2) ne " -"))
									{
										$insert_str = "INSERT INTO t_links (itemid, isbn, dept, course, section, term) VALUES (\'$itemid\', \'$Used_ISBN\', \'$v_dept\', \'$v_course\', \'$v_section\', \'$Term\');";
										$insertdb = $dbh->prepare($insert_str);
										$insertdb->execute();
										$insertdb->finish();
									}
									
									$selectdb1->finish();
									
									$itemid = $ISBN.'-'.$course_hold;
								}
							}
						}
					#end D record
					}
					
				##end book record
				}
				
				#### GM Record #####
				if (lc($Category) eq "gm")
				{	
					#establish gm dates
					$sale_start1 = "1900-01-01 00:00";
					$sale_end1 = "1900-01-01 00:00:00";
					$sale_start2 = "1900-01-01 00:00:00";
					$sale_end2 = "1900-01-01 00:00:00";
					$sale_start3 = "1900-01-01 00:00:00";
					$sale_end3 = "1900-01-01 00:00:00";
					$sale_start4 = "1900-01-01 00:00:00";
					$sale_end4 = "1900-01-01 00:00:00";
					
					#set up GM sale date information
					if ((length($Sale_Start_Date1) == 8) && (length($Sale_Start_Time1) == 4))
					{
						$sale_start1 = substr($Sale_Start_Date1,0,4).'-'.substr($Sale_Start_Date1,4,2).'-'.substr($Sale_Start_Date1,6,2).' '.substr($Sale_Start_Time1,0,2).':'.substr($Sale_Start_Time1,2,2).':00';
					}
					
					if ((length($Sale_End_Date1) == 8) && (length($Sale_End_Time1) == 4))
					{
						$sale_end1 = substr($Sale_End_Date1,0,4).'-'.substr($Sale_End_Date1,4,2).'-'.substr($Sale_End_Date1,6,2).' '.substr($Sale_End_Time1,0,2).':'.substr($Sale_End_Time1,2,2).':00';
					}
					
					if ((length($Sale_Start_Date2) == 8) && (length($Sale_Start_Time2) == 4))
					{
						$sale_start2 = substr($Sale_Start_Date2,0,4).'-'.substr($Sale_Start_Date2,4,2).'-'.substr($Sale_Start_Date2,6,2).' '.substr($Sale_Start_Time2,0,2).':'.substr($Sale_Start_Time2,2,2).':00';
					}
						
					if ((length($Sale_End_Date2) == 8) && (length($Sale_End_Time2) == 4))
					{	
						$sale_end2 = substr($Sale_End_Date2,0,4).'-'.substr($Sale_End_Date2,4,2).'-'.substr($Sale_End_Date2,6,2).' '.substr($Sale_End_Time2,0,2).':'.substr($Sale_End_Time2,2,2).':00';
					}
					
					if ((length($Sale_Start_Date3) == 8) && (length($Sale_Start_Time3) == 4))
					{
						$sale_start3 = substr($Sale_Start_Date3,0,4).'-'.substr($Sale_Start_Date3,4,2).'-'.substr($Sale_Start_Date3,6,2).' '.substr($Sale_Start_Time3,0,2).':'.substr($Sale_Start_Time3,2,2).':00';
					}
					
					if ((length($Sale_End_Date3) == 8) && (length($Sale_End_Time3) == 4))
					{
						$sale_end3 = substr($Sale_End_Date3,0,4).'-'.substr($Sale_End_Date3,4,2).'-'.substr($Sale_End_Date3,6,2).' '.substr($Sale_End_Time3,0,2).':'.substr($Sale_End_Time3,2,2).':00';
					}
			
					if ((length($Sale_Start_Date4) == 8) && (length($Sale_Start_Time4) == 4))
					{
						$sale_start4 = substr($Sale_Start_Date4,0,4).'-'.substr($Sale_Start_Date4,4,2).'-'.substr($Sale_Start_Date4,6,2).' '.substr($Sale_Start_Time4,0,2).':'.substr($Sale_Start_Time4,2,2).':00';
					}
					
					if ((length($Sale_End_Date4) == 8) && (length($Sale_End_Time4) == 4))
					{
						$sale_end4 = substr($Sale_End_Date4,0,4).'-'.substr($Sale_End_Date4,4,2).'-'.substr($Sale_End_Date4,6,2).' '.substr($Sale_End_Time4,0,2).':'.substr($Sale_End_Time4,2,2).':00';
					}
					
					$Sale_Price = 0;
					$save_amount = 0;
					$itemid = $ISBN;
					$Regular_Price = $New_Price;
					$save_amount = 0;
					$ITEMIPF = "Regular";
					
					$Regular_Price = sprintf '%.2f', $Regular_Price;
	
					if (($Sale_Price1 > 0) && ($sale_start1 gt '1900-01-01 00:00:00'))
					{			
						#Check if Item currently on Sale
						if (($long_now ge $sale_start1) && ($long_now le $sale_end1))
						{
							$save_amount = $Regular_Price - $Sale_Price1;
							$ITEMIPF = "Sale";
							
							$Sale_Price = sprintf '%.2f', $Sale_Price1;
							$save_amount = sprintf '%.2f', $save_amount;
							$target_date_start = $sale_start1;
							$target_date_end = $sale_end1;
							
							#write line for currently on sale
							&write_line;
						}
						elsif (($sale_start1 ge $long_now) && ($sale_end1 ge $long_now))
						{
							#start item delayed processing
							#######sale start
							$save_amount = $Regular_Price - $Sale_Price1;
							$ITEMIPF = "Sale";
							$target_date_start = $sale_start1;
							$target_date_end = $sale_end1;
							$target_delay = $sale_start1;
							
							$Sale_Price = sprintf '%.2f', $Sale_Price1;
							$save_amount = sprintf '%.2f', $save_amount;
							
							&write_line_delayed;
							
							#######sale end
							$ITEMIPF = "Regular";
							$save_amount = 0;
							$target_delay = $sale_end1;
							
							&write_line_delayed;
						}
					}
					
					if (($Sale_Price2 > 0) && ($sale_start2 gt '1900-01-01 00:00:00'))
					{			
						#Check if Item currently on Sale
						if (($long_now ge $sale_start2) && ($long_now le $sale_end2))
						{
							$save_amount = $Regular_Price - $Sale_Price2;
							$ITEMIPF = "Sale";
							
							$Sale_Price = sprintf '%.2f', $Sale_Price2;
							$save_amount = sprintf '%.2f', $save_amount;
							$target_date_start = $sale_start2;
							$target_date_end = $sale_end2;
							
							#write line for currently on sale
							&write_line;
						}
						elsif (($sale_start2 ge $long_now) && ($sale_end2 ge $long_now))
						{
							#start item delayed processing
							#######sale start
							$save_amount = $Regular_Price - $Sale_Price2;
							$ITEMIPF = "Sale";
							$target_date_start = $sale_start2;
							$target_date_end = $sale_end2;
							$target_delay = $sale_start2;
							
							$Sale_Price = sprintf '%.2f', $Sale_Price2;
							$save_amount = sprintf '%.2f', $save_amount;
							
							&write_line_delayed;
							
							#######sale end
							$ITEMIPF = "Regular";
							$save_amount = 0;
							$target_delay = $sale_end2;
							
							&write_line_delayed;
						}
					}
					
					if (($Sale_Price3 > 0) && ($sale_start3 gt '1900-01-01 00:00:00'))
					{			
						#Check if Item currently on Sale
						if (($long_now ge $sale_start3) && ($long_now le $sale_end3))
						{
							$save_amount = $Regular_Price - $Sale_Price3;
							$ITEMIPF = "Sale";
							
							$Sale_Price = sprintf '%.2f', $Sale_Price3;
							$save_amount = sprintf '%.2f', $save_amount;
							$target_date_start = $sale_start1;
							$target_date_end = $sale_end1;
							
							#write line for currently on sale
							&write_line;
						}
						elsif (($sale_start3 ge $long_now) && ($sale_end3 ge $long_now))
						{
							#start item delayed processing
							#######sale start
							$save_amount = $Regular_Price - $Sale_Price3;
							$ITEMIPF = "Sale";
							$target_date_start = $sale_start3;
							$target_date_end = $sale_end3;
							$target_delay = $sale_start3;
							
							$Sale_Price = sprintf '%.2f', $Sale_Price3;
							$save_amount = sprintf '%.2f', $save_amount;
							
							&write_line_delayed;
							
							#######sale end
							$ITEMIPF = "Regular";
							$save_amount = 0;
							$target_delay = $sale_end3;
							
							&write_line_delayed;
						}
					}
					if (($Sale_Price4 > 0) && ($sale_start4 gt '1900-01-01 00:00:00'))
					{			
						#Check if Item currently on Sale
						if (($long_now ge $sale_start4) && ($long_now le $sale_end4))
						{
							$save_amount = $Regular_Price - $Sale_Price4;
							$ITEMIPF = "Sale";
							
							$Sale_Price = sprintf '%.2f', $Sale_Price4;
							$save_amount = sprintf '%.2f', $save_amount;
							$target_date_start = $sale_start4;
							$target_date_end = $sale_end4;
							
							#write line for currently on sale
							&write_line;
						}
						elsif (($sale_start4 ge $long_now) && ($sale_end4 ge $long_now))
						{
							#start item delayed processing
							#######sale start
							$save_amount = $Regular_Price - $Sale_Price4;
							$ITEMIPF = "Sale";
							$target_date_start = $sale_start4;
							$target_date_end = $sale_end4;
							$target_delay = $sale_start4;
							
							$Sale_Price = sprintf '%.2f', $Sale_Price4;
							$save_amount = sprintf '%.2f', $save_amount;
							
							&write_line_delayed;
							
							#######sale end
							$ITEMIPF = "Regular";
							$save_amount = 0;
							$target_delay = $sale_end4;
							
							&write_line_delayed;
						}
					}
					
					if (($Sale_Price1 == 0) && ($Sale_Price2 == 0) && ($Sale_Price3 == 0) && ($Sale_Price4 == 0))
					{
						#write regular price output
						&write_line;
					}				
				#### End GM Record
				}
				
				if ((lc($Category) ne "gm") && (lc($Category) ne "t"))
				{
					if (lc($Logging) eq "true")
					{
						log_writer("$ISBN in $in_file failed Category decision, no record output", $log_file);
					}	
				}
			#end valid ISBN/SKU
			}
			else
			{
				if ($Logging eq "true")
				{
					log_writer("Row $in_count dropped, invalid ISBN/SKU", $log_file);
				}	
			}
			
		#end zero length line
		}
		else
		{
			if ($Logging eq "true")
			{
				log_writer("Row $in_count dropped, no data", $log_file);
			}	
		}
	#end valid license loop
	}	
#end input record while loop
}

if (($verbose eq "true") && ($Logging eq "true"))
{
	log_writer("$in_count records read from $in_file", $log_file);
	log_writer("$out_count records written to $i1_file", $log_file);
}

close (INFILE);
close (OUTFILE);
$dbh->disconnect();

#generate m1 file
unless(open (M1FILE, $hold_m1_file))
{
	log_writer("Could not open $temp_m1_file!", $log_file);
	goto END;
}

log_writer("$temp_m1_file opened for message record write", $log_file);

$new_line = "UPDATE, 0001,,".$i1_file.",".$r7_file;
print M1FILE "$new_line\n";

close (M1FILE);

log_writer("Temp message file $temp_m1_file closed", $log_file);

rename $temp_m1_file, $m1_file;

log_writer("Temp file $temp_m1_file renamed to $m1_file", $log_file);

############# clean up files #####################
if ($data_delete eq "false")
{
	move($in_file, $processed_path);
	move($ins_file, $processed_path_1);
	if ($Logging eq "true")
		{
			log_writer("Moved $in_file to $processed_path and $ins_file to $processed_path_1", $log_file);
		}
}
else
{
	unlink $in_file;
	unlink $ins_file;
	if ($Logging eq "true")
		{
			log_writer("Deleted $in_file and $ins_file\n", $log_file);
		}
}

if ($Logging eq "true")
{
	log_writer("Execution complete, closing log file\n", $log_file);
}

if ($Interactive eq "true")
{
	print "Press Enter to continue: ";
	$resp = <STDIN>;	
}

END:
exit 0;

############ write line to output file subroutine #########################
sub write_line
{
	$new_line = 
	"0001 ".$itemid
	." 7 0 |".$Author." ".$Title
	."| 9800 0 |".$Author
	."| 9801 0 |".$Title
	."| 9802 0 |".$ISBN
	."| 9803 0 |".$ISBN_HR
	."| 9804 0 |".$Vendor_Style
	."| 9805 0 |".$Publisher
	."| 9806 0 |".$Imprint
	."| 9807 0 |".$Edition
	."| 9895 0 |".$Edition_Status
	."| 9808 0 |".$New_Price
	."| 9809 0 |".$New_Price_Text
	."| 9810 0 |".$Used_Price
	."| 9811 0 |".$Used_Price_Text
	."| 9812 0 |".$New_Rental_Price
	."| 9813 0 |".$New_Rental_Price_Text
	."| 9814 0 |".$Ebook_Price
	."| 9815 0 |".$Ebook_Price_Text
	."| 9816 0 |".$Used_Rental_Price
	."| 9817 0 |".$Used_Rental_Price_Text
	."| 23 0 |".$Regular_Price
	."| 9818 0 |".$Sale_Price
	."| 9819 0 |".$target_date_start
	."| 9820 0 |".$target_date_end
	."| 9821 0 |".$Term
	."| 9822 0 |".$Term_Description
	."| 9823 0 |".$Requested_Qty
	."| 9824 0 |".$Class_Capacity_Qty
	."| 9825 0 |".$Actual_Enrollment_Qty
	."| 9826 0 |".$Est_Sales_Qty
	."| 9827 0 |".$Category
	."| 9828 0 |".$Division
	."| 9829 0 |".$Department
	."| 9830 0 |".$Class
	."| 9831 0 |".$New_Store_Qty
	."| 9832 0 |".$New_Warehouse_Qty
	."| 9833 0 |".$Used_Store_Qty
	."| 9834 0 |".$Used_Warehouse_Qty
	."| 9835 0 |".$On_Order_PO1
	."| 9836 0 |".$On_Order_Qty1
	."| 9837 0 |".$On_Order_Date1
	."| 9838 0 |".$On_Order_PO2
	."| 9839 0 |".$On_Order_Qty2
	."| 9840 0 |".$On_Order_Date2
	."| 9841 0 |".$On_Order_PO3
	."| 9842 0 |".$On_Order_Qty3
	."| 9843 0 |".$On_Order_Date3
	."| 9844 0 |".$Total_PO_Qty
	."| 121 0 |".$ITEMIPF
	."| 9885 0 |".$save_amount
	."| 9886 0 |".$out_price_1
	."| 9887 0 |".$out_text_1
	."| 9888 0 |".$out_price_2
	."| 9889 0 |".$out_text_2
	."| 9890 0 |".$out_price_3
	."| 9891 0 |".$out_text_3
	."| 9892 0 |".$out_price_4
	."| 9893 0 |".$out_text_4
	."| 9894 0 |".$out_price_5
	."| 9896 0 |".$out_text_5
	."| 9898 0 |".$v_dept
	."| 9897 0 |".$v_course
	."| 9899 0 |".$v_section
	."| 9900 0 |".$v_instructor
	."| 9914 0 |".$v_usage
	."| 9902 0 |".$v_delete
	."| 9915 0 |".$dept_course_section
	."| 9845 0 |".$v_loc_code
	."| 9846 0 |".$v_bookxofy
	."| 9847 0 |".$v_courseid
	."| 9848 0 |".$v_course_list
	."| 9849 0 |".$v_ebook_adopted
	."| 9850 0 |".$v_class_cap
	."| 9851 0 |".$v_prof_requested
	."| 9852 0 |".$v_estimated_sales
	."| 9853 0 |".$New_Pending_Return_Qty
	."| 9854 0 |".$Used_Pending_Return_Qty
	."| 9855 0 |".$New_insite_Pending_Order
	."| 9856 0 |".$Used_insite_Pending_Order
	."| 9857 0 |".$New_Rental_insite_Pending_Order
	."| 9858 0 |".$Used_Rental_insite_Pending_Order
	."| 9859 0 |".$On_Order_PO1_Vendor
	."| 9860 0 |".$On_Order_PO2_Vendor
	."| 9861 0 |".$On_Order_PO3_Vendor
	."| 9862 0 |".$On_Order_Qty1_Used
	."| 9863 0 |".$On_Order_Qty2_Used
	."| 9864 0 |".$On_Order_Qty3_Used	
	."| 9865 0 |".$Used_ISBN	
	."| 9903 0 |".$FormatFlag
	."| 9916 0 |".$Sale_Price1
	."| 9917 0 |".$Sale_Start_Date1
	."| 9918 0 |".$Sale_End_Date1
	."| 9919 0 |".$Sale_Start_Time1
	."| 9920 0 |".$Sale_End_Time1
	."| 9921 0 |".$Sale_Price2
	."| 9922 0 |".$Sale_Start_Date2
	."| 9923 0 |".$Sale_End_Date2
	."| 9924 0 |".$Sale_Start_Time2
	."| 9925 0 |".$Sale_End_Time2
	."| 9926 0 |".$Sale_Price3
	."| 9927 0 |".$Sale_Start_Date3
	."| 9928 0 |".$Sale_End_Date3
	."| 9929 0 |".$Sale_Start_Time3
	."| 9930 0 |".$Sale_End_Time3
	."| 9931 0 |".$Sale_Price4
	."| 9932 0 |".$Sale_Start_Date4
	."| 9933 0 |".$Sale_End_Date4
	."| 9934 0 |".$Sale_Start_Time4
	."| 9935 0 |".$Sale_End_Time4
	."| 9936 0 |".$image_name
	."| 9937 0 |".$v_ebook1_vendor
	."| 9938 0 |".$v_ebook1_period_1
	."| 9939 0 |".$v_ebook1_price_1
	."| 9940 0 |".$v_ebook1_period_2
	."| 9941 0 |".$v_ebook1_price_2
	."| 9942 0 |".$v_ebook1_period_3
	."| 9943 0 |".$v_ebook1_price_3
	."| 9944 0 |".$v_ebook1_period_4
	."| 9945 0 |".$v_ebook1_price_4
	."| 9946 0 |".$v_ebook1_period_5
	."| 9947 0 |".$v_ebook1_price_5
	."| 9948 0 |".$v_ebook2_vendor
	."| 9949 0 |".$v_ebook2_period_1
	."| 9950 0 |".$v_ebook2_price_1
	."| 9951 0 |".$v_ebook2_period_2
	."| 9952 0 |".$v_ebook2_price_2
	."| 9953 0 |".$v_ebook2_period_3
	."| 9954 0 |".$v_ebook2_price_3
	."| 9955 0 |".$v_ebook2_period_4
	."| 9956 0 |".$v_ebook2_price_4
	."| 9957 0 |".$v_ebook2_period_5
	."| 9958 0 |".$v_ebook2_price_5
	."|,";
	
	print OUTFILE "$new_line\n";
	#print "$line\n";
	$out_count++;
}

############ write delayed line to output file subroutine #########################
sub write_line_delayed
{
	$new_line = 
	"0001 ".$itemid
	." 7 0 |".$Author." ".$Title
	."| 9800 0 |".$Author
	."| 9801 0 |".$Title
	."| 9802 0 |".$ISBN
	."| 9803 0 |".$ISBN_HR
	."| 9804 0 |".$Vendor_Style
	."| 9805 0 |".$Publisher
	."| 9806 0 |".$Imprint
	."| 9807 0 |".$Edition
	."| 9895 0 |".$Edition_Status
	."| 9808 0 |".$New_Price
	."| 9809 0 |".$New_Price_Text
	."| 9810 0 |".$Used_Price
	."| 9811 0 |".$Used_Price_Text
	."| 9812 0 |".$New_Rental_Price
	."| 9813 0 |".$New_Rental_Price_Text
	."| 9814 0 |".$Ebook_Price
	."| 9815 0 |".$Ebook_Price_Text
	."| 9816 0 |".$Used_Rental_Price
	."| 9817 0 |".$Used_Rental_Price_Text
	."| 23 0 |".$Regular_Price
	."| 9818 0 |".$Sale_Price
	."| 9819 0 |".$Sale_Start_Date
	."| 9820 0 |".$Sale_End_Date
	."| 9821 0 |".$Term
	."| 9822 0 |".$Term_Description
	."| 9823 0 |".$Requested_Qty
	."| 9824 0 |".$Class_Capacity_Qty
	."| 9825 0 |".$Actual_Enrollment_Qty
	."| 9826 0 |".$Est_Sales_Qty
	."| 9827 0 |".$Category
	."| 9828 0 |".$Division
	."| 9829 0 |".$Department
	."| 9830 0 |".$Class
	."| 9831 0 |".$New_Store_Qty
	."| 9832 0 |".$New_Warehouse_Qty
	."| 9833 0 |".$Used_Store_Qty
	."| 9834 0 |".$Used_Warehouse_Qty
	."| 9835 0 |".$On_Order_PO1
	."| 9836 0 |".$On_Order_Qty1
	."| 9837 0 |".$On_Order_Date1
	."| 9838 0 |".$On_Order_PO2
	."| 9839 0 |".$On_Order_Qty2
	."| 9840 0 |".$On_Order_Date2
	."| 9841 0 |".$On_Order_PO3
	."| 9842 0 |".$On_Order_Qty3
	."| 9843 0 |".$On_Order_Date3
	."| 9844 0 |".$Total_PO_Qty
	."| 121 0 |".$ITEMIPF
	."| 9885 0 |".$save_amount
	."| 9886 0 |".$out_price_1
	."| 9887 0 |".$out_text_1
	."| 9888 0 |".$out_price_2
	."| 9889 0 |".$out_text_2
	."| 9890 0 |".$out_price_3
	."| 9891 0 |".$out_text_3
	."| 9892 0 |".$out_price_4
	."| 9893 0 |".$out_text_4
	."| 9894 0 |".$out_price_5
	."| 9896 0 |".$out_text_5
	."| 9898 0 |".$v_dept
	."| 9897 0 |".$v_course
	."| 9899 0 |".$v_section
	."| 9900 0 |".$v_instructor
	."| 9914 0 |".$v_usage
	."| 9902 0 |".$v_delete
	."| 9915 0 |".$dept_course_section
	."| 9845 0 |".$v_loc_code
	."| 9846 0 |".$v_bookxofy
	."| 9847 0 |".$v_courseid
	."| 9848 0 |".$v_course_list
	."| 9849 0 |".$v_ebook_adopted
	."| 9850 0 |".$v_class_cap
	."| 9851 0 |".$v_prof_requested
	."| 9852 0 |".$v_estimated_sales
	."| 9853 0 |".$New_Pending_Return_Qty
	."| 9854 0 |".$Used_Pending_Return_Qty
	."| 9855 0 |".$New_insite_Pending_Order
	."| 9856 0 |".$Used_insite_Pending_Order
	."| 9857 0 |".$New_Rental_insite_Pending_Order
	."| 9858 0 |".$Used_Rental_insite_Pending_Order
	."| 9859 0 |".$On_Order_PO1_Vendor
	."| 9860 0 |".$On_Order_PO2_Vendor
	."| 9861 0 |".$On_Order_PO3_Vendor
	."| 9862 0 |".$On_Order_Qty1_Used
	."| 9863 0 |".$On_Order_Qty2_Used
	."| 9864 0 |".$On_Order_Qty3_Used
	."| 9865 0 |".$Used_ISBN	
	."| 9903 0 |".$FormatFlag
	."| 9916 0 |".$Sale_Price1
	."| 9917 0 |".$Sale_Start_Date1
	."| 9918 0 |".$Sale_End_Date1
	."| 9919 0 |".$Sale_Start_Time1
	."| 9920 0 |".$Sale_End_Time1
	."| 9921 0 |".$Sale_Price2
	."| 9922 0 |".$Sale_Start_Date2
	."| 9923 0 |".$Sale_End_Date2
	."| 9924 0 |".$Sale_Start_Time2
	."| 9925 0 |".$Sale_End_Time2
	."| 9926 0 |".$Sale_Price3
	."| 9927 0 |".$Sale_Start_Date3
	."| 9928 0 |".$Sale_End_Date3
	."| 9929 0 |".$Sale_Start_Time3
	."| 9930 0 |".$Sale_End_Time3
	."| 9931 0 |".$Sale_Price4
	."| 9932 0 |".$Sale_Start_Date4
	."| 9933 0 |".$Sale_End_Date4
	."| 9934 0 |".$Sale_Start_Time4
	."| 9935 0 |".$Sale_End_Time4
	."| 9500 0 |".$target_delay
	."| 9936 0 |".$image_name
	."| 9937 0 |".$v_ebook1_vendor
	."| 9938 0 |".$v_ebook1_period_1
	."| 9939 0 |".$v_ebook1_price_1
	."| 9940 0 |".$v_ebook1_period_2
	."| 9941 0 |".$v_ebook1_price_2
	."| 9942 0 |".$v_ebook1_period_3
	."| 9943 0 |".$v_ebook1_price_3
	."| 9944 0 |".$v_ebook1_period_4
	."| 9945 0 |".$v_ebook1_price_4
	."| 9946 0 |".$v_ebook1_period_5
	."| 9947 0 |".$v_ebook1_price_5
	."| 9948 0 |".$v_ebook2_vendor
	."| 9949 0 |".$v_ebook2_period_1
	."| 9950 0 |".$v_ebook2_price_1
	."| 9951 0 |".$v_ebook2_period_2
	."| 9952 0 |".$v_ebook2_price_2
	."| 9953 0 |".$v_ebook2_period_3
	."| 9954 0 |".$v_ebook2_price_3
	."| 9955 0 |".$v_ebook2_period_4
	."| 9956 0 |".$v_ebook2_price_4
	."| 9957 0 |".$v_ebook2_period_5
	."| 9958 0 |".$v_ebook2_price_5
	."|,";
	
	print OUTFILE "$new_line\n";
	#print "$line\n";
	$out_count++;
}