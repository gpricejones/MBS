<?php 

require_once('connections/db_connect.php');

$db = mysqli_select_db($link,'Pricer') or die('Could not select database');

if (isset($_POST['lookup_action']))
{
	$lookup_action = $_POST['lookup_action'];
}
else
{
	$lookup_action = "";
}

//echo "Action: ".$lookup_action;

if (isset($_POST['isbn']))
{
	$isbn_sel = $_POST['isbn'];
}
else
{
	$isbn_sel = "";
}

if (isset($_POST['itemid']))
{
	$itemid_sel = $_POST['itemid'];
}
else
{
	$itemid_sel = "";
}

if (isset($_POST['address']))
{
	$address_sel = $_POST['address'];
}
else
{
	$address_sel = "";
}

if ($lookup_action == "")
{
	$isbn_sel = "";
	$address_sel = "";
}	

//search for isbn matches
if ($lookup_action == "find")
{
	$lu_str = "CALL sp_lookup_isbn ('".$isbn_sel."');";
	
	$lu_det = mysqli_query($link, $lu_str);
	if (!$lu_det) 
	{
		printf("Error: %s\n", mysqli_error($link));
		exit();
	}
}

//on second reload generate link
if ($lookup_action == "link")
{
	$timezone = date_default_timezone_set('UTC');
	$file = date('Ymdhis', time());

	$i1 = "c:\\pricer\\PFIFiles\\DataFiles\\".$file.".I1";
	$m1 = "c:\\pricer\\PFIFiles\\MessageFiles\\".$file.".M1";
	$r7 = "c:\\pricer\\PFIFiles\\ResultFiles\\".$file.".R7";
	
	$fh = fopen($i1,'w') or die("can't open I1 file: $php_errormsg");
	
	$write_str = '0001 '.$itemid_sel.' 1 0 |N| 93 0 |'.$address_sel.'|,';

	fwrite($fh, $write_str );

	fclose ($fh);
	
	$fh = fopen($m1,'w') or die("can't open I1 file: $php_errormsg");
	
	$write_str = 'TARGETLINK,0001,,'.$i1.','.$r7;
	
	fwrite($fh, $write_str );

	fclose ($fh);
	

	
	echo "<!DOCTYPE HTML><html><head><meta http-equiv=\"REFRESH\" content=\"0;url=http://localhost:8000/idcs_link.php\"></head><body></body></html>";

}
?>


<!DOCTYPE HTML>
<html>
<head>
<title>ESL Link</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta http-equiv="expires" content="0">
<script src="scripts/sorttable.js" type="text/javascript"></script>
<link href="css/general.css" rel="stylesheet" type="text/css" />

<script type="text/javascript">
function close_window() 
{
    close();
}
</script>
</head>

<body>
<?php include("includes/header1.php"); ?>
<table border="0">
  <form name="find" method="post" action="idcs_link.php" id="find" >
  <tr>
    <td class="cell">ESL Barcode</td>
    <td>&nbsp;</td>
    <td>
      <input name="address" type="text" id="address" size="20" maxlength="20" value="<?php echo $address_sel; ?>" ></td>
    </tr>
      <tr>
    <td colspan="3">&nbsp;</td>
  </tr>
    <tr>
    <td class="cell">ISBN</td>
    <td>&nbsp;</td>
    <td>
      <input name="isbn" type="text" id="isbn" size="20" maxlength="20" value="<?php echo $isbn_sel; ?>"/>
            <input type="submit" name="b_dept" value="    Find    "/>
            <input type="hidden" name="lookup_action" value="find" />
    </td>
    </tr>
      </form>
    <tr>
    <td colspan="3">&nbsp;</td>
  </tr>
  	 <?php 
        if ($lookup_action == "find")
        {
	?>
      <tr>
    <td colspan="3" class="cell">Find Results for ISBN <?php echo $isbn_sel; ?></td>
  </tr>
      <tr>
    <td colspan="3">
    <table border="1">

    <tr>
    	<td class="cell">Dept</td>
        <td class="cell">Course</td>
        <td class="cell">Section</td>
        <td class="cell">Term</td>
        <td class="cell">&nbsp;</td>
     </tr>
     <?php 
	 	while( $lu = mysqli_fetch_array($lu_det, MYSQLI_ASSOC))
		{
			if (mysqli_num_rows($lu_det) > 0)
	 		{
	?>
     <tr>
     <td class="cell"><?php echo $lu['dept']; ?></td>
     <td class="cell"><?php echo $lu['course']; ?></td>
     <td class="cell"><?php echo $lu['section']; ?></td>
     <td class="cell"><?php echo $lu['term']; ?></td>
     <td class="cell">
     	<form name="link" method="post" action="idcs_link.php" id="link" >
     	<input type="submit" name="link" value="    Link    "/>
        <input type="hidden" name="itemid" value="<?php echo $lu['itemid']; ?>" />
        <input type="hidden" name="isbn" value="<?php echo $isbn_sel; ?>" />
        <input type="hidden" name="address" value="<?php echo $address_sel; ?>" />
        <input type="hidden" name="lookup_action" value="link" />
     	</form>
     </td>
     </tr>
     <?php
			}
			else
			{
		?>
     	<tr>
        <td colspan="5" class="cell">
        No Results Found for ISBN
        </td>
        </tr>
     <?php

     			}
     		}
	mysqli_free_result($lu_det);   
	mysqli_next_result($link);
	}
	?>
     
    </table>
    </td>
  </tr>
      <tr>
    <td colspan="3">&nbsp;</td>
  </tr>
      <tr>
    <td colspan="3"><font face="Arial, Helvetica, sans-serif"><a href="javascript:close_window();">Return to Standard Linking</a></font></td>
  </tr>
  
 
</table>

</body>
</html>
