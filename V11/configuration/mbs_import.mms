<?xml version="1.0" encoding="utf-8"?>

<MMImport Drive="c" >
	<Logging
		Level="Info"
		Path="pricer\R3Server\logs\"
		Log_Name="data_import.log"
		Delete_After="60"
		Log_Max_MB="100"
	/>
	<DataLocal
		UsePricer="True"
		LocalDB="MspWij8kda+j3KywuhGEryWjm+YRi4FCeB+wewq2qfRlLM16cFVnl94wSMGkV8bNCxf+jJWKzI8b5YdPqEkBsf+rSmM2LUCUptXayTGbqIt9va2t0y3GdA=="
	/>
	<DataIn
		DataInputSave="False"
		DeleteAfter="7"
		Path="Pricer\Import\data\"
		InputHoldPath="Pricer\Import\data\input\"
		FileWaitTime="60"
	/>
    <Sort
		Order="Natural"
    />
    <Price
		None_100="False"
    />
    <Usage
		Convert="True"
    />
    <Zero_Zero
		IPF100="True"
    />
    <Term
		Multi="True"
    />
    <Date
		Format="MM/DD/YYYY"
    />
    <IPF1x5
		Threshold="0"
    />
    <Replace_Section_Commas
		Char="-"
		DisplayOriginal="True"
    />
    <QOH_Calc
		New_On_Hand="Add"
		Used_On_Hand="Add"
		New_Addl="Add"
		Used_Addl="Add"
		New_Pend_Ret="Add"
		Used_Pend_Ret="Add"
		New_insite_Pend_Ord="Add"
		Used_insite_Pend_Ord="Add"
		New_Rental_insite_Pend_Ord="Add"
		Used_Rental_insite_Pend_Ord="Add"
    />
	<DataOut
		UsePFI="False"
        UseAPI="True"
        UseSoap="True"
        i1_Path="pricer\PFIFiles\DataFiles\"
        m1_Path="pricer\PFIFiles\MessageFiles\"
        r7_Path="pricer\PFIFiles\ResultFiles\"
        APIPageCount="1000"
        SoapIP="g6GxX2vmX/5x3K3LD+YaqiahnhZzCrVn"
        SoapToken="QRjSFVbX3JzRQ/CaeWU+4rhKW6+PuR5GKZqNlAu/hkA="
        SoapUser="E4AddKo1/Ygpmo2UC7+GQA=="
        RestURL="naSYAoFcT7AzN7aN2BIPjoooIrLE0Lq6N66ZnCIUvrtkoqCvKw05JrBNdiga36Qy9IT2OcXRvTM="
        RestToken="z9JQLq1XLX/c+6T44q8ptdzjqaPyAeEhDeMXwYvgP4U="
	/>
    <Alerts
		WELLogging="True"
		WELLogName="Pricer"
		Email="False"
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
	/>
</MMImport>