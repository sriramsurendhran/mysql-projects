function Execute-SqlQuery($stmt){
	$destnInsertCmd.CommandText = "";
	$destnInsertCmd.CommandText = $stmt;
    $destnInsertCmd.ExecuteNonQuery() | out-null
}
$procStartDate = Get-Date


[string]$sSourceMySQLUserName = 'root'
[string]$sSourceMySQLPW = 'mysql'
[string]$sSourceMySQLDB = 'destination'
[string]$sSourceMySQLHost = 'localhost'

[string]$sDestMySQLUserName = 'root'
[string]$sDestMySQLPW = 'mysql'
[string]$sDestMySQLDB = 'destination'
[string]$sDestMySQLHost = 'localhost'

$sSourceQuery  = "SELECT emp_no,birth_date,first_name,last_name,gender,hire_date from employees"
$sInsertHeadTemplate = "INSERT INTO employees_test(emp_no,birth_date,first_name,last_name,gender,hire_date)"
$sInsertStatement = @'
('{0}','{1}','{2}','{3}','{4}','{5}')
'@
$counter = 0;
$totalCounter = 0;
$batchSize = 10000;

$oSourceConnection = New-Object System.Data.Odbc.OdbcConnection
$oSourceConnection.ConnectionString = "DSN=" + $sSourceMySQLDB + ";UID=" + $sSourceMySQLUserName + ";Pwd=" + $sSourceMySQLPW + ""


$oDestConnection = New-Object System.Data.Odbc.OdbcConnection
$oDestConnection.ConnectionString = "DSN=" + $sDestMySQLDB + ";UID=" + $sDestMySQLUserName + ";Pwd=" + $sDestMySQLPW + ""


$Error.Clear()
try
{
    $oSourceConnection.Open()
}
catch
{
    write-warning ("Could not open a connection to Database $sSourceMySQLDB on Host $sSourceMySQLHost. Error: "+$Error[0].ToString())
}

$Error.Clear()
try
{
    $oDestConnection.Open()
}
catch
{
    write-warning ("Could not open a connection to Database $sDestMySQLDB on Host $sDestMySQLHost. Error: "+$Error[0].ToString())
}


$destnInsertCmd = $oDestConnection.CreateCommand()

$sSourceQueryStmt = New-Object System.Data.Odbc.OdbcCommand($sSourceQuery,$oSourceConnection)
$sSourceQueryResult = New-Object System.Data.DataSet
Write-Host "Fetch from Source Database - Begin"
$tempStartTime = Get-Date
(New-Object System.Data.Odbc.OdbcDataAdapter($sSourceQueryStmt)).Fill($sSourceQueryResult) | out-null
$tempEndTime   = Get-Date
Write-Host "Fetch Process completed in " + ($tempEndTime - $tempStartTime)

Write-Host "Fetch from Source Database - Completed"

Write-Host "Insert into Destination Database - Begin"
$tempStartTime = Get-Date
$stringBuilderQuery = [System.Text.StringBuilder]::new() 
foreach($row in $sSourceQueryResult.Tables[0])
{
    #$sInsertData = ($sInsertStatement -f $row.Item("emp_no"), $row.Item("birth_date"),$row.Item("first_name"),$row.Item("last_name"),$row.Item("gender"),$row.Item("hire_date"))
	$sInsertData = ($sInsertStatement -f $row.Item("emp_no"),"1986-06-26",$row.Item("first_name"),$row.Item("last_name"),$row.Item("gender"),"1986-06-26")
	if($counter -eq 0) {
	   [void]$stringBuilderQuery.AppendLine($sInsertHeadTemplate)
       [void]$stringBuilderQuery.AppendLine("VALUES ")
	   $sValuesString = $sInsertData
	}
	else
	{
	   $sValuesString = "," + $sInsertData
	}
	
    
	[void]$stringBuilderQuery.AppendLine($sValuesString)
	if($counter -eq $batchSize){
		$sBulkPrepareStmt = $stringBuilderQuery.toString()
        Execute-SqlQuery $sBulkPrepareStmt;
		$totalCounter += $counter
		Write-Host "Insert Row(s) Completed - $totalCounter"
		$counter = 0;
        $sBulkPrepareStmt = "";
		[void]$stringBuilderQuery.Clear()
    }
	else {
	    $counter += 1
	}
     
}

#Last batch if any

$sBulkPrepareStmt=$stringBuilderQuery.toString()
if ($sBulkPrepareStmt.Trim() -ne "") { 
    Execute-SqlQuery $sBulkPrepareStmt
	$totalCounter += $counter
	Write-Host "Insert Row(s) Completed - $totalCounter"
	$counter = 0;
	$sBulkPrepareStmt = ""
	[void]$stringBuilderQuery.Clear()
}
$tempEndTime   = Get-Date
Write-Host "Insert Process completed in " + ($tempEndTime - $tempStartTime)

$procEndtDate = Get-Date
Write-Host "Process completed in " + ($procEndtDate - $procStartDate)

$oSourceConnection.Close()
$oDestConnection.Close()
