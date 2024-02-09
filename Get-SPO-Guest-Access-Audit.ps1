#######################################################################################################################
# Get-SPO-Guest-Access-Audit
#######################################################################################################################

#setting unified log query parameters
$record = "SharePointFileOperation"
$operations = "FileAccessed,FileAccessedExtended,FileDownloaded,FileSyncDownloadedFull"
$resultSize = 1000
$intervalMinutes = 10
$errorSleep = 30
$stdSleep = 1
$daysBackOffset
$TenantName = "xyz.onmicrosoft.com"

#setting date variables
[DateTime]$Start = ((Get-Date -Hour 0 -Minute 0 -Second 0)).AddDays(-1 -$daysBackOffset)
[DateTime]$End = $Start.AddDays(1)

#setting guest suffix value
$guestUPNSuffix = "#ext#@"+$TenantName
$DoW = $Start.DayOfWeek

$deletedUsersODfBUPNList = @()
[int]$totalCount = 0
[int]$totalGuestsALL = 0
[int]$totalLabeledAll = 0
[int]$totalLabeledSNE = 0
[int]$totalAccessedDEL = 0
$currentStart = $start

#######################################################################################################################

Write-Host "Manual auth: $($ManualAuth)"
Write-Host "RecordType: $($record)"
Write-Host "Operations: $($operations)"
Write-Host "PageSize: $($resultSize) records"
Write-Host "Query interval: $($intervalMinutes) minutes"
Write-Host "Query start: $($start)"
Write-Host "Query end:   $($end)"
Write-Host "Output file ALL: $($OutputFileAll)"
Write-Host "Output file SNE: $($OutputFileSNE)"
Write-Host "Output file DEL: $($OutputFileDEL)"
Write-Host "Guest UPN suffix: $($guestUPNSuffix)"

#importing CSV with Teams data into hashtable, keyname is attribute FilesFolderUrl
#TBD
$O365TeamGroup_DB = Import-CSVtoHashDB -Path $DBFileTeamsChannelsOwners -Keyname "FilesFolderUrl"
#importing CSV with AAD guest data into hashtable, keyname is attribute mail
#TBD
$AADGuest_DB = Import-CSVtoHashDB -Path $DBFileGuests -KeyName "mail"

while ($true) {
    Write-Host
    $currentEnd = $currentStart.AddMinutes($intervalMinutes)
    if ($currentEnd -gt $end) {
        $currentEnd = $end
    }

    if ($currentStart -eq $currentEnd) {
        break
    }

    Write-Host "Retrieving activities between " -ForegroundColor Green -NoNewline
    Write-Host "$($currentStart) " -ForegroundColor Yellow -NoNewline
    Write-Host "and " -ForegroundColor Green -NoNewline
    Write-Host "$($currentEnd) " -ForegroundColor Yellow -NoNewline
    Write-Host "($($DoW))" -ForegroundColor Green
    #setting SessionID for Search-UnifiedAuditLog
    #$sessionID = "AuditLog_" + (Get-Date).ToString("yyyyMMddHHmmssfff")
    $sessionID = [Guid]::NewGuid().ToString()

    [int]$currentCount = 0
    [int]$pageCount = 0
    [int]$currentLabeledAll = 0
    [int]$currentLabeledSNE = 0
    [int]$currentAccessedDEL = 0

    [array]$PartialSPOAuditLogAll = $null
    [array]$PartialSPOAuditLogSNE = $null
    [array]$PartialSPOAuditLogDEL = $null
    
    do {
        #TBD - connecting to EXO PS module
        Connect-EXOService -AppRegName "CEZ_EXO_MBX_MGMT" -TTL 60
        $resCount = 0
        $indexError = $false
        $queryError = $false
        Write-Host "Search-UnifiedAuditLog (SessionID: " -ForegroundColor Gray -NoNewline
        Write-Host "$($sessionID)" -ForegroundColor White -NoNewline
        Write-Host ") Run time:$($Stopwatch.Elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Gray
        Try {
            $results = Search-UnifiedAuditLog -StartDate $currentStart -EndDate $currentEnd -RecordType $record -Operations $operations -SessionId $sessionID -SessionCommand ReturnLargeSet -ResultSize $resultSize -ErrorAction Stop -WarningAction Stop
            $resCount = $results.Count
            $pageCount++
        }
        Catch {
	        $msg = $_.Exception.Message
            Write-Host "Exception: $($msg)" -ForegroundColor White -BackgroundColor DarkBlue
            Write-Host $_.Exception -ForegroundColor White -BackgroundColor DarkBlue
            if (($msg.Contains("errors during the search process")) -or ($msg.Contains("underlying connection was closed")) -or ($msg.Contains("operation has timed out"))) {
                $queryError = $true 
            }
            else {
                Exit
            }
        }
        if ($resCount -ne 0) {
            #storing resultIndex and ResultCount properties of first and last members of $results array
            $indClr = "White"
            $cntClr = "White"
            $resIndFrst = $results[0].ResultIndex
            $resCntFrst = $results[0].ResultCount
            $resIndLast = $results[$resCount-1].ResultIndex
            $resCntLast = $results[$resCount-1].ResultCount
            if (($resIndFrst -eq -1) -or ($resIndLast -eq -1)) {
                $indexError = $true
                $indClr = "Red"
            }
            if ($indexError -and (($resCntFrst -eq 0) -or ($resCntLast -eq 0))) {
                $cntClr = "Red"
            }
            if (($pageCount -eq 1) -and (-not($indexError))) {
                Write-Host "Records:" -ForegroundColor Green -NoNewline
                Write-Host "$($resCntFrst) " -ForegroundColor Yellow -NoNewline
                Write-Host "Pages:" -ForegroundColor Green -NoNewline
                Write-Host "$([Math]::Ceiling($resCntFrst/$resultSize)) " -ForegroundColor Yellow
            }
           
            if (-not ($indexError -or $queryError)) {
                Write-Host "Page:" -ForegroundColor Gray -NoNewline
                Write-Host "$($pageCount)/$([Math]::Ceiling($resCntFrst/$resultSize)) " -ForegroundColor  Yellow -NoNewline
                Write-Host "Records:$($resCount) (First:" -ForegroundColor Gray -NoNewline
                Write-Host "$($resIndFrst)" -ForegroundColor $indClr  -NoNewline
                Write-Host "/" -ForegroundColor Gray -NoNewline
                Write-Host "$($resCntFrst)" -ForegroundColor $cntClr -NoNewline
                Write-Host ") (Last:" -ForegroundColor Gray -NoNewline
                Write-Host "$($resIndLast)" -ForegroundColor $indClr -NoNewline
                Write-Host "/" -ForegroundColor Gray -NoNewline
                Write-Host "$($resCntLast)" -ForegroundColor $cntClr -NoNewline
                Write-Host ")" -ForegroundColor Gray

                foreach ($result in $results) {
                    #searching for activities by B2B guest accounts - UPN ends with #ext#@cezdata.onmicrosoft.com
                    #we are not interested in anything else
                    $SiteURL = $null
                    $auditData = ConvertFrom-Json -InputObject $result.AuditData
                    if ($auditData.SiteUrl) {
                        $SiteURL = ($auditData.SiteUrl).Trim().ToLower()
                        if ($SiteURL.EndsWith("/")) {
                            $SiteURL = $SiteURL.Trim("/")
                        }
                    }
                  
                    if ($result.UserIds.EndsWith($guestUPNSuffix)) {
                        $totalGuestsALL++
                        if ($auditData.SensitivityLabelId) {
                            $sensitivityLabelName = $AIPLabelDB.Item($auditData.SensitivityLabelId)
                        }
                        if ($SiteUrl) {
                            $currentTeam = $O365TeamGroup_DB.Item($SiteURL)
                            if ($currentTeam) {
                                switch ($currentTeam.Level) {
                                    "Team"      {$TeamName = $currentTeam.Level + ":" + $currentTeam.teamName}
                                    "Channel"   {$TeamName = $currentTeam.Level + ":" + $currentTeam.channelName}
                                    Default     {$TeamName = ""}
                                }
                                $SiteOwners = $currentTeam.Owners
                                $TeamId     = $currentTeam.TeamId
                            }
                        }
                        
                        $mail = Get-MailFromGuestUPN $auditData.UserId
                        if ($AADGuest_DB.ContainsKey($mail)) {
                            [pscustomobject]$guestData = $AADGuest_DB[$mail]
                        }
                        
                        $auditObject = [pscustomobject]@{
                            CreationTime            = $auditData.CreationTime;
                            CorrelationId           = $auditData.CorrelationId;
                            Id                      = $auditData.Id;
                            EventSource             = $auditData.EventSource;
                            Workload                = $auditData.Workload;
                            OperationType           = $auditData.Operation;
                            UserId                  = $auditData.UserId;
                            Mail                    = $mail;
                            UserType                = Get-AuditUserTypeFromCode $auditData.UserType;
                            guestId                 = $guestData.userId;
                            displayName             = $guestData.displayName;
                            createdDateTime         = $guestData.createdDateTime;
                            createdBy               = $guestData.createdBy;
                            mailDomain              = $guestData.MailDomain;
                            extAADTenantId		    = $guestData.ExtAADTenantId;
                            extAADDisplayName	    = $guestData.ExtAADDisplayName;
                            extAADdefaultDomain	    = $guestData.ExtAADdefaultDomain
                            ClientIP                = $auditData.ClientIP;
                            RecordType              = Get-AuditRecordTypeFromCode $auditData.RecordType;
                            Version                 = $auditData.Version;
                            IsManagedDevice         = $auditData.IsManagedDevice;
                            ItemType                = $auditData.ItemType;
                            SensitivityLabelId      = $auditData.SensitivityLabelId;
                            SensitivityLabelName    = $sensitivityLabelName;
                            SensitivityLabelOwner   = $auditData.SensitivityLabelOwnerEmail;
                            SourceFileExtension     = $auditData.SourceFileExtension;
                            SiteURL                 = $SiteURL;
                            TeamName                = $TeamName;
                            SPOSiteName             = $SiteName;
                            TeamId                  = $TeamId;
                            Owners                  = $SiteOwners;
                            SourceFileName          = $auditData.SourceFileName;
                            SourceRelativeUrl       = $auditData.SourceRelativeUrl;
                            ObjectId                = $auditData.ObjectId;
                            ListId                  = $auditData.ListId;
                            ListItemUniqueId        = $auditData.ListItemUniqueId;
                            WebId                   = $auditData.WebId;
                            ListBaseType            = $auditData.ListBaseType;
                            ListServerTemplate      = $auditData.ListServerTemplate;
                            SiteUserAgent           = $auditData.SiteUserAgent;
                            HighPrioMediaProc       = $auditData.HighPriorityMediaProcessing
                        }
                    }
                }#foreach ($result in $results)
                
                $currentTotal = $resCntFrst
                $totalCount += $resCount
                $currentCount += $resCount
                
                #end reached successfully
                if ($currentTotal -eq $resIndLast) {
                    #exporting/appending B2B guest all audit events to output CSV file
                    Write-Host "Total processed records:" -ForegroundColor Gray -NoNewline
                    Write-Host "$($currentTotal) " -ForegroundColor Yellow -NoNewline
                    Write-Host "Pages:" -ForegroundColor Gray -NoNewline
                    Write-Host "$($pageCount) " -ForegroundColor Yellow -NoNewline
                    Write-Host "Records/page:$($resultSize)" -ForegroundColor Gray
                    if ($PartialSPOAuditLogAll.Count -gt 0) {
                        Write-Host "Guest records:" -ForegroundColor Gray -NoNewline
                        Write-Host "$($PartialSPOAuditLogAll.Count) " -ForegroundColor Yellow -NoNewline
                        Write-Host "(labeled:" -ForegroundColor Gray -NoNewline
                        Write-Host "$($currentLabeledAll)" -ForegroundColor Yellow -NoNewline
                        Write-Host ") SNE:" -ForegroundColor Gray -NoNewline
                        Write-Host "$($currentLabeledSNE)" -ForegroundColor Red
                        $PartialSPOAuditLogAll | Export-Csv -Path $OutputFileAll -Append -NoTypeInformation
                        if ($PartialSPOAuditLogSNE.Count -gt 0) {
                            $PartialSPOAuditLogSNE | Export-Csv -Path $OutputFileSNE -Append -NoTypeInformation
                        }
                    }
                    else {
                        Write-Host "No guests records to export." -ForegroundColor Gray
                    }
                    if ($PartialSPOAuditLogDEL.Count -gt 0) {
                        Write-Host "User records: deleted ODfB sites access:" -ForegroundColor Gray -NoNewline
                        Write-Host "$($currentAccessedDEL)" -ForegroundColor Red
                        $PartialSPOAuditLogDEL | Export-Csv -Path $OutputFileDEL -Append -NoTypeInformation
                    }
                    else {
                        Write-Host "No user records to export." -ForegroundColor Gray
                    }
                    Start-Sleep -s $stdSleep
                    break
                }
            
            }#if (-not ($indexError -or $queryError))
            else {
                if ($indexError) {
                    Write-Host "INDEX ERROR OCCURED" -ForegroundColor Red
                    Write-host "$($resIndFrst)/$($resCntFrst) $($resIndLast)/$($resCntLast)" -ForegroundColor Red
                    Start-SleepDots "Waiting $($errorSleep) seconds and then retrying" -Seconds $errorSleep -ForegroundColor "Red"
                    break
                }
                if ($queryError) {
                    Write-Host "QUERY ERROR OCCURED" -ForegroundColor Red
                    Start-SleepDots "Waiting $($errorSleep) seconds and then retrying" -Seconds $errorSleep -ForegroundColor "Red"
                    Connect-EXOService -AppRegName "CEZ_EXO_MBX_MGMT" -TTL 60 -ForceReconnect
                    break    
                }
            }
        }#if ($resCount -ne 0)
    }#do (inner loop)
    while ($resCount -ne 0)

    if (-not ($indexError -or $queryError)) {
        $currentStart = $currentEnd
    }
}#while ($true)

Write-Host "totalGuestsALL: $($totalGuestsALL)"
Write-Host "totalLabeledAll: $($totalLabeledAll)"
Write-Host "totalLabeledSNE: $($totalLabeledSNE)"
Write-Host "totalAccessedDEL: $($totalAccessedDEL)"
Write-Host "END. Total count: " -NoNewline
Write-Host "$($totalCount)" -ForegroundColor Yellow

. $ScriptPath\include-Script-EndLog-generic.ps1