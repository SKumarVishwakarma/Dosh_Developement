USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_DR_METRIC_GATHER]    Script Date: 03/08/2019 16:27:32 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[USP_DR_METRIC_GATHER] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 3. Gather Debit Metrics Merchant
    ** NAME: USP_DR_METRIC_GATHER
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 23/02/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** DATE:        AUTHOR:             DESCRIPTION:
	22/02/2019      SHASHI              @MONTH Can Be Taken from @CustList if Incorporated
	
	22/02/2019		SHASHI				All the Parameters for Now Handled in the Procedure keeping CustList as HardCoded can be made dynamic
    **************************************************/
SET NOCOUNT ON

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_DR_METRIC_GATHER'
				
				IF @lv_Status<>'C' 
					Begin	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_DR_METRIC_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_DR_METRIC_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR (MAX)
DECLARE @NextMonth VARCHAR (8)
Declare @Merchant      VarChar(50)
Declare @MerchantDesc  VarChar(400)
Declare @MerchantIndexNumber  VARCHAR(10)
Declare @version VARCHAR(50)
Declare @onlineFlag VARCHAR (10)
DEclare @SicCode VARCHAR (500)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList


SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR FOR
SELECT Merchant,OnlineFlag, MerchantIndexNumber,MerchantDebitSicCode AS SicCode 
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1  and Competitor<>'' and CampaignStartDate is not NULL  -- Include all Merchant 
*/
DECLARE MerchantCursor CURSOR FOR
SELECT distinct Merchant,OnlineFlag, MerchantIndexNumber,MerchantDebitSicCode AS SicCode 
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1  and Competitor<>'' and CampaignStartDate is not NULL  -- Include all Merchant 
--Joseph R K changes on 23/04/2019 end
	
OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @SicCode

WHILE @@FETCH_STATUS=0
begin

EXEC ('
/*Step 0 Build a shell*/
use dosh

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'''
	)

DROP TABLE Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'


create table Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'(
merchant  Varchar(25),
TransactionDate date,
pop  Varchar(50),
Campaign_Zip_Flag VARCHAR(20),
SEGMENTFLAG Varchar(50),
REDEEMFLAG Varchar(50),
TotalCustomers Int,
[Dosh Debit SIG Spend]  money,
[Dosh Debit PIN Spend]  money,
[Dosh Debit SIG Transactions] money,
[Dosh Debit PIN Transactions] money,
[Dosh Debit SIG Shoppers] money,
[Dosh Debit PIN Shoppers] money)
')

SET @Month = @ReportStart 
WHILE @Month <= @ReportEnd

BEGIN


EXEC ('
-- Linked Without Transactions
INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'
SELECT '''+@Merchant+''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag,z.SEGMENTFLAG,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM (
		SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop , Campaign_Zip_Flag , SegmentFlag
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+' a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		AND a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'')  
		group by a.TransactionDate, SEGMENTFLAG,Campaign_Zip_Flag  ) z
 left join(
		SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit SIG Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit SIG Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''SIG'') a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'') 
		group by a.TransactionDate, SEGMENTFLAG ,Campaign_Zip_Flag) a
 ON z.TransactionDate = a.TransactionDate and z.SEGMENTFLAG=a.segmentflag and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit PIN Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit PIN Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''PIN'') a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'') 
		group by a.TransactionDate, SEGMENTFLAG, Campaign_Zip_Flag ) b
  ON z.TransactionDate = b.TransactionDate and z.SEGMENTFLAG=b.segmentflag and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
 
 ')

 EXEC ('
 --Linked with Transactions
INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'
SELECT '''+@Merchant+''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag,z.SEGMENTFLAG,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM (
		SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop , Campaign_Zip_Flag , SegmentFlag
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+' a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		AND a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'')  
		group by a.TransactionDate, SEGMENTFLAG,Campaign_Zip_Flag  ) z
 left join(
		SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit SIG Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit SIG Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''SIG'') a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'') 
		group by a.TransactionDate, SEGMENTFLAG ,Campaign_Zip_Flag) a
 ON z.TransactionDate = a.TransactionDate and z.SEGMENTFLAG=a.segmentflag and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit PIN Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit PIN Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_Dosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''PIN'') a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'') 
		group by a.TransactionDate, SEGMENTFLAG, Campaign_Zip_Flag ) b
ON z.TransactionDate = b.TransactionDate and z.SEGMENTFLAG=b.segmentflag and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
  
  ')
  EXEC ('
--NonDosh

INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+'
SELECT '''+@Merchant+''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag,z.SEGMENTFLAG,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM (
		SELECT a.TransactionDate, ''NonDosh'' AS Pop , Campaign_Zip_Flag , SegmentFlag
		FROM Dosh.dbo.Trans_Debit_NonDosh_'+@MONTH+'_'+@Version+' a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		AND a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'')  
		group by a.TransactionDate, SEGMENTFLAG,Campaign_Zip_Flag  ) z
 left join(
		SELECT a.TransactionDate, ''NonDosh'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit SIG Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit SIG Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_NonDosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''SIG'') a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'') 
		group by a.TransactionDate,Campaign_Zip_Flag, SEGMENTFLAG ) a
 ON z.TransactionDate = a.TransactionDate and z.SEGMENTFLAG=a.segmentflag and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate, ''NonDosh'' AS Pop,Campaign_Zip_Flag,SEGMENTFLAG,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS  [Dosh Debit PIN Spend],
		SUM(PositiveTrans+NegativeTrans) AS [Dosh Debit PIN Transactions], COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM (SELECT TransactionDate, AccountID, BankID , PositiveSpend , NegativeSpend, PositiveTrans , NegativeTrans
		FROM Dosh.dbo.Trans_Debit_NonDosh_'+@MONTH+'_'+@Version+'
		where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' AND OnlineFlag IN '+@OnlineFlag+'
		and MerchantLookupType = ''PIN'') a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'') 
		group by a.TransactionDate, SEGMENTFLAG ,Campaign_Zip_Flag) b
  ON z.TransactionDate = b.TransactionDate and z.SEGMENTFLAG=b.segmentflag and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
 

')

SET @Month = 
case 
     when Right(@Month, 2) = 12 then @Month + 89 
	 else @Month + 1 END

End



EXEC ('
CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_' + @version + ' (pop,transactiondate)
ALTER TABLE Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
	

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @SicCode
    
END 
CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_DR_METRIC_GATHER''');
END           
