USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_DR_METRIC_CUSTOM_FS] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 4. Gather Debit Metrics Competitor
    ** NAME: USP_DR_METRIC_CUSTOM_FS
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 13/03/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** DATE:        AUTHOR:             DESCRIPTION:
	
    **************************************************/
SET NOCOUNT ON

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_DR_METRIC_CUSTOM_FS'
				
				IF @lv_Status<>'C' 
					Begin 	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_DR_METRIC_CUSTOM_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_DR_METRIC_CUSTOM_FS'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR(MAX)
DECLARE @NextMonth VARCHAR(8)
DECLARE @Merchant VARCHAR(50)
DECLARE @Competitor VARCHAR(50)
DECLARE @MerchantDesc VARCHAR(400)
DECLARE @CompetitorIndexNumber VARCHAR(10)
DECLARE @version VARCHAR(50)
DECLARE @onlineFlag VARCHAR(10)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList


SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

--Joseph R K changes on 23/04/2019 start
--here the distinct may not be required but kept as a safety net
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Competitor
	,Merchant
	,OnlineFlag
	,CompetitorIndexNumber
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL and CompetitorDebitSicCode=''   --WHERE CompetitorIndexNumber IN (208) Removed to Include all Competitor
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Competitor
	,Merchant
	,OnlineFlag
	,CompetitorIndexNumber
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL and CompetitorDebitSicCode=''   --WHERE CompetitorIndexNumber IN (208) Removed to Include all Competitor
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @competitor
	,@merchant
	,@OnlineFlag
	,@CompetitorIndexNumber


WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC  (
			'
/*Step 0 Build a shell*/
use dosh

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '''
	)


DROP TABLE Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '


create table Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '(
merchant  Varchar(25),
TransactionDate date,
pop  Varchar(50),
Campaign_Zip_Flag VARCHAR(20),
PreFrequency VARCHAR(10),
REDEEMFLAG Varchar(50),
TotalCustomers Int,
[Dosh Debit SIG Spend]  money,
[Dosh Debit PIN Spend]  money,
[Dosh Debit SIG Transactions] money,
[Dosh Debit PIN Transactions] money,
[Dosh Debit SIG Shoppers] money,
[Dosh Debit PIN Shoppers] money)
'
			)

	SET @Month = @ReportStart 
	WHILE @Month <= @ReportEnd  --it will be same 12 Month logic asked Prachi and Confirmed 
	BEGIN
		EXEC  (
				'
-- Linked Without Transactions
INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '
SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag,z.Prefrequency,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM (
		SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + ') a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'')  and OnlineFlag IN ' + 
				@OnlineFlag + '
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency  ) z
 left join(
		SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag,Prefrequency,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
		 SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit SIG Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + 
				' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
		and MerchantLookupType = ''SIG'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'') 
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) a
 ON z.TransactionDate = a.TransactionDate and z.Prefrequency=a.Prefrequency and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate,''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency ,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
		SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit PIN Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + 
				'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
		and MerchantLookupType = ''pin'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked Without Transactions'')
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) b
  ON z.TransactionDate = b.TransactionDate and z.Prefrequency=b.Prefrequency and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
  --order by 3,2,5
 
 '
				)

		EXEC  (
				'
 --Linked with Transactions
INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '
SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag,z.Prefrequency,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM (
		SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'')  and OnlineFlag IN ' + @OnlineFlag 
				+ '
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency  ) z
 left join(
		SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop,Campaign_Zip_Flag,Prefrequency,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
		 SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit SIG Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + 
				' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
		and MerchantLookupType = ''SIG'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'') 
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) a
 ON z.TransactionDate = a.TransactionDate and z.Prefrequency=a.Prefrequency and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate,''Linked With Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency ,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
		SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit PIN Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + 
				'_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
		On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d 
		On c.dosh_customer_key = d.dosh_customer_key
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
		and MerchantLookupType = ''pin'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''Linked With Transactions'')
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) b
  ON z.TransactionDate = b.TransactionDate and z.Prefrequency=b.Prefrequency and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
  --order by 3,2,5

  
  '
				)

		EXEC  (
				'
--NonDosh

 INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + '
SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,z.Campaign_Zip_Flag, z.Prefrequency,''0'' AS REDEEMFLAG,''0'' AS TotalCustomers,
[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
 FROM(
		SELECT a.TransactionDate, ''NonDosh'' AS Pop,Campaign_Zip_Flag, Prefrequency
		FROM ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where Onlineflag IN ' + @OnlineFlag + 
				' and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'')  
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) z
 left join(
		SELECT a.TransactionDate, ''NonDosh'' AS Pop,Campaign_Zip_Flag, Prefrequency,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
		 SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit SIG Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
		FROM  ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + 
				' and MerchantLookupType = ''SIG'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'') 
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) a
 ON z.TransactionDate = a.TransactionDate and z.Prefrequency=a.Prefrequency and z.pop = a.pop AND z.Campaign_Zip_Flag = a.Campaign_Zip_Flag
 LEFT JOIN (
		SELECT a.TransactionDate,''NonDosh'' AS Pop,Campaign_Zip_Flag, Prefrequency ,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
		SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit PIN Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
		FROM  ( SELECT * 
			   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
			   )a
		inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
		ON a.AccountID = b.DepositAccountID
		AND a.bankId = b.BankID
		inner join Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' E
		ON B.ARGUSPERMID=E.ARGUSPERMID
		where OnlineFlag IN ' + @OnlineFlag + 
				' and  MerchantLookupType = ''pin'' 
		and a.BankID in (400,410,411,421,424,429,439,440,479,489)   and e.pop in (''NonDosh'')
		group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency ) b
  ON z.TransactionDate = b.TransactionDate and  z.Prefrequency=b.Prefrequency and z.pop =b.pop AND z.Campaign_Zip_Flag = b.Campaign_Zip_Flag
  --order by 3,2,5
 
 

'
				)



		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END
	END
	
	EXEC ('
	CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + ' (pop,transactiondate)
	ALTER TABLE Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_FrequencySegment_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	')

	FETCH NEXT
	FROM MerchantCursor
	INTO @competitor
		,@merchant
		,@OnlineFlag
		,@CompetitorIndexNumber
	
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_DR_METRIC_CUSTOM_FS''');
END