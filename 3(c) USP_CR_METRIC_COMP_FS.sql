USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[USP_CR_METRIC_COMP_FS] @CustList VARCHAR(50)

AS
/*******************************************
    ** FILE: Gather Credit Metrics Competitor Frequency Segment
    ** NAME: USP_CR_METRIC_COMP_FS
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 19/03/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** DATE:        AUTHOR:             DESCRIPTION:
	
    **************************************************/
SET NOCOUNT ON

Declare @lv_Status varchar(2);
SET @lv_Status = ''
				Select @lv_Status=Status from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_CR_METRIC_COMP_FS'
				
				IF @lv_Status<>'C'
					Begin

--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CR_METRIC_COMP_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_CR_METRIC_COMP_FS'', CURRENT_TIMESTAMP, NULL,''I''');

/*this will include HawPRD 201708-201801,201805-201808 Dosh Gas Prospects*/
DECLARE @Month VARCHAR (MAX)
DECLARE @NextMonth VARCHAR (8)
Declare @Merchant      VarChar(50)
Declare @Competitor      VarChar(50)
Declare @MerchantDesc  VarChar(400)
Declare @CompetitorIndexNumber  VARCHAR(10)
Declare @version VARCHAR(50)
Declare @onlineFlag VARCHAR (10)
DEclare @SicCode VARCHAR (500)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList


SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

--Joseph R K changes on 23/04/2019 start
--here the distinct may not be required but kept as a safety net
/*
DECLARE MerchantCursor CURSOR FOR
	SELECT Competitor, Merchant,OnlineFlag, CompetitorIndexNumber,CompetitorCreditSicCode AS SicCode 
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and CompetitorCreditSicCode<>''   --Only For Standard Merchants
*/
DECLARE MerchantCursor CURSOR FOR
SELECT distinct Competitor, Merchant,OnlineFlag, CompetitorIndexNumber,CompetitorCreditSicCode AS SicCode 
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and CompetitorCreditSicCode<>'' and CampaignStartDate is not NULL     --Only For Standard Merchants
--Joseph R K changes on 23/04/2019 end	
	
OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @competitor,@merchant ,@OnlineFlag , @CompetitorIndexNumber, @SicCode

WHILE @@FETCH_STATUS=0
begin
EXEC('
/*Step 0 Build a shell*/
use dosh

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'''
	)

DROP TABLE Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'


create table Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'(
merchant  Varchar(25),
TransactionDate date,
pop  Varchar(50),
Campaign_Zip_Flag VARCHAR(20),
PreFrequency VARCHAR(10),
REDEEMFLAG Varchar(50),
TotalCustomers Int,
[Dosh Credit Spend]  money,
[Dosh Credit Transactions] money,
[Dosh Credit Shoppers] money)
')


	SET @Month = @ReportStart 
	WHILE @Month <= @ReportEnd 

BEGIN

EXEC('
--1) Linked Without Transactions
INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'
SELECT '''+@Competitor+''' as merchant,a.TransactionDate,''Linked Without Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency, ''0'' AS REDEEMFLAG,  '''' as TotalCustomers,
SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
FROM (
	select accountid as accountid,Onlineflag, bankid,CAST(transactiondate AS DATE) AS TRANSACTIONDATE,PositiveSpend,NegativeSpend,PositiveTrans,NegativeTrans 
	FROM Dosh.dbo.Trans_Credit_Dosh_Industry_'+@Month+'_'+@Version+'    WITH(NoLock)
	WHERE SicCode IN '+@SicCode+'
	)a
	INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped b with (nolock)
	ON a.AccountID = b.AccountId
	AND a.bankId = b.BankID
	INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
	On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
	INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d
	On c.dosh_customer_key = d.dosh_customer_key
	inner join Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+' E
	ON  cast(b.arguspermid as varchar)=e.arguspermid
	where OnlineFlag IN '+@OnlineFlag+' and LinkedTag = ''Linked'' and TransTag = ''WithoutTrans''   and e.pop in (''Linked Without Transactions'') 
	and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240)  
	group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency 
	--order by 3,2,5
	

--1) Linked With Transactions
INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'
SELECT '''+@Competitor+''' as merchant,a.TransactionDate,''Linked With Transactions'' AS Pop,Campaign_Zip_Flag, Prefrequency, ''0'' AS REDEEMFLAG,  '''' as TotalCustomers,
SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
FROM (
	select accountid as accountid,Onlineflag, bankid,CAST(transactiondate AS DATE) AS TRANSACTIONDATE,PositiveSpend,NegativeSpend,PositiveTrans,NegativeTrans 
	FROM Dosh.dbo.Trans_Credit_Dosh_Industry_'+@Month+'_'+@Version+'    WITH(NoLock)
		WHERE SicCode IN '+@SicCode+'
	)a
	INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped b with (nolock)
	ON a.AccountID = b.AccountId
	AND a.bankId = b.BankID
	INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  c
	On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
	INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' d
	On c.dosh_customer_key = d.dosh_customer_key
	inner join Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+' E
	ON  cast(b.arguspermid as varchar)=e.arguspermid
	where OnlineFlag IN '+@OnlineFlag+' and LinkedTag = ''Linked'' and TransTag = ''WithTrans''   and e.pop in (''Linked With Transactions'') 
	and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240)  
	group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency 
	--order by 3,2,5
	

--3) NonDosh
INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+'
SELECT '''+@Competitor+''' as merchant,a.TransactionDate,''NoNDosh'' AS Pop,Campaign_Zip_Flag, Prefrequency, ''0'' AS REDEEMFLAG,  '''' as TotalCustomers,
SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
FROM (
	select accountid as accountid,Onlineflag, bankid,CAST(transactiondate AS DATE) AS TRANSACTIONDATE,PositiveSpend,NegativeSpend,PositiveTrans,NegativeTrans 
	FROM Dosh.dbo.Trans_Credit_NonDosh_Industry_'+@Month+'_'+@Version+'    WITH(NoLock)
		WHERE SicCode IN '+@SicCode+'
	)a
	INNER JOIN Dosh.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped b with (nolock)
	ON a.AccountID = b.AccountId
	AND a.bankId = b.BankID
	inner join Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+' E
	ON  cast(b.arguspermid as varchar)=e.arguspermid
	where OnlineFlag IN '+@OnlineFlag+'  and e.pop in (''NonDosh'') 
	and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240)  
	group by a.TransactionDate,Campaign_Zip_Flag, Prefrequency 
	--order by 3,2,5
	



')
SET @Month = 
case 
     when Right(@Month, 2) = 12 then @Month + 89 
	 else @Month + 1 END

End

EXEC('
CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+' (pop,transactiondate)
ALTER TABLE Dosh.dbo.Credit_'+@Merchant+'_'+@competitor+'_Aggregate_FrequencySegment_'+@version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')

FETCH NEXT FROM MerchantCursor INTO @competitor,@merchant ,@OnlineFlag , @CompetitorIndexNumber, @SicCode
    
END 
CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_CR_METRIC_COMP_FS''');
END 