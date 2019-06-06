USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: 
    ** NAME: USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 08/04/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** DATE:        AUTHOR:             DESCRIPTION:
	
    **************************************************/
SET NOCOUNT ON
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time

	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH'', CURRENT_TIMESTAMP, NULL,''I''');


DECLARE @Month VARCHAR(10)
DECLARE @PrevMonth VARCHAR(8)
DECLARE @NextMonth VARCHAR(8)
DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantDesc VARCHAR(400)
DECLARE @MerchantIndexNumber VARCHAR(10)
DECLARE @MerchantCreditSicCode VARCHAR(500)
DECLARE @MerchantDebitSicCode VARCHAR(500)
DECLARE @version VARCHAR(50)
DECLARE @onlineFlag VARCHAR(10)
DECLARE @CampaignStartDate VARCHAR(12)
DECLARE @startDate VARCHAR(12)
DECLARE @EndDate VARCHAR(12)
DECLARE @Campaign_Zip_Flag VARCHAR(20)
DECLARE @ReportStart VARCHAR(MAX)
DECLARE @ReportEnd VARCHAR(MAX)

SET @ReportStart = LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6) --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6) --Report End Date 1 Month Before Running Date CustList

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantCreditSicCode
	,MerchantDebitSicCode
	,CampaignStartDate
	,Campaign_Zip_Flag
FROM Dosh.dbo.AllDoshMerchantListNew
WHERE Is_Active = 1
	AND Competitor <> ''  and CampaignStartDate is not NULL  -- Removed and MerchantIndexNumber=92 Condition here will take care of All Merchant including Custom Merchants
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantCreditSicCode
	,MerchantDebitSicCode
	,CampaignStartDate
	,Campaign_Zip_Flag
FROM Dosh.dbo.AllDoshMerchantListNew
WHERE Is_Active = 1
AND Competitor <> ''  and CampaignStartDate is not NULL  -- Removed and MerchantIndexNumber=92 Condition here will take care of All Merchant including Custom Merchants
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @merchant
	,@OnlineFlag
	,@MerchantIndexNumber
	,@MerchantCreditSicCode
	,@MerchantDebitSicCode
	,@CampaignStartDate
	,@Campaign_Zip_Flag

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version = LEFT(DATENAME(MM, DATEADD(mm, - 1, @CustList)), 3) + RIGHT(LEFT(@CustList, 4), 2) + 'TPS'
	SET @startDate = CAST(@CampaignStartDate AS DATE)   --For Post Frequency StartDate will be Campaign Date as Provided by Yilun 
	SET @EndDate =  case when LEFT(CONVERT(VARCHAR(10), (DATEADD(MM, 6, @CampaignStartDate)), 112), 6)<=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)
					then Cast(DATEADD(MM, 6, @CampaignStartDate) as Date)
					else Cast(CONVERT(VARCHAR(10), dbo.udf_GetLastDayOfMonth((DATEADD(month, - 1, @CustList))), 112) as Date)   --Report End Date	
					end    --End Date will be 6 month from the Campaign Date or Max Transaction available whichever is lesser
	SET @Month = LEFT(CONVERT(VARCHAR(10), CONVERT(DATE, @startDate, 121), 112), 6) -- Get month from Start Date

	EXEC (
			'  --Create Table here 
		USE Dosh

		IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '''  --Table Name Earlier Dosh.dbo.PP_' + @Merchant + '_CampaignShoppers_FrequencySegment_' + @Version + '
			)

		DROP TABLE  Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
		
		create table Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '(
		pop varchar(30),
		segment varchar(30),
		transactiondate Date,
		ArgusPermid Int)
'
			)

	WHILE @Month <= LEFT(CONVERT(VARCHAR(10), convert(DATE, @EndDate, 121), 112), 6)
	BEGIN
		EXEC (
				'
USE Dosh

--With Transaction Credit and Debit

INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Credit'' AS pop,''Linked with Transactions'' as segment, CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE,A.ArgusPermId
FROM Dosh.dbo.Trans_Credit_Dosh_' + @Month + '_' + @version + ' a
INNER JOIN Dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On cast (A.ArgusPermId as varchar)= cast(p.ArguspermID as varchar)
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE OnlineFlag IN ' + @OnlineFlag + '
AND LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
AND MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' 
and CAST(a.transactiondate AS DATE)<=''' + @enddate + 
				'''
Group by CAST(a.transactiondate AS DATE) ,A.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,A.ArgusPermId


INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Debit''  AS pop,''Linked with Transactions'' as segment, CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE,a.ArgusPermId
FROM Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + ' a
INNER JOIN Dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On cast (A.ArgusPermId as varchar)= cast(p.ArguspermID as varchar)
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE OnlineFlag IN ' + @OnlineFlag + '
and LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
AND MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantDebitSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' and 
CAST(a.transactiondate AS DATE)<=''' + @enddate + 
				'''
Group by CAST(a.transactiondate AS DATE) ,a.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,a.ArgusPermId

		'
				)

		EXEC (
				'
USE Dosh

--Without Transaction Credit and Debit


INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Credit'' AS pop,''Linked Without Transactions'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE,A.ArgusPermId
FROM Dosh.dbo.Trans_Credit_Dosh_' + @Month + '_' + @version + ' a
INNER JOIN Dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On cast (A.ArgusPermId as varchar)= cast(p.ArguspermID as varchar)
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE OnlineFlag IN ' + @OnlineFlag + '
AND LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
AND MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' 
and CAST(a.transactiondate AS DATE)<=''' + @enddate + 
				'''
Group by CAST(a.transactiondate AS DATE) ,A.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,A.ArgusPermId


INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Debit'' AS pop,''Linked Without Transactions'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE,A.ArgusPermId
FROM Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + ' a
INNER JOIN Dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On cast (A.ArgusPermId as varchar)= cast(p.ArguspermID as varchar)
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE OnlineFlag IN ' + @OnlineFlag + '
AND LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
AND MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantDebitSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' 
and CAST(a.transactiondate AS DATE)<=''' + @enddate + 
				'''
Group by CAST(a.transactiondate AS DATE) ,A.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,A.ArgusPermId

'
				)

		EXEC (
				'
USE Dosh

--Non Dosh Credit and Debit
				
				
INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Credit'' AS pop,''NonDosh'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, A.ArgusPermId
FROM Dosh.dbo.Trans_Credit_Nondosh_' + @Month + '_' + @version + ' a
WHERE OnlineFlag IN ' + @OnlineFlag + '
AND MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' and 
CAST(a.transactiondate AS DATE)<=''' + @enddate + ''' 
Group by CAST(a.transactiondate AS DATE) ,A.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,A.ArgusPermId


INSERT INTO Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
SELECT ''Debit'' AS pop,''NonDosh'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, A.ArgusPermId
FROM Dosh.dbo.Trans_Debit_Nondosh_' + @Month + '_' + @version + ' a
WHERE OnlineFlag IN ' + @OnlineFlag + '
AND MerchantIndexNumber = ' + @MerchantIndexNumber + 
				' AND SicCode IN ' + @MerchantDebitSicCode + '
AND CAST(a.transactiondate AS DATE)>=  ''' + @startdate + ''' and 
CAST(a.transactiondate AS DATE)<=''' + @enddate + ''' 
Group by CAST(a.transactiondate AS DATE) ,A.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,A.ArgusPermId


'
				)

		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END
	END
	--Added on 24-05-2019 start
	EXEC ('
	ALTER TABLE Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE) 
	')
	--Added on 24-05-2019 end

	FETCH NEXT
	FROM MerchantCursor
	INTO @merchant
		,@OnlineFlag
		,@MerchantIndexNumber
		,@MerchantCreditSicCode
		,@MerchantDebitSicCode
		,@CampaignStartDate
		,@Campaign_Zip_Flag
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor

/*only have SIG transaction here as dosh does not get PIN transaction*/

--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_CAMPAIGN_SHOPPERS_FS_POST_T6MONTH''');
           
           END