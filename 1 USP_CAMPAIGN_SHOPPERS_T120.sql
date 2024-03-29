USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_CAMPAIGN_SHOPPERS_T120]    Script Date: 03/08/2019 16:25:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_CAMPAIGN_SHOPPERS_T120] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 1. Cursor to gather the Campaign Shoppers for T120 days
    ** NAME: USP_CAMPAIGN_SHOPPERS_T120
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
	22/02/2019		PRACHI				Changed the PP_industrySegment to IndustrySegment
	22/02/2019		PRACHI				I have added the INTO statement where the naming convention is as per the final deliverable of CSV file.
										Final table :Dosh.dbo.'+@Merchant+'_Industry_'+@CustList+'
	22/02/2019		SHASHI				All the Parameters for Now Handled in the Procedure keeping CustList as HardCoded can be made dynamic
    **************************************************/
SET NOCOUNT ON
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_CAMPAIGN_SHOPPERS_T120'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CAMPAIGN_SHOPPERS_T120'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_CAMPAIGN_SHOPPERS_T120'', CURRENT_TIMESTAMP, NULL,''I''');

/* In this step we need to gather all the customers for 120 days before the campaign making a transaction at the merchant (not competitor). The start date
is 120 days before the campaign and end date will be 1 day before the campaign. The month will be set according to the date.
*/
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
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL    --Pick all the Merchants 
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
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL    --Pick all the Merchants 
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
	SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	SET @startDate = DATEADD(DD, - 120, @CampaignStartDate) -- 120 days before the campaign
	-- SHASHI CHANGES on 04/06/2019 Starts
	-- SET @EndDate = DATEADD(DD, - 1, @CampaignStartDate) -- 1 day before the Campaign
	SET @EndDate = CASE WHEN LEFT(CONVERT(VARCHAR(10), (DATEADD(DD, - 1, @CampaignStartDate)), 112), 6)<=LEFT(CONVERT(VARCHAR(10), 
                                                            (DATEADD(MONTH, - 1, @CUSTLIST)), 112), 6)
                                 THEN CAST(DATEADD(DD, - 1, @CampaignStartDate) AS DATE)
                                 ELSE CAST(CONVERT(VARCHAR(10), DBO.UDF_GETLASTDAYOFMONTH((DATEADD(MONTH, - 1, @CUSTLIST))), 112) AS DATE)   --REPORT END DATE     
                                 END    --END DATE WILL BE 1 Day Before FROM THE CAMPAIGN DATE OR MAX TRANSACTION AVAILABLE WHICHEVER IS LESSER
	-- SHASHI CHANGES on 04/06/2019 ENDS
	-- Added since for run of May New Merchants added as Part of AllDoshMerchantListNew table having campaign start date as future date of report end date 	
	SET @Month = left(CONVERT(VARCHAR(10), convert(DATE,@startDate, 121), 112) , 6) -- Get month from Start Date

EXEC  (
		'
		USE Dosh

		IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''CampaignShoppersActivity_' + @Merchant + '_' + @Version + '''
			)

		DROP TABLE  Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '


		SELECT Top 0 ''Credit'' AS pop,''Without Transaction'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE,c.ArgusPermId
		INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
		FROM (
			Select TransactionDate, AccountID , BankID 
			FROM Dosh.dbo.Trans_Credit_dosh_' + @Month + '_' + @Version + '
			WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
			AND OnlineFlag IN ' + @OnlineFlag + '
			AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
			)a
		INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped c
		ON a.AccountID = c.AccountId
		AND a.bankId = c.BankID
		INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
		On CASt(c.ArgusPermId AS VARCHAR(MAX))= CAST(p.ArguspermID AS VARCHAR(MAX))
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
		On p.dosh_customer_key = l.dosh_customer_key
		WHERE LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
		Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId 
		--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
		'
		)

WHILE @Month <= LEFT(CONVERT(VARCHAR(10), convert(DATE,@EndDate, 121), 112), 6)
BEGIN
	SET @PrevMonth = CASE 
			WHEN RIGHT(@MONTH, 2) <> 01
				THEN (@MONTH - 01)
			ELSE (@MONTH - 89)
			END

		EXEC  (
				'
USE Dosh

INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
SELECT ''Credit'' AS pop,''With Transaction'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
FROM (
	Select TransactionDate, AccountID , BankID 
	FROM Dosh.dbo.Trans_Credit_dosh_' + @Month + '_' + @Version + '
	WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
	AND OnlineFlag IN ' + @OnlineFlag + '
	AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
	)a
INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped c
ON a.AccountID = c.AccountId
AND a.bankId = c.BankID
INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On CASt(c.ArgusPermId AS VARCHAR(MAX))= CAST(p.ArguspermID AS VARCHAR(MAX))
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId


INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
SELECT ''Debit''  AS pop,''With Transaction'' as segment, CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
FROM (
	Select TransactionDate, AccountID , BankID 
	FROM Dosh.dbo.Trans_Debit_dosh_' + @Month + '_' + @Version + '
	WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantdebitSicCode + '
	AND OnlineFlag IN ' + @OnlineFlag + ' 
	AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
	)a
INNER JOIN Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + 
				'_Deduped c
ON a.AccountID = c.DepositAccountID
AND a.bankId = c.BankID
INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On CASt(c.ArgusPermId AS VARCHAR(MAX))= CAST(p.ArguspermID AS VARCHAR(MAX))
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId


'
				)

		EXEC  (
				'
USE Dosh

INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
SELECT ''Credit'' AS pop,''Without Transaction'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
FROM (
	Select TransactionDate, AccountID , BankID 
	FROM Dosh.dbo.Trans_Credit_dosh_' + @Month + '_' + @Version + '
	WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
	AND OnlineFlag IN ' + @OnlineFlag + '
	AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
	)a
INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped c
ON a.AccountID = c.AccountId
AND a.bankId = c.BankID
INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On CASt(c.ArgusPermId AS VARCHAR(MAX))= CAST(p.ArguspermID AS VARCHAR(MAX))
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + 
				' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId


INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
SELECT ''Debit''  AS pop,''Without Transaction'' as segment, CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
FROM (
	Select TransactionDate, AccountID , BankID 
	FROM Dosh.dbo.Trans_Debit_dosh_' + @Month + '_' + @Version + '
	WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantdebitSicCode + '
	AND OnlineFlag IN ' + @OnlineFlag + ' 
	AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
	)a
INNER JOIN Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + 
				'_Deduped c
ON a.AccountID = c.DepositAccountID
AND a.bankId = c.BankID
INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
On CASt(c.ArgusPermId AS VARCHAR(MAX))= CAST(p.ArguspermID AS VARCHAR(MAX))
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
On p.dosh_customer_key = l.dosh_customer_key
WHERE LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId

'
				)

	--Non Dosh
	
	
	EXEC  (
			'
			INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + '
			SELECT ''Credit'' AS pop,''NonDosh'' as segment,CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
			FROM (
				Select TransactionDate, AccountID , BankID 
				FROM Dosh.dbo.Trans_Credit_Nondosh_' + @Month + '_' + @Version + '
				WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantCreditSicCode + '
				AND OnlineFlag IN ' + @OnlineFlag + '
				AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
				)a
			INNER JOIN Dosh.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped c
			ON a.AccountID = c.AccountId
			AND a.bankId = c.BankID 
			Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId
			--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId


			INSERT INTO Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + 
							'
			SELECT ''Debit''  AS pop,''NonDosh'' as segment, CAST(a.transactiondate AS DATE) AS TRANSACTIONDATE, c.ArgusPermId
			FROM (
				Select TransactionDate, AccountID , BankID 
				FROM Dosh.dbo.Trans_Debit_Nondosh_' + @Month + '_' + @Version + '
				WHERE MerchantIndexNumber = ' + @MerchantIndexNumber + ' AND SicCode IN ' + @MerchantdebitSicCode + '
				AND OnlineFlag IN ' + @OnlineFlag + ' 
				AND CAST(transactiondate AS DATE)>=  ''' + @startdate + ''' and CAST(transactiondate AS DATE)<=''' + @enddate + '''
				)a
			INNER JOIN Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped c
			ON a.AccountID = c.DepositAccountID
			AND a.bankId = c.BankID
			Group by CAST(a.transactiondate AS DATE) ,c.ArgusPermId 
			--order by CAST(a.transactiondate AS DATE) ,c.ArgusPermId

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
	ALTER TABLE Dosh.dbo.CampaignShoppersActivity_' + @Merchant + '_' + @Version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
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


--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_CAMPAIGN_SHOPPERS_T120''');
           
           END