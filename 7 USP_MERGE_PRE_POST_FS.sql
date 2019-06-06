USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_MERGE_PRE_POST_FS] @CustList VARCHAR(50)

AS
/*******************************************
    ** FILE: 
    ** NAME: USP_MERGE_PRE_POST_FS
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

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_MERGE_PRE_POST_FS'
				
				IF @lv_Status<>'C' 
					Begin	
	EXEC ('DELETE FROM Dosh.DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_MERGE_PRE_POST_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO Dosh.DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_MERGE_PRE_POST_FS'', CURRENT_TIMESTAMP, NULL,''I''');

-- I have added the INTO statement where the naming convention is as per the final deliverable of CSV file.
DECLARE @Month VARCHAR(10)
DECLARE @Version VARCHAR(20)
DECLARE @Merchant VARCHAR(100)
DECLARE @MerchantIndexNumber INT
DECLARE @Industry_ID VARCHAR(500)
DECLARE @Competitor VARCHAR(100)
DECLARE @lastMonday VARCHAR(15)

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	,MerchantIndexNumber
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' --and CompetitorCreditSicCode<>''	and CompetitorDebitSicCode<>''  Removed to Include All Merchants Including Custom Merchants 
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,MerchantIndexNumber
	,Competitor		--newly added
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>''   and CampaignStartDate is not NULL  --and CompetitorCreditSicCode<>''	and CompetitorDebitSicCode<>''  Removed to Include All Merchants Including Custom Merchants 
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @Merchant
	,@MerchantIndexNumber
	,@Competitor	--Joseph R K changes on 23/04/2019

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	--Shashi Changes on 06/05/2019 start 
	--replace the ‘@lastMonday’ parameter with the Monday that covers the report end date
	--SET @lastMonday=CONVERT(VARCHAR(10), DATEADD(dd, - (DATEDIFF(dd, 0, dbo.udf_GetLastDayOfMonth(DateAdd(mm, -1, @CustList )))%7), dbo.udf_GetLastDayOfMonth(DateAdd(mm, -1, @CustList ))),112) */
	SET @lastMonday=CONVERT(VARCHAR,DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, @CustList), @CustList)), 0),112)
	--Shashi Changes on 06/05/2019 end


--Joseph R K changes on 23/04/2019 start
/*	
	EXEC (
			'

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''' + @Merchant + '_Frequency_Segment_' + @CustList + '''
	)

DROP TABLE Dosh.dbo.' + @Merchant + '_Frequency_Segment_' + @CustList + '


Select *
Into Dosh.dbo.'+ @Merchant + '_Frequency_Segment_' + @CustList + '
From
(
Select ''Pre-Campaign-Frequency'' CampFreq
,a.transaction_date
,a.merchant
,a.population
,a.campaign_zip_flag
,a.PreFrequency as Frequency
,a.total_customers
,a.redeemed_customers
,a.merchant_credit_spend
,a.industry_credit_spend
,a.merchant_credit_transaction
,a.industry_credit_transaction
,a.merchant_credit_shoppers
,a.industry_credit_shoppers
,a.credit_Spend_Share
,a.merchant_credit_ticket_size
,a.industry_credit_ticket_size
,a.merchant_debit_sig_spend
,a.merchant_debit_pin_spend
,a.merchant_debit_sig_transaction
,a.merchant_debit_pin_transaction
,a.industry_debit_sig_spend
,a.industry_debit_pin_spend
,a.industry_debit_sig_transaction
,a.industry_debit_pin_transaction
,a.merchant_debit_sig_shoppers
,a.merchant_debit_pin_shoppers
,a.industry_debit_sig_shoppers
,a.industry_debit_pin_shoppers
,a.debit_sig_spend_share
,a.debit_pin_spend_share
,a.merchant_debit_sig_ticket_size
,a.merchant_debit_pin_ticket_size
,a.industry_debit_sig_ticket_size
,a.industry_debit_pin_ticket_size
,a.industry
,a.industry_id
,a.customer_refresh_date

from Dosh.dbo.'+ @Merchant + '_Pre_Frequency_Segment_' + @CustList + ' a 

union all

Select ''Post-Campaign-Frequency'' CampFreq
,a.transaction_date
,a.merchant
,a.population
,a.campaign_zip_flag
,a.PostFrequency as Frequency
,a.total_customers
,a.redeemed_customers
,a.merchant_credit_spend
,a.industry_credit_spend
,a.merchant_credit_transaction
,a.industry_credit_transaction
,a.merchant_credit_shoppers
,a.industry_credit_shoppers
,a.credit_Spend_Share
,a.merchant_credit_ticket_size
,a.industry_credit_ticket_size
,a.merchant_debit_sig_spend
,a.merchant_debit_pin_spend
,a.merchant_debit_sig_transaction
,a.merchant_debit_pin_transaction
,a.industry_debit_sig_spend
,a.industry_debit_pin_spend
,a.industry_debit_sig_transaction
,a.industry_debit_pin_transaction
,a.merchant_debit_sig_shoppers
,a.merchant_debit_pin_shoppers
,a.industry_debit_sig_shoppers
,a.industry_debit_pin_shoppers
,a.debit_sig_spend_share
,a.debit_pin_spend_share
,a.merchant_debit_sig_ticket_size
,a.merchant_debit_pin_ticket_size
,a.industry_debit_sig_ticket_size
,a.industry_debit_pin_ticket_size
,a.industry
,a.industry_id
,a.customer_refresh_date

from Dosh.dbo.'+ @Merchant + '_Post_Frequency_Segment_' + @CustList + '  a ) a

'
			)
*/

EXEC (
	'

	IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''' + @Merchant +  '_' + @Competitor + '_Frequency_Segment_' + @CustList + '''
	)

	DROP TABLE Dosh.dbo.' + @Merchant +  '_' + @Competitor + '_Frequency_Segment_' + @CustList + '


	Select *
	Into Dosh.dbo.'+ @Merchant +  '_' + @Competitor + '_Frequency_Segment_' + @CustList +'
	From
	(
	Select ''Pre-Campaign-Frequency'' CampFreq
	,a.transaction_date
	,a.merchant
	,a.population
	,a.campaign_zip_flag
	,a.PreFrequency as Frequency
	,a.total_customers
	,a.redeemed_customers
	,a.merchant_credit_spend
	,a.industry_credit_spend
	,a.merchant_credit_transaction
	,a.industry_credit_transaction
	,a.merchant_credit_shoppers
	,a.industry_credit_shoppers
	,a.credit_Spend_Share
	,a.merchant_credit_ticket_size
	,a.industry_credit_ticket_size
	,a.merchant_debit_sig_spend
	,a.merchant_debit_pin_spend
	,a.merchant_debit_sig_transaction
	,a.merchant_debit_pin_transaction
	,a.industry_debit_sig_spend
	,a.industry_debit_pin_spend
	,a.industry_debit_sig_transaction
	,a.industry_debit_pin_transaction
	,a.merchant_debit_sig_shoppers
	,a.merchant_debit_pin_shoppers
	,a.industry_debit_sig_shoppers
	,a.industry_debit_pin_shoppers
	,a.debit_sig_spend_share
	,a.debit_pin_spend_share
	,a.merchant_debit_sig_ticket_size
	,a.merchant_debit_pin_ticket_size
	,a.industry_debit_sig_ticket_size
	,a.industry_debit_pin_ticket_size
	,a.industry
	,a.industry_id
	,a.customer_refresh_date

	from Dosh.dbo.'+ @Merchant + '_' + @Competitor + '_Pre_Frequency_Segment_' + @CustList +' a 

	union all

	Select ''Post-Campaign-Frequency'' CampFreq
	,a.transaction_date
	,a.merchant
	,a.population
	,a.campaign_zip_flag
	,a.PostFrequency as Frequency
	,a.total_customers
	,a.redeemed_customers
	,a.merchant_credit_spend
	,a.industry_credit_spend
	,a.merchant_credit_transaction
	,a.industry_credit_transaction
	,a.merchant_credit_shoppers
	,a.industry_credit_shoppers
	,a.credit_Spend_Share
	,a.merchant_credit_ticket_size
	,a.industry_credit_ticket_size
	,a.merchant_debit_sig_spend
	,a.merchant_debit_pin_spend
	,a.merchant_debit_sig_transaction
	,a.merchant_debit_pin_transaction
	,a.industry_debit_sig_spend
	,a.industry_debit_pin_spend
	,a.industry_debit_sig_transaction
	,a.industry_debit_pin_transaction
	,a.merchant_debit_sig_shoppers
	,a.merchant_debit_pin_shoppers
	,a.industry_debit_sig_shoppers
	,a.industry_debit_pin_shoppers
	,a.debit_sig_spend_share
	,a.debit_pin_spend_share
	,a.merchant_debit_sig_ticket_size
	,a.merchant_debit_pin_ticket_size
	,a.industry_debit_sig_ticket_size
	,a.industry_debit_pin_ticket_size
	,a.industry
	,a.industry_id
	,a.customer_refresh_date

	from Dosh.dbo.'+ @Merchant + '_' + @Competitor + '_Post_Frequency_Segment_' + @CustList +'  a ) a
	'
	)

--Joseph R K changes on 23/04/2019 end


	FETCH NEXT
	FROM MerchantCursor
	INTO @Merchant
		,@MerchantIndexNumber
		,@Competitor	--Joseph R K changes on 23/04/2019
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
		   EXEC (' UPDATE Dosh.DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
				   AND PROC_NAME = ''USP_MERGE_PRE_POST_FS''');
		   EXEC (' UPDATE Dosh.DBO.Execution_Period_Detail_DEV SET FS_IS_PROCESSED=''Y'' WHERE  PeriodId = '+@CustList+'');           
   
            
END