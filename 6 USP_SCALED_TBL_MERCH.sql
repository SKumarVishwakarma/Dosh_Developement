USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_SCALED_TBL_MERCH]    Script Date: 03/08/2019 16:28:35 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_SCALED_TBL_MERCH] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 5.b Combined Credit and Debit Metrics for Regional Campaign
    ** NAME: USP_SCALED_TBL_MERCH
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
	22/02/2019		PRACHI				I have added the INTO statement where the naming convention is as per the final deliverable of CSV file.
	22/02/2019		SHASHI				CustList parameter missing @ added to all parameter wherever being used 
										
	22/02/2019		SHASHI				All the Parameters for Now Handled in the Procedure keeping CustList as HardCoded can be made dynamic
	 26/02/2019		PRACHI				Changing the columns names to meet the final client requirement for naming convention
    **************************************************/
SET NOCOUNT ON

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_SCALED_TBL_MERCH'
				
				IF @lv_Status<>'C' 
					Begin	
	EXEC  ('DELETE FROM Dosh.DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_SCALED_TBL_MERCH'' AND PERIOD='+@CustList+';
		   INSERT INTO Dosh.DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_SCALED_TBL_MERCH'', CURRENT_TIMESTAMP, NULL,''I''');

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
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL -- Removed this condition and  MerchantIndexNumber=92  
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,MerchantIndexNumber
	,Competitor		--newly added
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL -- Removed this condition and  MerchantIndexNumber=92  
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

	EXEC  (
			'

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''' + @Merchant + '_Value_Of_Dosh_' + @CustList + '''
	)

DROP TABLE Dosh.dbo.' + @Merchant + '_Value_Of_Dosh_' + @CustList + '


SELECT 
a.transactiondate AS transaction_date,
''' + @Merchant + ''' AS merchant,
cast(a.pop as Varchar(100)) AS [population],  --Cast Varchar(100) has been Added as requested by Yilun
a.Campaign_Zip_Flag AS campaign_zip_flag,
cast(a.Segment as varchar(10)) AS segment,    --Cast Varchar(10) has been Added as requested by Yilun
CAST(ROUND(totalCustomer,0) AS INT) AS total_customers,
CAST(ROUND(RedeemedCustomers,0) AS INT) AS redeemed_customers,  -- Added to Round and Cast as INT for the redeemed customer prev RedeemedCustomers AS redeemed_customers
           
CAST(([Merchant Credit Spend]/c.creditspend ) AS NUMERIC(38,2)) as merchant_credit_spend,
CAST(([Industry Credit Spend]/d.creditspend ) AS NUMERIC(38,2))as industry_credit_spend,

CAST(([Merchant Credit Transaction]/c.creditspend ) AS NUMERIC(38,2))as merchant_credit_transaction,
CAST(([Industry Credit Transaction]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_transaction,

CAST(ROUND(([Merchant Credit Shoppers]/c.CreditShopper),0) AS INT) as merchant_credit_shoppers,
CAST(ROUND(([Industry Credit Shoppers]/d.CreditShopper),0) AS INT) as industry_credit_shoppers,

case when [Industry Credit Spend] =0 then 0 else ([Merchant Credit Spend]*d.creditspend)/(c.creditspend *[Industry Credit Spend]) end as credit_Spend_Share,

CAST((case when [Merchant Credit Transaction] =0 then 0 else [Merchant Credit Spend]/[Merchant Credit Transaction] end ) AS NUMERIC(38,2)) as merchant_credit_ticket_size,
CAST((case when [Industry Credit Transaction] =0 then 0 else [Industry Credit Spend]/[Industry Credit Transaction] end ) AS NUMERIC(38,2)) as industry_credit_ticket_size,


CAST(([Merchant Debit Sig Spend]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_sig_spend,
CAST(([Merchant Debit Pin Spend]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_pin_spend,
CAST(([Merchant Debit Sig transactions]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_sig_transaction,
CAST(([Merchant Debit Pin transactions]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_pin_transaction,

CAST(([Industry Debit Sig Spend]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_sig_spend,
CAST(([Industry Debit Pin Spend]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_pin_spend,
CAST(([Industry Debit Sig transactions]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_sig_transaction,
CAST(([Industry Debit Pin transactions]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_pin_transaction,

CAST(ROUND(([Merchant debit Sig Shoppers]/c.debitShopper),0) AS INT) as merchant_debit_sig_shoppers,
CAST(ROUND(([Merchant debit Pin Shoppers]/c.debitShopper),0) AS INT) as merchant_debit_pin_shoppers,
CAST(ROUND(([Industry debit Sig Shoppers]/d.debitShopper),0) AS INT) as industry_debit_sig_shoppers,
CAST(ROUND(([Industry debit Pin Shoppers]/d.debitShopper),0) AS INT) as industry_debit_pin_shoppers,

case when [Industry debit sig Spend] =0 then 0 else ([Merchant debit sig Spend]*d.debitspend)/(c.debitspend *[Industry debit sig Spend]) end as debit_sig_spend_share,
case when [Industry debit pin Spend] =0 then 0 else ([Merchant debit pin Spend]*d.debitspend)/(c.debitspend *[Industry debit pin Spend]) end as debit_pin_spend_share,
CAST((case when [Merchant debit sig Transactions] =0 then 0 else [Merchant debit sig Spend]/[Merchant debit sig Transactions] end) AS NUMERIC(38,2)) as merchant_debit_sig_ticket_size,
CAST((case when [Merchant debit pin Transactions] =0 then 0 else [Merchant debit pin Spend]/[Merchant debit pin Transactions] end) AS NUMERIC(38,2)) as merchant_debit_pin_ticket_size,
CAST((case when [Industry debit sig Transactions] =0 then 0 else [Industry debit sig Spend]/[Industry debit sig Transactions] end) AS NUMERIC(38,2)) as industry_debit_sig_ticket_size,
CAST((case when [Industry debit pin Transactions] =0 then 0 else [Industry debit pin Spend]/[Industry debit pin Transactions] end) AS NUMERIC(38,2)) as industry_debit_pin_ticket_size,
industry,
industry_id,
CAST('''+@lastMonday+''' AS DATE) AS customer_refresh_date

INTO Dosh.dbo.' 
			+ @Merchant + '_Value_Of_Dosh_' + @CustList + '
FROM 
DOSH.dbo.'+@Merchant+'_Raw_Data_OutPut_'+@Version+' a
LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
			WHERE merchant in (''Merchant'')
		  ) c
ON a.Pop = c.Pop

LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
			WHERE merchant in (''Competitor'')
		  ) d
ON a.Pop = d.Pop
--order by 3,1,4,LEN(Segment)

'
			)
*/


EXEC  (
	'

	IF EXISTS
		(
		SELECT * 
		FROM Dosh.INFORMATION_SCHEMA.Tables
		WHERE Table_Name = ''' + @Merchant + '_' + @Competitor + '_Value_Of_Dosh_' + @CustList + '''
		)

	DROP TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Value_Of_Dosh_' + @CustList + '

	SELECT 
	a.transactiondate AS transaction_date,
	''' + @Merchant + ''' AS merchant,
	cast(a.pop as Varchar(100)) AS [population],  --Cast Varchar(100) has been Added as requested by Yilun
	a.Campaign_Zip_Flag AS campaign_zip_flag,
	cast(a.Segment as varchar(10)) AS segment,    --Cast Varchar(10) has been Added as requested by Yilun
	CAST(ROUND(totalCustomer,0) AS INT) AS total_customers,
	CAST(ROUND(RedeemedCustomers,0) AS INT) AS redeemed_customers,  -- Added to Round and Cast as INT for the redeemed customer prev RedeemedCustomers AS redeemed_customers
			   
	CAST(([Merchant Credit Spend]/c.creditspend ) AS NUMERIC(38,2)) as merchant_credit_spend,
	CAST(([Industry Credit Spend]/d.creditspend ) AS NUMERIC(38,2))as industry_credit_spend,

	CAST(([Merchant Credit Transaction]/c.creditspend ) AS NUMERIC(38,2))as merchant_credit_transaction,
	CAST(([Industry Credit Transaction]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_transaction,

	CAST(ROUND(([Merchant Credit Shoppers]/c.CreditShopper),0) AS INT) as merchant_credit_shoppers,
	CAST(ROUND(([Industry Credit Shoppers]/d.CreditShopper),0) AS INT) as industry_credit_shoppers,

	case when [Industry Credit Spend] =0 then 0 else ([Merchant Credit Spend]*d.creditspend)/(c.creditspend *[Industry Credit Spend]) end as credit_Spend_Share,

	CAST((case when [Merchant Credit Transaction] =0 then 0 else [Merchant Credit Spend]/[Merchant Credit Transaction] end ) AS NUMERIC(38,2)) as merchant_credit_ticket_size,
	CAST((case when [Industry Credit Transaction] =0 then 0 else [Industry Credit Spend]/[Industry Credit Transaction] end ) AS NUMERIC(38,2)) as industry_credit_ticket_size,


	CAST(([Merchant Debit Sig Spend]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_sig_spend,
	CAST(([Merchant Debit Pin Spend]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_pin_spend,
	CAST(([Merchant Debit Sig transactions]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_sig_transaction,
	CAST(([Merchant Debit Pin transactions]/c.DebitSpend) AS NUMERIC(38,2)) as merchant_debit_pin_transaction,

	CAST(([Industry Debit Sig Spend]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_sig_spend,
	CAST(([Industry Debit Pin Spend]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_pin_spend,
	CAST(([Industry Debit Sig transactions]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_sig_transaction,
	CAST(([Industry Debit Pin transactions]/d.DebitSpend) AS NUMERIC(38,2)) as industry_debit_pin_transaction,

	CAST(ROUND(([Merchant debit Sig Shoppers]/c.debitShopper),0) AS INT) as merchant_debit_sig_shoppers,
	CAST(ROUND(([Merchant debit Pin Shoppers]/c.debitShopper),0) AS INT) as merchant_debit_pin_shoppers,
	CAST(ROUND(([Industry debit Sig Shoppers]/d.debitShopper),0) AS INT) as industry_debit_sig_shoppers,
	CAST(ROUND(([Industry debit Pin Shoppers]/d.debitShopper),0) AS INT) as industry_debit_pin_shoppers,

	case when [Industry debit sig Spend] =0 then 0 else ([Merchant debit sig Spend]*d.debitspend)/(c.debitspend *[Industry debit sig Spend]) end as debit_sig_spend_share,
	case when [Industry debit pin Spend] =0 then 0 else ([Merchant debit pin Spend]*d.debitspend)/(c.debitspend *[Industry debit pin Spend]) end as debit_pin_spend_share,
	CAST((case when [Merchant debit sig Transactions] =0 then 0 else [Merchant debit sig Spend]/[Merchant debit sig Transactions] end) AS NUMERIC(38,2)) as merchant_debit_sig_ticket_size,
	CAST((case when [Merchant debit pin Transactions] =0 then 0 else [Merchant debit pin Spend]/[Merchant debit pin Transactions] end) AS NUMERIC(38,2)) as merchant_debit_pin_ticket_size,
	CAST((case when [Industry debit sig Transactions] =0 then 0 else [Industry debit sig Spend]/[Industry debit sig Transactions] end) AS NUMERIC(38,2)) as industry_debit_sig_ticket_size,
	CAST((case when [Industry debit pin Transactions] =0 then 0 else [Industry debit pin Spend]/[Industry debit pin Transactions] end) AS NUMERIC(38,2)) as industry_debit_pin_ticket_size,
	industry,
	industry_id,
	CAST('''+@lastMonday+''' AS DATE) AS customer_refresh_date

	INTO Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Value_Of_Dosh_' + @CustList + '
	FROM 
	dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Output_'+@Version+' a
	LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
				WHERE merchant in (''Merchant'')
			  ) c
	ON a.Pop = c.Pop

	LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
				WHERE merchant in (''Competitor'')
			  ) d
	ON a.Pop = d.Pop
	--order by 3,1,4,LEN(Segment)

	'
	)			
	--Joseph R K changes on 23/04/2019 end

	--Added on 24-05-2019 start
	EXEC('	
	ALTER TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Value_Of_Dosh_' + @CustList + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	')
	--Added on 24-05-2019 end

	FETCH NEXT
	FROM MerchantCursor
	INTO @Merchant
		,@MerchantIndexNumber
		,@Competitor	--Joseph R K changes on 23/04/2019
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
   EXEC  (' UPDATE Dosh.DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_SCALED_TBL_MERCH''');
   EXEC  (' UPDATE Dosh.DBO.Execution_Period_Detail_DEV SET VOD_IS_PROCESSED=''Y'' WHERE  PeriodId = '+@CustList+'');           
            
END