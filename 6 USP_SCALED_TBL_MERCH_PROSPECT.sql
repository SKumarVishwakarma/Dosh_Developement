USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_SCALED_TBL_MERCH_PROSPECT] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: 
    ** NAME: USP_SCALED_TBL_MERCH_PROSPECT
    ** DESC: 
    **
    ** AUTH: SHASHIKUMAR VISHWAKARMA
    ** DATE: 15/05/2019
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

SELECT @lv_Status=[STATUS] FROM DBO.Performance_Monitoring
WHERE PERIOD=@CustList AND PROC_NAME='USP_SCALED_TBL_MERCH_PROSPECT'
				
IF @lv_Status<>'C' 
BEGIN	
	EXEC  ('DELETE FROM Dosh.DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_SCALED_TBL_MERCH_PROSPECT'' AND PERIOD='+@CustList+';
		   INSERT INTO Dosh.DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_SCALED_TBL_MERCH_PROSPECT'', CURRENT_TIMESTAMP, NULL,''I''');

	DECLARE @Month VARCHAR(10)
	DECLARE @Version VARCHAR(20)
	DECLARE @Merchant VARCHAR(100)
	DECLARE @MerchantIndexNumber INT
	DECLARE @Industry_ID VARCHAR(500)
	DECLARE @Competitor VARCHAR(100)
	DECLARE @lastMonday VARCHAR(15)


	DECLARE MerchantCursor CURSOR
	FOR
	SELECT DISTINCT Merchant
		,MerchantIndexNumber
		,Competitor
	FROM Dosh.dbo.AllDoshMerchantListNew WHERE Is_Active=1 AND Competitor<>''  and CampaignStartDate is NULL  --FOR PROSPECT'S MERCHANT CampaignStartDate=''

	OPEN MerchantCursor

	FETCH NEXT
	FROM MerchantCursor
	INTO @Merchant
		,@MerchantIndexNumber
		,@Competitor

	WHILE @@FETCH_STATUS = 0
		BEGIN
			
			SET @version=LEFT(DATENAME(MM, DATEADD(mm,-1,@CustList)), 3) + RIGHT(LEFT(@CustList,4),2) + 'TPS'
			
			SET @lastMonday=CONVERT(VARCHAR,DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, @CustList), @CustList)), 0),112)


			EXEC  (
				'

				IF EXISTS
					(
					SELECT * 
					FROM Dosh.INFORMATION_SCHEMA.Tables
					WHERE Table_Name = ''' + @Merchant + '_' + @Competitor + '_Prospect_Report_' + @CustList + '''
					)

				DROP TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Prospect_Report_' + @CustList + '


				SELECT 
				a.transactiondate AS transaction_date,
				''' + @Merchant + ''' AS merchant,
				cast(a.pop as Varchar(100)) AS [population],  --Cast Varchar(100) has been Added as requested by Yilun
				CAST(ROUND(totalCustomer,0) AS INT) AS total_customers,
				
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

				INTO Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Prospect_Report_' + @CustList + '
				FROM 
				dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Prospect_'+@Version+' a
				LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
							WHERE merchant in (''Merchant'')
						  ) c
				ON a.Pop = c.Pop

				LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
							WHERE merchant in (''Competitor'')
						  ) d
				ON a.Pop = d.Pop
				--order by 3,1,4

				'
				)	

				
			EXEC ('				
			ALTER TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Prospect_Report_' + @CustList + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
			')


			FETCH NEXT
			FROM MerchantCursor
			INTO @Merchant
				,@MerchantIndexNumber
				,@Competitor	
		END

		CLOSE MerchantCursor

		DEALLOCATE MerchantCursor


		--Log the End Time
		   
		EXEC  (' UPDATE Dosh.DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
				   AND PROC_NAME = ''USP_SCALED_TBL_MERCH_PROSPECT''');
		EXEC  (' UPDATE Dosh.DBO.Execution_Period_Detail_DEV SET PROS_IS_PROCESSED=''Y'' WHERE  PeriodId = '+@CustList+'');           
            
	END