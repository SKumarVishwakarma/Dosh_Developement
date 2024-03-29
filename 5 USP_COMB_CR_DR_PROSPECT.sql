USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_COMB_CR_DR_PROSPECT] @CustList VARCHAR(50)

AS
/*******************************************
    ** FILE: 
    ** NAME: USP_COMB_CR_DR_PROSPECT
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
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''

SELECT @lv_Status=[STATUS] FROM DBO.Performance_Monitoring
WHERE PERIOD=@CustList AND PROC_NAME='USP_COMB_CR_DR_PROSPECT'
				
IF @lv_Status<>'C' 
	
BEGIN
	--Log the Start Time

EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_COMB_CR_DR_PROSPECT'' AND PERIOD='+@CustList+';
		INSERT INTO DBO.Performance_Monitoring
		SELECT '+@CustList+', ''USP_COMB_CR_DR_PROSPECT'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR (10)
DECLARE @Version VARCHAR(20)
DECLARE @StartDate VARCHAR(10)
DECLARE @EndDate VARCHAR(10)
DECLARE @Merchant      VarChar(100)
DECLARE @MerchantIndexNumber INT
DECLARE @Industry_ID VARCHAR(500)
DECLARE @Competitor VARCHAR(100)

DECLARE MerchantCursor CURSOR FOR
	SELECT DISTINCT Merchant, MerchantIndexNumber, Industry_ID, Competitor
	FROM Dosh.dbo.AllDoshMerchantListNew   
	WHERE Is_Active=1 AND Competitor<>''  and CampaignStartDate is NULL  --FOR PROSPECT'S MERCHANT CampaignStartDate=''

OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @Merchant, @MerchantIndexNumber, @Industry_ID, @Competitor

WHILE @@FETCH_STATUS=0
		
	BEGIN

			SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

			-- the start and date should be 12 months of data from the refresh date
			SET @StartDate = LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)
			SET @EndDate = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  

			EXEC ('

				IF EXISTS
				(
				SELECT * 
				FROM Dosh.INFORMATION_SCHEMA.Tables
				WHERE Table_Name = '''+@Merchant+'_'+@Competitor+'_Raw_Data_Prospect_'+@Version+'''
				)

				DROP TABLE dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Prospect_'+@Version+'


				select a.TransactionDate
				, a.pop ,
				t.PostScaled  as totalCustomer,   --This Column as totalCustomer Finalized by Yilun on 16/05/2019
				ISNULL(b.[dosh credit spend],0) as ''Merchant Credit Spend'',
				ISNULL(d.[Dosh Credit Spend],0) as ''Industry Credit Spend'',
				ISNULL(b.[Dosh Credit Transactions],0) as ''Merchant Credit Transaction'',
				ISNULL(d.[Dosh Credit Transactions],0) as ''Industry Credit Transaction'',
				ISNULL(b.[Dosh Credit Shoppers],0) as ''Merchant Credit Shoppers'',
				ISNULL(d.[Dosh Credit Shoppers],0) as ''Industry Credit Shoppers'',
				'''' AS ''Credit Spend Share'',
				'''' as ''Merchant ticketsize'',
				'''' as ''Industry ticketsize'',
				ISNULL(c.[Dosh Debit SIG Spend],0) as ''Merchant Debit Sig Spend'',
				ISNULL(c.[Dosh Debit PIN Spend],0) as ''Merchant Debit Pin Spend'',
				ISNULL(e.[Dosh Debit SIG Spend],0) as ''Industry Debit Sig Spend'',
				ISNULL(e.[Dosh Debit PIN Spend],0) as ''Industry Debit pin Spend'',
				ISNULL(c.[Dosh Debit SIG Transactions],0) as ''Merchant Debit Sig Transactions'',
				ISNULL(c.[Dosh Debit PIN Transactions],0) as ''Merchant Debit Pin Transactions'',
				ISNULL(e.[Dosh Debit SIG Transactions],0) as ''Industry Debit Sig Transactions'',
				ISNULL(e.[Dosh Debit PIN Transactions],0) as ''Industry Debit Pin Transactions'',
				ISNULL(c.[Dosh Debit SIG Shoppers],0) as ''Merchant Debit SIG Shoppers'',
				ISNULL(c.[Dosh Debit PIN Shoppers],0) as ''Merchant Debit PIN Shoppers'',
				ISNULL(e.[Dosh Debit SIG Shoppers],0) as ''Industry Debit SIG Shoppers'',
				ISNULL(e.[Dosh Debit PIN Shoppers],0) as ''Industry Debit PIN Shoppers'',
				'''' AS ''Debit Sig Spend Share'',
				'''' AS ''Debit Pin Spend Share'',
				'''' as ''Merchant Debit Sig Ticket Size'',
				'''' as ''Merchant Debit Pin Ticket Size'',
				'''' as ''Industry Debit Sig Ticket Size'',
				'''' as ''Industry Debit Pin Ticket Size'',
				'''+@Competitor+''' AS Industry,
				'''+@Industry_ID+''' AS Industry_Id

				into dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Prospect_'+@Version+'
				from Dosh.dbo.YG_Prospect_Dosh_Driver a    --This Table Finalized by Yilun on Call 16/05/2019
				LEFT JOIN Dosh.dbo.TotalCustomerScalingFactor_'+@CustList+' t    --This Table Finalized by Yilun on Call 16/05/2019
				ON a.Pop = t.POp 
				left join Dosh.dbo.Credit_'+@Merchant+'_Aggregate_Prospect_'+@Version+' b
				on a.transactiondate=b.transactiondate 
				and A.POP=B.POP
				left join Dosh.dbo.Debit_'+@Merchant+'_Aggregate_Prospect_'+@Version+' c
				on a.transactiondate=c.transactiondate  
				and A.POP=C.POP
				left join Dosh.dbo.Credit_'+@Merchant+'_'+@Competitor+'_Aggregate_Prospect_'+@Version+' d
				on a.transactiondate=d.transactiondate 
				and A.POP=d.POP
				left join Dosh.dbo.Debit_'+@Merchant+'_'+@Competitor+'_Aggregate_Prospect_'+@Version+' e
				on a.transactiondate=e.transactiondate 
				and A.POP=e.POP
				where left(CONVERT(VARCHAR(10), cast(a.transactiondate as date), 112) , 6) BETWEEN '''+@StartDate+'''  AND '''+@EndDate+'''
				and a.pop not in (''Industry'')
				')
				
			
			EXEC ('				
			ALTER TABLE dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Prospect_'+@Version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
			')

			FETCH NEXT FROM MerchantCursor INTO @Merchant, @MerchantIndexNumber, @Industry_ID, @Competitor

		END 
		CLOSE MerchantCursor

		DEALLOCATE MerchantCursor


		--Log the End Time

	   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
			   AND PROC_NAME = ''USP_COMB_CR_DR_PROSPECT''');
END