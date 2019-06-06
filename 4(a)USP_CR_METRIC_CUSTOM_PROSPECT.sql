USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_CR_METRIC_CUSTOM_PROSPECT] @CustList VARCHAR(50)

AS
/*******************************************
    ** FILE: 
    ** NAME: USP_CR_METRIC_CUSTOM_PROSPECT
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
		WHERE PERIOD=@CustList AND PROC_NAME='USP_CR_METRIC_CUSTOM_PROSPECT'
				
IF @lv_Status<>'C' 
	
BEGIN 	
EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CR_METRIC_CUSTOM_PROSPECT'' AND PERIOD='+@CustList+';
		INSERT INTO DBO.Performance_Monitoring
		SELECT '+@CustList+', ''USP_CR_METRIC_CUSTOM_PROSPECT'', CURRENT_TIMESTAMP, NULL,''I''');

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

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)


SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Competitor
	,Merchant
	,OnlineFlag
	,CompetitorIndexNumber
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CompetitorCreditSicCode=''  and CampaignStartDate is NULL  --FOR PROSPECT'S MERCHANT CampaignStartDate=''


OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @competitor
	,@merchant
	,@OnlineFlag
	,@CompetitorIndexNumber


WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC 	 (
			 '
			/*Step 0 Build a shell*/
			use dosh

			IF EXISTS
				(
				SELECT * 
				FROM Dosh.INFORMATION_SCHEMA.Tables
				WHERE Table_Name = ''Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '''
				)


			DROP TABLE Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '


			create table dosh.[dbo].Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '(
			merchant  Varchar(25),
			TransactionDate date,
			pop  Varchar(50),
			TotalCustomers Int,
			[Dosh Credit Spend]  money,
			[Dosh Credit Transactions] money,
			[Dosh Credit Shoppers] money)
			'
			)

	SET @Month = @ReportStart 
	WHILE @Month <= @ReportEnd  --it will be same 12 Month logic asked Prachi and Confirmed 
	
		BEGIN
			EXEC  (
				  '
					--1) Linked Without Transactions
					INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @competitor + ''' as merchant,a.TransactionDate,''Linked Without Transactions'' AS Pop, '''' as TotalCustomers,
					SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
					FROM Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + ' a
						INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped b with (nolock)
						ON a.AccountID = b.AccountId
						AND a.bankId = b.BankID
						INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
						On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
						INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d
						On c.dosh_customer_key = d.dosh_customer_key
						where  OnlineFlag IN ' + @OnlineFlag + ' 
						AND LinkedTag = ''Linked'' and TransTag = ''WithoutTrans''
						and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240) 
						group by a.TransactionDate


					--1) Linked With Transactions
					INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @competitor + '''as merchant,a.TransactionDate,''Linked With Transactions'' AS Pop, '''' as TotalCustomers,
					SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
					FROM Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '  a
						INNER JOIN Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped b with (nolock)
						ON a.AccountID = b.AccountId
						AND a.bankId = b.BankID
						INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  c
						On CAST(b.ArgusPermId  AS VARCHAR(MAX))=CAST( c.ArguspermID AS VARCHAR(MAX))
						INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d
						On c.dosh_customer_key = d.dosh_customer_key
						where  OnlineFlag IN ' + @OnlineFlag + ' 
						AND LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
						and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240) 
						group by a.TransactionDate


					--3) NonDosh
					INSERT INTO Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @competitor + ''' as merchant,a.TransactionDate,''NonDosh'' AS Pop, '''' as TotalCustomers,
					SUM(PositiveSpend+NegativeSpend) AS [Dosh Credit Spend], SUM(PositiveTrans+NegativeTrans) AS [Dosh Credit Transactions], COUNT(DISTINCT B.ArgusPermID) AS [Dosh Credit Shoppers]
					FROM Dosh.dbo.Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '  a
						INNER JOIN Dosh.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped b with (nolock)
						ON a.AccountID = b.AccountId
						AND a.bankId = b.BankID
						where OnlineFlag IN ' + @OnlineFlag + ' 
						and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240)
						group by a.TransactionDate



					'
					)

			SET @Month = CASE 
			WHEN RIGHT(@Month, 2) = 12
					THEN @Month + 89
					ELSE @Month + 1
					END
		END

		EXEC ('
				CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + ' (pop,transactiondate)
				ALTER TABLE Dosh.dbo.Credit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
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
	   AND PROC_NAME = ''USP_CR_METRIC_CUSTOM_PROSPECT''');
END