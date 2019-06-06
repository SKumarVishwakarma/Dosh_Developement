USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[USP_DR_METRIC_CUSTOM_PROSPECT] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 
    ** NAME: USP_DR_METRIC_CUSTOM_PROSPECT
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

SELECT @lv_Status=[STATUS] from DBO.Performance_Monitoring
WHERE PERIOD=@CustList and PROC_NAME='USP_DR_METRIC_CUSTOM_PROSPECT'
				
IF @lv_Status<>'C' 
BEGIN 	

	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_DR_METRIC_CUSTOM_PROSPECT'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_DR_METRIC_CUSTOM_PROSPECT'', CURRENT_TIMESTAMP, NULL,''I''');

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
	SELECT DISTINCT Competitor
		,Merchant
		,OnlineFlag
		,CompetitorIndexNumber
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CompetitorDebitSicCode='' and CampaignStartDate is NULL  --FOR PROSPECT'S MERCHANT CampaignStartDate=''
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
				WHERE Table_Name = ''Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '''
				)


			DROP TABLE Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '


			create table Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '(
			merchant  Varchar(25),
			TransactionDate date,
			pop  Varchar(50),
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
					INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,''0'' AS TotalCustomers,
					[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
					 FROM (
							SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop
							FROM ( SELECT * 
								   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + ') a
							inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
							ON a.AccountID = b.DepositAccountID
							AND a.bankId = b.BankID
							where a.BankID in (400,410,411,421,424,429,439,440,479,489) and OnlineFlag IN ' + 
									@OnlineFlag + '
							group by a.TransactionDate) z
					 left join(
							SELECT a.TransactionDate, ''Linked Without Transactions'' AS Pop,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
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
							where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
							and MerchantLookupType = ''SIG'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489)
							group by a.TransactionDate) a
					 ON z.TransactionDate = a.TransactionDate and z.pop = a.pop
					 LEFT JOIN (
							SELECT a.TransactionDate,''Linked Without Transactions'' AS Pop,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
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
							where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithoutTrans'' 
							and MerchantLookupType = ''pin'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489) 
							group by a.TransactionDate) b
					  ON z.TransactionDate = b.TransactionDate and z.pop =b.pop
					 
						'
						)

				EXEC  (
						'
					 --Linked with Transactions
					INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,''0'' AS TotalCustomers,
					[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
					 FROM (
							SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop
							FROM ( SELECT * 
								   FROM Dosh.dbo.Trans_Debit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
								   )a
							inner join Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
							ON a.AccountID = b.DepositAccountID
							AND a.bankId = b.BankID
							where a.BankID in (400,410,411,421,424,429,439,440,479,489)  and OnlineFlag IN ' + @OnlineFlag 
									+ '
							group by a.TransactionDate) z
					 left join(
							SELECT a.TransactionDate, ''Linked With Transactions'' AS Pop,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
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
							where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
							and MerchantLookupType = ''SIG'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489) 
							group by a.TransactionDate) a
					 ON z.TransactionDate = a.TransactionDate and z.pop = a.pop 
					 LEFT JOIN (
							SELECT a.TransactionDate,''Linked With Transactions'' AS Pop, SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
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
							where OnlineFlag IN ' + @OnlineFlag + ' and LinkedTag = ''Linked'' and TransTag = ''WithTrans'' 
							and MerchantLookupType = ''pin'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489)  
							group by a.TransactionDate) b
					  ON z.TransactionDate = b.TransactionDate and z.pop =b.pop 

					  
					 '
					)

				EXEC  (
					   '
					--NonDosh

					 INSERT INTO Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + '
					SELECT ''' + @Competitor + ''' AS Merchant,  Z.TransactionDate, Z.Pop,''0'' AS TotalCustomers,
					[Dosh Debit SIG Spend],[Dosh Debit PIN Spend], [Dosh Debit SIG Transactions],  [Dosh Debit PIN Transactions], [Dosh Debit SIG SHoppers], [Dosh Debit PIN Shoppers]
					 FROM(
							SELECT a.TransactionDate, ''NonDosh'' AS Pop
							FROM ( SELECT * 
								   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
								   )a
							inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
							ON a.AccountID = b.DepositAccountID
							AND a.bankId = b.BankID
							where Onlineflag IN ' + @OnlineFlag + 
									' and a.BankID in (400,410,411,421,424,429,439,440,479,489)  
							group by a.TransactionDate) z
					 left join(
							SELECT a.TransactionDate, ''NonDosh'' AS Pop,'''' as TotalCustomers,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'',
							 SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit SIG Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit SIG Shoppers''
							FROM  ( SELECT * 
								   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
								   )a
							inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
							ON a.AccountID = b.DepositAccountID
							AND a.bankId = b.BankID
							where OnlineFlag IN ' + @OnlineFlag + 
									' and MerchantLookupType = ''SIG'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489)
							group by a.TransactionDate) a
					 ON z.TransactionDate = a.TransactionDate and z.pop = a.pop
					 LEFT JOIN (
							SELECT a.TransactionDate,''NonDosh'' AS Pop,SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', 
							SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit PIN Transactions'', COUNT(DISTINCT B.ArgusPermID) AS ''Dosh Debit PIN Shoppers''
							FROM  ( SELECT * 
								   FROM Dosh.dbo.Trans_Debit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
								   )a
							inner join Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped b
							ON a.AccountID = b.DepositAccountID
							AND a.bankId = b.BankID
							where OnlineFlag IN ' + @OnlineFlag + 
									' and  MerchantLookupType = ''pin'' 
							and a.BankID in (400,410,411,421,424,429,439,440,479,489)
							group by a.TransactionDate) b
					  ON z.TransactionDate = b.TransactionDate and z.pop =b.pop 
					 
					 

					'
					)

					SET @Month = CASE 
					WHEN Right(@Month, 2) = 12
					THEN @Month + 89
					ELSE @Month + 1
					END
			END
			
			EXEC ('
			CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + ' (pop,transactiondate)
			ALTER TABLE Dosh.dbo.Debit_'+@Merchant+'_' + @competitor + '_Aggregate_Prospect_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
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
			   AND PROC_NAME = ''USP_DR_METRIC_CUSTOM_PROSPECT''');
END