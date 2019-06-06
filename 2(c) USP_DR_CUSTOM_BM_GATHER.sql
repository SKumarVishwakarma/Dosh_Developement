

USE [Dosh]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_DR_CUSTOM_BM_GATHER] @CustList VARCHAR(50)
AS
/*******************************************
    ** FILE: Gather Debit Merchant Industry 
    ** NAME: USP_DR_CUSTOM_BM_GATHER
    ** DESC: Gather Debit Merchant Industry basis Group of Selective Merchants (For e.g. SamsClubGroup)
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 13/03/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** DATE:        AUTHOR:             DESCRIPTION:
	22/02/2019      SHASHI              
    **************************************************/
SET NOCOUNT ON

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)

SET @lv_Status = ''

SELECT @lv_Status = [STATUS]
FROM DBO.Performance_Monitoring
WHERE PERIOD = @CustList
	AND PROC_NAME = 'USP_DR_CUSTOM_BM_GATHER'

IF @lv_Status <> 'C'
BEGIN
	EXEC (
			'DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_DR_CUSTOM_BM_GATHER'' AND PERIOD=' + @CustList + ';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT ' + @CustList + ', ''USP_DR_CUSTOM_BM_GATHER'', CURRENT_TIMESTAMP, NULL,''I''
		   
		   '
			);

	DECLARE @Month VARCHAR(MAX)
	DECLARE @Competitor VARCHAR(MAX)
	DECLARE @Merchant VARCHAR(50)
	DECLARE @MerchantList VARCHAR(max)
	DECLARE @version VARCHAR(50)
	DECLARE @onlineFlag VARCHAR(10)
	DECLARE @ReportStart VARCHAR(MAX)
	DECLARE @ReportEnd VARCHAR(MAX)

	SET @ReportStart = LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6) --Report Start Date 1 Year Prior Running Date CustList
	SET @ReportEnd = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6) --Report End Date 1 Month Before Running Date CustList
	SET @version = left(datename(MM, dateadd(mm, - 1, @CustList)), 3) + right(left(@CustList, 4), 2) + 'TPS'

	DECLARE CustomBMCursor CURSOR
	FOR
	SELECT DISTINCT Competitor   --Shashi Changes on 29/04/2019 Added DISTINCT 
		,CompetitorMerchantList
	FROM Dosh.dbo.AllDoshMerchantListNew
	WHERE CompetitorDebitSicCode = ''
		AND Competitor <> ''
		AND Is_Active = 1


	OPEN CustomBMCursor

	FETCH NEXT
	FROM CustomBMCursor
	INTO @Competitor
		,@MerchantList

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC (
				'

			IF EXISTS
                (
                SELECT * 
                FROM Dosh.INFORMATION_SCHEMA.Tables
                WHERE Table_Name = ''Debit_Industry_' + @Competitor + '_' + @version + '''
                )
                
				DROP TABLE Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '

				CREATE TABLE Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '(
				Merchant VARCHAR(50),
				TransactionDate DATE,
				POP VARCHAR(50),
				 [Dosh Debit SIG Spend] NUMERIC(38,4),
				  [Dosh Debit PIN Spend] NUMERIC(38,4),
				[Dosh Debit SIG Transactions] NUMERIC(38,4),
				 [Dosh Debit PIN Transactions] NUMERIC(38,2))

					IF EXISTS
                (
                SELECT * 
                FROM Dosh.INFORMATION_SCHEMA.Tables
                WHERE Table_Name = ''Debit_Industry_' + @Competitor + '_' + @version + '_Step1''
                )
                
				DROP TABLE Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '_Step1
			
				CREATE TABLE Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '_Step1(
				Merchant VARCHAR(50),
				TransactionDate DATE,
				POP VARCHAR(50),
				 [Dosh Debit SIG Spend] NUMERIC(38,4),
				  [Dosh Debit PIN Spend] NUMERIC(38,4),
				[Dosh Debit SIG Transactions] NUMERIC(38,4),
				 [Dosh Debit PIN Transactions] NUMERIC(38,2))
				')
		
		--Joseph R K changes on 23/04/2019 start
		/*
		EXEC (
				'DECLARE CustomBMMerchant CURSOR FOR
                SELECT Merchant FROM Dosh.dbo.AllDoshMerchantListNew
                where  Is_Active=1 and MerchantIndexNumber IN  ' + @MerchantList + ''
				) --,'15','20','50','69','92','93','94','95','96','97','98')
		*/
		EXEC (
				'DECLARE CustomBMMerchant CURSOR FOR
                SELECT distinct Merchant FROM Dosh.dbo.AllDoshMerchantListNew
                where  Is_Active=1 and MerchantIndexNumber IN  ' + @MerchantList + ''
				) --,'15','20','50','69','92','93','94','95','96','97','98')
		--Joseph R K changes on 23/04/2019 end				
				

		OPEN CustomBMMerchant

		FETCH NEXT
		FROM CustomBMMerchant
		INTO @Merchant

		WHILE @@FETCH_STATUS = 0
		BEGIN
			EXEC (
					'

				INSERT INTO Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '_Step1
				SELECT ''' + @Competitor + ''' AS Merchant
				,TransactionDate,POP,
				 [Dosh Debit SIG Spend], 
				 [Dosh Debit PIN Spend],
				[Dosh Debit SIG Transactions],
				[Dosh Debit PIN Transactions]
				FROM Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @version + '

				'
				)

			FETCH NEXT
			FROM CustomBMMerchant
			INTO @Merchant
		END

		CLOSE CustomBMMerchant

		DEALLOCATE CustomBMMerchant
		
		EXEC ('



		insert into
		Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '
		select merchant,transactiondate,pop,
		sum([Dosh Debit SIG Spend]) as [Dosh Debit SIG Spend],
		sum([Dosh Debit PIN Spend]) as [Dosh Debit PIN Spend],
		sum([Dosh Debit SIG Transactions]) as [Dosh Debit SIG Transactions],
		sum([Dosh Debit PIN Transactions]) as [Dosh Debit PIN Transactions]
		from
		Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + '_Step1
		group by merchant,transactiondate,pop ')
		

		--Added on 24-05-2019 start
		EXEC('

		ALTER TABLE Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
		')
		--Added on 24-05-2019 end

		FETCH NEXT
		FROM CustomBMCursor
		INTO @Competitor
			,@MerchantList
	END --Added here to Include the Scope Of Cursor

	CLOSE CustomBMCursor

	DEALLOCATE CustomBMCursor

	--Log the End Time
	EXEC (
			' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = ' + @CustList + '
           AND PROC_NAME = ''USP_DR_CUSTOM_BM_GATHER'''
			);
END

