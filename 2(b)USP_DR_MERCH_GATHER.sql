USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_DR_MERCH_GATHER]    Script Date: 03/08/2019 16:26:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_DR_MERCH_GATHER] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 2.b Gather Debit Merchant
    ** NAME: USP_DR_MERCH_GATHER
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
    **************************************************/
SET NOCOUNT ON

DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_DR_MERCH_GATHER'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_DR_MERCH_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_DR_MERCH_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR(MAX)
DECLARE @NextMonth VARCHAR(8)
DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantDesc VARCHAR(400)
DECLARE @MerchantIndexNumber VARCHAR(10)
DECLARE @version VARCHAR(50)
DECLARE @onlineFlag VARCHAR(10)
DECLARE @SicCode VARCHAR(500)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList

SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantDebitSicCode AS SicCode
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantDebitSicCode AS SicCode
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @merchant
	,@OnlineFlag
	,@MerchantIndexNumber
	,@SicCode

WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC (
			'
	
	IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Debit_Industry_' + @Merchant + '_' + @version + '''
	)

DROP TABLE Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @version + '


CREATE TABLE Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @version + '(
Merchant VARCHAR(50),
TransactionDate DATE,
POP VARCHAR(50),
[Dosh Debit SIG Spend] NUMERIC(38,4),
[Dosh Debit PIN Spend] NUMERIC(38,4),
[Dosh Debit SIG Transactions] NUMERIC(38,2),
[Dosh Debit PIN Transactions] NUMERIC(38,2))

'
			)

	SET @Month =  @ReportStart
	WHILE @Month <= @ReportEnd
	BEGIN
		SET @NextMonth = CASE 
				WHEN RIGHT(@MONTH, 2) <> 12
					THEN (@MONTH + 01)
				ELSE (@MONTH + 89)
				END

		EXEC (
				'
				INSERT INTO Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @version + '
				SELECT ''' + @Merchant + ''' AS Merchant, CAST(a.TransactionDate AS DATE) AS Transactiondate, a.pop AS Pop, ABS([Dosh Debit SIG Spend]) AS [Dosh Debit SIG Spend], 
				ABS([Dosh Debit PIN Spend]) AS [Dosh Debit PIN Spend],[Dosh Debit SIG Transactions], [Dosh Debit PIN Transactions]
				FROM (
				SELECT a.TransactionDate, ''Industry'' AS Pop, SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit SIG Spend'', SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit SIG Transactions''
				FROM (
				SELECT * FROM Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @Version + '
				where OnlineFlag IN ' + @OnlineFlag + ' and SicCode IN ' + @SicCOde + ' AND MerchantIndexNumber = ' + @MerchantINdexNumber + 
								' and MerchantLookupType = ''SIG'' )a
				WHERE a.BankID in (400,410,411,421,424,429,439,440,479,489) 
				group by a.TransactionDate ) a
				LEFT JOIN
				(
				SELECT a.TransactionDate, ''Industry'' AS Pop, SUM(PositiveSpend+NegativeSpend) AS ''Dosh Debit PIN Spend'', SUM(PositiveTrans+NegativeTrans) AS ''Dosh Debit PIN Transactions''
				FROM (
				SELECT * FROM Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @Version + '
				where OnlineFlag IN ' + @OnlineFlag + ' and SicCode IN ' + @SicCOde + ' AND MerchantIndexNumber = ' + @MerchantINdexNumber + ' and MerchantLookupType = ''PIN'' )a
				WHERE a.BankID in (400,410,411,421,424,429,439,440,479,489) 
				group by a.TransactionDate ) b
				ON a.TransactionDate = b.TransactionDate
				--order by 1 

'
				)

		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END
	END
	
	--Added on 24-05-2019 start
	EXEC('

		ALTER TABLE Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
		')
	--Added on 24-05-2019 end
	
	FETCH NEXT
	FROM MerchantCursor
	INTO @merchant
		,@OnlineFlag
		,@MerchantIndexNumber
		,@SicCode
END
	CLOSE MerchantCursor

	DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_DR_MERCH_GATHER''');

END