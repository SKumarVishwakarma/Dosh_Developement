USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_CR_CUSTOM_BM_VOD] @CustList NVARCHAR(50) --PeriodID on which Procedure Need to be Executed
	/*******************************************
    ** FILE: 
    ** NAME: USP_DR_CUSTOM_BM_VOD
    ** DESC: 
    **
    ** AUTH: SHASHIKUMAR VISHWAKARMA
    ** DATE: 13/03/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** 
    **************************************************/
AS
SET NOCOUNT ON

BEGIN
Declare @lv_Status varchar(2);
SET @lv_Status =''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_CR_CUSTOM_BM_VOD'
				
				IF @lv_Status<>'C'
					Begin

    --Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CR_CUSTOM_BM_VOD'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_CR_CUSTOM_BM_VOD'', CURRENT_TIMESTAMP, NULL,''I''');
	
	
	
	DECLARE @Month VARCHAR(MAX)
	DECLARE @DB VARCHAR(20)
	DECLARE @Version VARCHAR(50)
	DECLARE @Lag VARCHAR(MAX)
	DECLARE @ReportStartDt VARCHAR(MAX)
	DECLARE @ReportEndDt VARCHAR(MAX)
	DECLARE @Xref VARCHAR(MAX)
	DECLARE @Competitor VARCHAR(MAX)
	DECLARE @MerchantList VARCHAR (MAX)
	DECLARE @MerchantIndexNumber varchar(max)
	DECLARE @MerchantCreditSicCode varchar(max)

	SET @Lag = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 3, @CustList)), 112), 6)
	SET @version = left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	SET @ReportStartDt = left((select Report_Start_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)
    SET @ReportEndDt=left((select Report_END_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)
	
	DECLARE CustomBMCursor CURSOR
	FOR
	SELECT DISTINCT Competitor   --Shashi Changes on 29/04/2019 Added DISTINCT 
		,CompetitorMerchantList
	FROM Dosh.dbo.AllDoshMerchantListNew
	WHERE CompetitorCreditSicCode = ''
		AND Competitor <> ''
		AND Is_Active = 1
                

OPEN CustomBMCursor

FETCH NEXT FROM CustomBMCursor INTO @Competitor , @MerchantList


WHILE @@FETCH_STATUS=0

BEGIN
	
SET @Month = @ReportStartDt
WHILE @Month <= @ReportEndDt
BEGIN

		   
	SET @Xref = CASE WHEN LEFT(@MONTH,4) = 2017 THEN 'CCPS_2017'
											  ELSE 'CCPS'   END

EXEC (
	'
	/*Dosh Custom Trans_Industry*/


	IF EXISTS
		(
		SELECT * 
		FROM Dosh.INFORMATION_SCHEMA.Tables
		WHERE Table_Name = ''Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + ''' 	--''Trans_Credit_Dosh_' + @MONTH + '_' + @version + ''' Need to be Custom 
		)

	DROP TABLE Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '


	SELECT  top 0 
	''' + @Competitor + ''' AS ''Merchant''
	,MerchantIndexNumber
	,Accountid
	,Period
	,BankId
	,TransactionDate
	,siccode
	,ArgusPermId
	,MerchantLookupType
	,PositiveSpend
	,NegativeSpend
	,PositiveTrans
	,NegativeTrans
	,OnlineFlag
	,MerchantCity
	,MerchantState
	INTO Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
	From Dosh.dbo.Trans_Credit_Dosh_'+@Month+'_' + @version + '     

	IF EXISTS
		(
		SELECT * 
		FROM Dosh.INFORMATION_SCHEMA.Tables
		WHERE Table_Name = ''Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + ''' 	--''Trans_Credit_Dosh_' + @MONTH + '_' + @version + ''' Need to be Custom 
		)

	DROP TABLE Dosh.dbo.Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '


	SELECT  top 0 
	''' + @Competitor + ''' AS ''Merchant''
	,MerchantIndexNumber
	,Accountid
	,Period
	,BankId
	,TransactionDate
	,siccode
	,ArgusPermId
	,MerchantLookupType
	,PositiveSpend
	,NegativeSpend
	,PositiveTrans
	,NegativeTrans
	,OnlineFlag
	,MerchantCity
	,MerchantState
	INTO Dosh.dbo.Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
	From Dosh.dbo.Trans_Credit_NonDosh_'+@Month+'_' + @version + '     


	')


--Joseph R K changes on 23/04/2019 start
/*
EXEC ('DECLARE CustomBMMerchant CURSOR FOR
                select MerchantIndexNumber,MerchantCreditSicCode from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1 and MerchantIndexNumber in '+@MerchantList+' ') --CompetitorCreditSicCode in ('''') To pick the Merchant Index List to Pass all the Merchants with Their respective Sic Codes
*/
EXEC ('DECLARE CustomBMMerchant CURSOR FOR
		select distinct MerchantIndexNumber,MerchantCreditSicCode from Dosh.dbo.AllDoshMerchantListNew
		where Is_Active=1 and MerchantIndexNumber in '+@MerchantList+' ') --CompetitorCreditSicCode in ('''') To pick the Merchant Index List to Pass all the Merchants with Their respective Sic Codes	
--Joseph R K changes on 23/04/2019 end


OPEN CustomBMMerchant

FETCH NEXT FROM CustomBMMerchant INTO @MerchantIndexNumber,@MerchantCreditSicCode

WHILE @@FETCH_STATUS=0

BEGIN

EXEC ('
INSERT INTO Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
SELECT 
''' + @Competitor + '''
,MerchantIndexNumber
,Accountid
,Period
,BankId
,TransactionDate
,siccode
,ArgusPermId
,MerchantLookupType
,PositiveSpend
,NegativeSpend
,PositiveTrans
,NegativeTrans
,OnlineFlag
,MerchantCity
,MerchantState
from Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + '
where MerchantIndexNumber='+@MerchantIndexNumber+' and siccode in '+@MerchantCreditSicCode+'


INSERT INTO Dosh.dbo.Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + '
SELECT 
''' + @Competitor + '''
,MerchantIndexNumber
,Accountid
,Period
,BankId
,TransactionDate
,siccode
,ArgusPermId
,MerchantLookupType
,PositiveSpend
,NegativeSpend
,PositiveTrans
,NegativeTrans
,OnlineFlag
,MerchantCity
,MerchantState
from Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '
where MerchantIndexNumber='+@MerchantIndexNumber+' and siccode in '+@MerchantCreditSicCode+'


')

--Added on 24-05-2019 start
EXEC('

ALTER TABLE Dosh.dbo.Trans_Credit_Dosh_' + @Competitor + '_' + @MONTH + '_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')

EXEC('

ALTER TABLE Dosh.dbo.Trans_Credit_NonDosh_' + @Competitor + '_' + @MONTH + '_' + @version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
--Added on 24-05-2019 end


FETCH NEXT FROM CustomBMMerchant INTO @MerchantIndexNumber,@MerchantCreditSicCode
								
END

CLOSE CustomBMMerchant

DEALLOCATE CustomBMMerchant

SET @Month = CASE 
	WHEN Right(@Month, 2) = 12
		THEN @Month + 89
	ELSE @Month + 1
	END

END 


FETCH NEXT FROM CustomBMCursor INTO @Competitor , @MerchantList
END    --Added here to Include the Scope Of Cursor
CLOSE CustomBMCursor
DEALLOCATE CustomBMCursor
			
		
		
--Log the End Time
   
EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
	   AND PROC_NAME = ''USP_CR_CUSTOM_BM_VOD''');

END
END /*End of Procedure Begin */