USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_CR_MERCH_GATHER]    Script Date: 03/08/2019 16:26:45 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[USP_CR_MERCH_GATHER] @CustList VARCHAR(50)
	
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 1.b Gather Credit Merchant
    ** NAME: USP_CR_MERCH_GATHER
    ** DESC: Gather Credit Merchant Trans
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

--Log the Start Time
	DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_CR_MERCH_GATHER'
				
				IF @lv_Status<>'C' 
					Begin
					
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_CR_MERCH_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_CR_MERCH_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR (MAX)
DECLARE @NextMonth VARCHAR (8)
Declare @Merchant      VarChar(50)
Declare @MerchantDesc  VarChar(400)
Declare @MerchantIndexNumber  VARCHAR(10)
Declare @version VARCHAR(50)
Declare @onlineFlag VARCHAR (10)
DEclare @SicCode VARCHAR (500)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)

SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList

SET @version = left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS' 


--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR FOR	
	SELECT Merchant,OnlineFlag, MerchantIndexNumber,MerchantCreditSicCode AS SicCode
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1
*/
DECLARE MerchantCursor CURSOR FOR	
	SELECT distinct Merchant,OnlineFlag, MerchantIndexNumber,MerchantCreditSicCode AS SicCode
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @SicCode

WHILE @@FETCH_STATUS=0

BEGIN
	EXEC('

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Credit_Industry_'+@Merchant+'_'+@version+'''
	)
	
DROP TABLE Dosh.dbo.Credit_Industry_'+@Merchant+'_'+@version+'

CREATE TABLE Dosh.dbo.Credit_Industry_'+@Merchant+'_'+@version+'(
Merchant VARCHAR(50),
TransactionDate DATE,
POP VARCHAR(50),
[Dosh Credit Spend] NUMERIC(38,4),
[Dosh Credit Transactions] NUMERIC(38,2))
')

SET @Month =  @ReportStart
WHILE @Month <= @ReportEnd 

BEGIN
SET @NextMonth = CASE WHEN RIGHT(@MONTH,2) <> 12 THEN (@MONTH+01)
		ELSE (@MONTH+89)
		END 

		
		EXEC('

INSERT INTO Dosh.dbo.Credit_Industry_'+@Merchant+'_'+@version+'
SELECT '''+@Merchant+''' AS Merchant, CAST(a.TransactionDate AS DATE) AS TransactionDate, ''Industry'' AS Pop, SUM(PositiveSpend+NegativeSpend) AS ''Dosh Credit Spend'', 
SUM(PositiveTrans+NegativeTrans) AS ''Dosh Credit Transactions''
FROM Dosh.dbo.Trans_Credit_IndustryBM_'+@Month+'_'+@version+' a
where MerchantIndexNumber = '+@MerchantIndexNumber+' AND SicCode IN '+@SicCode+' 
AND OnlineFlag IN '+@OnlineFlag+' and a.BankID in (10, 12, 16, 17, 24, 31, 37, 41, 47, 64, 67, 69, 71, 75, 78, 79, 80, 110, 127, 187, 240)
group by CAST(a.TransactionDate AS DATE)

')
		

		SET @Month = 
case 
     when Right(@Month, 2) = 12 then @Month + 89 
	 else @Month + 1 END

End

--Added on 24-05-2019 start
EXEC('

ALTER TABLE Dosh.dbo.Credit_Industry_'+@Merchant+'_'+@version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
--Added on 24-05-2019 end

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @SicCode
    
END
CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_CR_MERCH_GATHER''');
           
END           