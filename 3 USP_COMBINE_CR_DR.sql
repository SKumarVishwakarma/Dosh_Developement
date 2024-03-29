USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_COMBINE_CR_DR]    Script Date: 03/08/2019 16:25:34 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_COMBINE_CR_DR] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 3. Combining Credit and debit metrics -Overall Industry
    ** NAME: USP_COMBINE_CR_DR
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
	22/02/2019		PRACHI				Changed the PP_industrySegment to IndustrySegment
    **************************************************/
SET NOCOUNT ON
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_COMBINE_CR_DR'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_COMBINE_CR_DR'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_COMBINE_CR_DR'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR(10)
DECLARE @Version VARCHAR(20)
DECLARE @StartDate VARCHAR(10)
DECLARE @EndDate VARCHAR(10)
DECLARE @Merchant VARCHAR(100)
DECLARE @MerchantIndexNumber INT
DECLARE @Industry_ID VARCHAR(500)
DECLARE @Competitor VARCHAR(100)

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	,MerchantIndexNumber
	,Industry_ID
	,Competitor
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>''   --Added to Handle the Custom Industry
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,MerchantIndexNumber
	,Industry_ID
	,Competitor
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>''   --Added to Handle the Custom Industry
--Joseph R K changes on 23/04/2019 end


OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @Merchant
	,@MerchantIndexNumber
	,@Industry_ID
	,@Competitor

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	
	-- the start and date should be 12 months of data from the refresh date
	SET @StartDate=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
	SET @EndDate=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList


--Joseph R K changes on 23/04/2019 start	
/*
	EXEC (
			'
USE Dosh

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Raw_Data_Output_' + @Merchant + '_OverallIndustry_' + @Version + '''
	)


DROP TABLE dosh.dbo.Raw_Data_Output_' + @Merchant + '_OverallIndustry_' + @Version + 
			'


SELECT a. TransactionDate, a.Pop
,[Merchant Credit Spend], [Industry Credit Spend]
,[Merchant Credit Transactions], [Industry Credit Transactions]
,'''' AS ''Market Share''
,'''' AS ''Merchant Credit Ticket Size''
,'''' AS ''Industry Credit Ticket Size''
,[Merchant Debit SIG Spend], [Merchant Debit PIN Spend]
,[Industry Debit SIG Spend], [Industry Debit PIN Spend]
, [Merchant Debit SIG Transactions], [Merchant Debit PIN Transactions]
, [Industry Debit SIG Transactions], [Industry Debit PIN Transactions]
,'''' AS ''Debit SIG Market Share''
,'''' AS ''Debit PIN Market Share''
,'''' AS ''Merchant Debit SIG Ticket Size''
,'''' AS ''Merchant Debit PIN Ticket Size''
,'''' AS ''Industry Debit SIG Ticket Size''
,'''' AS ''Industry Debit PIN Ticket Size''
,''' + @Industry_Id + '''  AS Industry_Id
,''' + @Competitor + ''' AS Industry

INTO Dosh.dbo.Raw_Data_Output_' + @Merchant + '_OverallIndustry_' + @Version + 
			'
FROM Dosh.dbo.IndustrySegment a

LEFT JOIN (SELECT TransactionDate, pop, [DOsh Credit Spend] AS ''Merchant Credit Spend'', [Dosh Credit transactions] AS ''Merchant Credit Transactions''
		   FROM Dosh.dbo.Credit_Industry_' + @Merchant + '_' + @Version + ') b
ON a.transactionDate = b.TransactionDate

LEFT JOIN (SELECT TransactionDate, pop, [DOsh Credit Spend] AS ''Industry Credit Spend'', [Dosh Credit transactions] AS ''Industry Credit Transactions''
		   FROM Dosh.dbo.Credit_Industry_' + @Competitor + '_' + @Version + ') c
ON a.transactionDate = c.TransactionDate		   

LEFT JOIN (SELECT TransactionDate, pop, [Dosh Debit SIG Spend] AS ''Merchant Debit SIG Spend'', [Dosh Debit PIN Spend] AS ''Merchant Debit PIN Spend''
			      , [Dosh Debit SIG Transactions] AS ''Merchant Debit SIG Transactions'' , [Dosh Debit PIN Transactions] AS ''Merchant Debit PIN Transactions''
		   FROM Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @Version + 
			') d
ON a.transactionDate = d.TransactionDate	

LEFT JOIN (SELECT TransactionDate, pop, [Dosh Debit SIG Spend] AS ''Industry Debit SIG Spend'', [Dosh Debit PIN Spend] AS ''Industry Debit PIN Spend''
			      , [Dosh Debit SIG Transactions] AS ''Industry Debit SIG Transactions'' , [Dosh Debit PIN Transactions] AS ''Industry Debit PIN Transactions''
		   FROM Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @Version + ') e
ON a.transactionDate = e.TransactionDate	
where left(CONVERT(VARCHAR(10), a.TransactionDate, 112),6) BETWEEN ''' + @StartDate + ''' AND ''' + @EndDate + '''  --changes to keep consistency of the derived start date and end date 
'
			)
*/

EXEC (
	'
	USE Dosh

	IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Raw_Data_Output_' + @Merchant + '_' + @Competitor + '_OverallIndustry_' + @Version + '''
	)


	DROP TABLE dosh.dbo.Raw_Data_Output_' + @Merchant + '_' + @Competitor + '_OverallIndustry_' + @Version + '


	SELECT a. TransactionDate, a.Pop
	,[Merchant Credit Spend], [Industry Credit Spend]
	,[Merchant Credit Transactions], [Industry Credit Transactions]
	,'''' AS ''Market Share''
	,'''' AS ''Merchant Credit Ticket Size''
	,'''' AS ''Industry Credit Ticket Size''
	,[Merchant Debit SIG Spend], [Merchant Debit PIN Spend]
	,[Industry Debit SIG Spend], [Industry Debit PIN Spend]
	, [Merchant Debit SIG Transactions], [Merchant Debit PIN Transactions]
	, [Industry Debit SIG Transactions], [Industry Debit PIN Transactions]
	,'''' AS ''Debit SIG Market Share''
	,'''' AS ''Debit PIN Market Share''
	,'''' AS ''Merchant Debit SIG Ticket Size''
	,'''' AS ''Merchant Debit PIN Ticket Size''
	,'''' AS ''Industry Debit SIG Ticket Size''
	,'''' AS ''Industry Debit PIN Ticket Size''
	,''' + @Industry_Id + '''  AS Industry_Id
	,''' + @Competitor + ''' AS Industry

	INTO Dosh.dbo.Raw_Data_Output_' + @Merchant + '_' + @Competitor + '_OverallIndustry_' + @Version +
	'
	FROM Dosh.dbo.IndustrySegment a

	LEFT JOIN (SELECT TransactionDate, pop, [DOsh Credit Spend] AS ''Merchant Credit Spend'', [Dosh Credit transactions] AS ''Merchant Credit Transactions''
			   FROM Dosh.dbo.Credit_Industry_' + @Merchant + '_' + @Version + ') b
	ON a.transactionDate = b.TransactionDate

	LEFT JOIN (SELECT TransactionDate, pop, [DOsh Credit Spend] AS ''Industry Credit Spend'', [Dosh Credit transactions] AS ''Industry Credit Transactions''
			   FROM Dosh.dbo.Credit_Industry_' + @Competitor + '_' + @Version + ') c
	ON a.transactionDate = c.TransactionDate		   

	LEFT JOIN (SELECT TransactionDate, pop, [Dosh Debit SIG Spend] AS ''Merchant Debit SIG Spend'', [Dosh Debit PIN Spend] AS ''Merchant Debit PIN Spend''
					  , [Dosh Debit SIG Transactions] AS ''Merchant Debit SIG Transactions'' , [Dosh Debit PIN Transactions] AS ''Merchant Debit PIN Transactions''
			   FROM Dosh.dbo.Debit_Industry_' + @Merchant + '_' + @Version + 
				') d
	ON a.transactionDate = d.TransactionDate	

	LEFT JOIN (SELECT TransactionDate, pop, [Dosh Debit SIG Spend] AS ''Industry Debit SIG Spend'', [Dosh Debit PIN Spend] AS ''Industry Debit PIN Spend''
					  , [Dosh Debit SIG Transactions] AS ''Industry Debit SIG Transactions'' , [Dosh Debit PIN Transactions] AS ''Industry Debit PIN Transactions''
			   FROM Dosh.dbo.Debit_Industry_' + @Competitor + '_' + @Version + ') e
	ON a.transactionDate = e.TransactionDate	
	where left(CONVERT(VARCHAR(10), a.TransactionDate, 112),6) BETWEEN ''' + @StartDate + ''' AND ''' + @EndDate + '''  --changes to keep consistency of the derived start date and end date 
	'
	)
	--Joseph R K changes on 23/04/2019 end
	
	
	--Added on 24-05-2019 start
	EXEC('
	ALTER TABLE dosh.dbo.Raw_Data_Output_' + @Merchant + '_' + @Competitor + '_OverallIndustry_' + @Version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
		')
	--Added on 24-05-2019 end

	FETCH NEXT
	FROM MerchantCursor
	INTO @Merchant
		,@MerchantIndexNumber
		,@Industry_ID
		,@Competitor
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_COMBINE_CR_DR''');
           
END           