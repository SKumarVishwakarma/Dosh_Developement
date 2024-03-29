USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_MERCH_CREDIT_TRANS_GATHER]    Script Date: 03/08/2019 16:28:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_MERCH_CREDIT_TRANS_GATHER] @CustList NVARCHAR(50) --PeriodID on which Procedure Need to be Executed
	/*******************************************
    ** FILE: Step 3 Trans Gather --> Credit TPS EMT Gather Merchant
    ** NAME: USP_MERCH_CREDIT_TRANS_GATHER
    ** DESC: THIS STORED PROCEDURE GATHER THE MERCHANT CREDIT TRANSACTIONS (Across All MERCHANTS) 
    **
    ** AUTH: SHASHIKUMAR VISHWAKARMA
    ** DATE: 22/02/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** Comment from Prachi on 22/02/2019 -->As discussed the start month in case of merchant gather will be according to the campaign start date for each  merchant. The End month will be 
	as per the 12 months logic. Please adjust the where condition (WHERE LEFT(CONVERT (VARCHAR(10),(DATEADD(Day,-120,CampaignStartDate)) ,112),6) ='+@Month+' ) as per logic.
	Added logic in where condition to gather Txns Prior 1 Year Campaign Date till the the Campaign Date a.TransactionDate between DATEADD(year,-1,x.CampaignStartDate) and CampaignStartDate
    **************************************************
	** changes Yilun Made to test Sams Code **
	** 3 table add  version and the stored proc is  version**
	** Change the date start 3 months back to correspond to current period **
	** print instead of exec the stored procedure 
    **************************************************
	** changes by Joseph to handle multiple rows in [dbo].AllDoshMerchantListnew for a merchant - 23/04/2019**	
	**/
AS
SET NOCOUNT ON

BEGIN
Declare @lv_Status varchar(2);
SET @lv_Status =''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_MERCH_CREDIT_TRANS_GATHER'
				
				IF @lv_Status<>'C'
					Begin

    --Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_MERCH_CREDIT_TRANS_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_MERCH_CREDIT_TRANS_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');

	DECLARE @Month VARCHAR(MAX)
	DECLARE @PrevMonth VARCHAR(MAX)
	DECLARE @SICCODE VARCHAR(50)
	DECLARE @Merchant VARCHAR(50)
	DECLARE @indexnumber VARCHAR(4)
	DECLARE @DB VARCHAR(20)
	DECLARE @Version VARCHAR(50)
	DECLARE @Lag VARCHAR(MAX)
	DECLARE @ReportStartDt VARCHAR(MAX)
	DECLARE @ReportEndDt VARCHAR(MAX)
	DECLARE @Xref VARCHAR(MAX)	

	SET @Lag = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 3, @CustList)), 112), 6)
	SET @version = left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	SET @ReportStartDt = left((select Report_Start_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)
    --SET @Month = '201703'
	SET @Month = '201702'	--changed on Yilun's request on 30-04-2019 to cater to new merchant requirement
	SET @ReportEndDt=left((select Report_END_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)
	
	WHILE @Month <= @ReportEndDt
	BEGIN /*Beginning of YEAR Block */
		SET @PrevMonth = CASE WHEN RIGHT(@Month,2)-1 < 1 THEN @Month - 89 ELSE @Month - 1 END 
		IF @PrevMonth > @Lag
		 BEGIN SET @PrevMonth= @Lag END
		SET @DB = CASE WHEN LEFT(@MONTH,4) = 2017 THEN CASE WHEN RIGHT(@MONTH,2) <=06 THEN '2017'
											      WHEN RIGHT(@Month,2)> 06 THEN '2017_2'
											      END
			   WHEN LEFT(@MONTH,4) > 2017 THEN RIGHT(@Month, 2) --- REPLACED "= 2018" WITH "> 2017"
			   END
			   
			   SET @Xref = CASE WHEN LEFT(@MONTH,4) = 2017 THEN 'CCPS_2017'
											      ELSE 'CCPS'   END

		EXEC (
				'
/*Dosh*/
USE  TPS_Trans_' + @Db + '

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Credit_Dosh_' + @MONTH + '_' + @version + '''
	)

DROP TABLE Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + '


SELECT  top 0 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + ' 

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber    
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON B.accountid = A.accountid
   AND B.bankid = A.bankid
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + '
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + 
				''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
--Joseph R K changes on 23/04/2019 start
--INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
INNER JOIN (select distinct MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
--Joseph R K changes on 23/04/2019 end
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber      
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + 
				'_Open_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON B.accountid = A.accountid
   AND B.bankid = A.bankid
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, - 6, CampaignStartDate),112),6))		
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,InternetPhoneIdentifier , MerchantCity, MerchantState


CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + ' (MerchantIndexNumber,accountid,ArgusPermId,BAnkid,transactionDate)
ALTER TABLE Dosh.dbo.Trans_Credit_Dosh_' + @MONTH + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

')

		EXEC (
				'

USE  TPS_Trans_' + @Db + '

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '''
	)

DROP TABLE Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '


SELECT  top 0 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber          
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON B.accountid = A.accountid
   AND B.bankid = A.bankid
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
--Joseph R K changes on 23/04/2019 start
--INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
INNER JOIN (select distinct MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
--Joseph R K changes on 23/04/2019 end
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber          
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + 
				'_Open_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON B.accountid = A.accountid
   AND B.bankid = A.bankid
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, - 6, CampaignStartDate),112),6))		
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,InternetPhoneIdentifier , MerchantCity, MerchantState


CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + ' (MerchantIndexNumber,accountid,ArgusPermId,BAnkid,transactionDate)
ALTER TABLE Dosh.dbo.Trans_Credit_NonDosh_' + @MONTH + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

')

		EXEC (
				'

--IndustryBM

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + '''
	)

drop table Dosh.dbo.Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + ' 

SELECT TOP 0
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + 
				''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + ' 
FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber 
	INNER JOIN 
	 (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '    
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A   
	  on a.ArgusMerchantID=c.ArgusMerchantID
	inner join 
       (SELECT *
	      FROM Dosh.dbo.Credit_Open_Industry_' + @PrevMonth + 
				' B   
		 WHERE bankid NOT IN (246,234)
	   ) B
	on a.bankid=b.bankid and a.accountid=b.AccountID
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + ' 
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @MONTH + 
				''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,''Null'' AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
--Joseph R K changes on 23/04/2019 start
--INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
INNER JOIN (select distinct MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
--Joseph R K changes on 23/04/2019 end
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber 
	INNER JOIN 
	 (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.ccps_' + @MONTH + '    
	       WHERE TransactionProfitCode IN (1,5,6)
	   ) A   
	  on a.ArgusMerchantID=c.ArgusMerchantID
	inner join 
       (SELECT *
	      FROM Dosh.dbo.Credit_Open_Industry_' + @PrevMonth + 
				' B   
		 WHERE bankid NOT IN (246,234)
	   ) B
	on a.bankid=b.bankid and a.accountid=b.AccountID
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, - 6, CampaignStartDate),112),6))		
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,InternetPhoneIdentifier , MerchantCity, MerchantState



CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + '  (MerchantIndexNumber,accountid,BAnkid,transactionDate)
ALTER TABLE Dosh.dbo.Trans_Credit_IndustryBM_' + @MONTH + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

')

		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END

	END /*End of YEAR */

  --Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_MERCH_CREDIT_TRANS_GATHER''');
	
	END
END /*End of Procedure Begin */