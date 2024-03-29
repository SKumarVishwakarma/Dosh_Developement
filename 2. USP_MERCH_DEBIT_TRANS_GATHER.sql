USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_MERCH_DEBIT_TRANS_GATHER]    Script Date: 03/08/2019 16:28:30 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_MERCH_DEBIT_TRANS_GATHER] @CustList NVARCHAR(50) --PeriodID on which Procedure Need to be Executed
	/*******************************************
    ** FILE: Step 3 Trans Gather --> Debit TPS EMT Gather Merchant
    ** NAME: USP_MERCH_DEBIT_TRANS_GATHER
    ** DESC: THIS STORED PROCEDURE GATHER THE MERCHANT DEBIT TRANSACTIONS (Across All MERCHANTS) 
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
    **************************************************/

AS
SET NOCOUNT ON

BEGIN	
Declare @lv_Status varchar(2);
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_MERCH_DEBIT_TRANS_GATHER'
				
				IF @lv_Status<>'C' 
					Begin
	
EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_MERCH_DEBIT_TRANS_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_MERCH_DEBIT_TRANS_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');	
		   
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
		 SET @DB = CASE WHEN LEFT(@MONTH,4) = 2017 THEN CASE WHEN RIGHT(@MONTH,2) <=06 THEN 'DAPS_2017'
											      WHEN RIGHT(@Month,2)> 06 THEN 'DAPS_2017_2'
											      END
			   WHEN LEFT(@MONTH,4) > 2017 THEN RIGHT(@Month, 2) --- REPLACED "= 2018" WITH "> 2017"
			   END
			   
			   SET @Xref = CASE WHEN LEFT(@MONTH,4) = 2017 THEN 'DAPS_2017'
											      ELSE 'DAPS'   END

		EXEC (
				'
/*Dosh*/


USE  TPS_Trans_' + @Db + '

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_Dosh_' + @Month + '_' + @version + '''
	)

DROP TABLE Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + '  


SELECT  top 0 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + '   

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber          
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month + 
				'
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON b.DepositAccountID = a.AccountID
   AND B.bankid = A.bankid
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, -6, CampaignStartDate),112),6))
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + '  
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month + 
				''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
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
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month + 
				'
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON b.DepositAccountID = a.AccountID
   AND B.bankid = A.bankid
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, -6, CampaignStartDate),112),6))
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState
CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + '  (MerchantIndexNumber,accountid,ArgusPermId,transactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_Dosh_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
'
)

EXEC  ('
USE  TPS_Trans_' + @Db + '
IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_NonDosh_' + @Month + '_' + @version + '''
	)

DROP TABLE Dosh.dbo.Trans_Debit_NonDosh_' + @Month + '_' + @version + '  


SELECT  top 0 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Debit_NonDosh_' + @Month + '_' + @version + '   

FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber          
 INNER JOIN 
       (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month + 
				'
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Non_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON b.DepositAccountID = a.AccountID
   AND B.bankid = A.bankid
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Debit_NonDosh_' + @Month + '_' + @version + '  
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode,ArgusPermId
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
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
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month +'
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON C.ArgusMerchantID = A.ArgusMerchantId
 INNER JOIN 
       (SELECT *
	      FROM DOSH.dbo.YG_Non_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
    ON b.DepositAccountID = a.AccountID
   AND B.bankid = A.bankid
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, -6, CampaignStartDate),112),6))
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,ArgusPermId,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState
CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Debit_NonDosh_' + @Month + '_' + @version + '  (MerchantIndexNumber,accountid,ArgusPermId,transactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_NonDosh_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

')

		EXEC  (
				'

--IndustryBM

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_IndustryBM_' + @Month + '_' + @version + '''
	)

drop table Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @version + '  

SELECT TOP 0
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month +''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @version + '
FROM dosh.dbo.Dosh_EMT_XRef_'+@Xref+' C
INNER JOIN (select MerchantIndexNumber,Merchant, CampaignStartDate from Dosh.dbo.AllDoshMerchantListNew
	where Is_Active=1) x
	ON c.IndexNumber = x.MerchantIndexNumber 
	INNER JOIN 
	 (SELECT *
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month + 
				'    
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A   
	  on a.ArgusMerchantID=c.ArgusMerchantID
	inner join 
       (SELECT *
	      FROM Dosh.dbo.Debit_Open_Industry_' + @PrevMonth + ' B   
		 WHERE bankid NOT IN (246,234)
	   ) B
	on a.bankid=b.bankid and a.accountid=b.DepositAccountID
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @version + ' 
SELECT 
x.Merchant AS Merchant,
 x.MerchantIndexNumber AS MerchantIndexNumber,
a.accountid as Accountid
,''' + @Month + ''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
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
	      FROM TPS_Trans_' + @Db + '.dbo.daps_' + @Month + 
				'    
	       WHERE TransactionProfitCode IN (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A   
	  on a.ArgusMerchantID=c.ArgusMerchantID
	inner join 
       (SELECT *
	      FROM Dosh.dbo.Debit_Open_Industry_' + @PrevMonth + ' B   
		 WHERE bankid NOT IN (246,234)
	   ) B
	on a.bankid=b.bankid and a.accountid=b.DepositAccountID
WHERE ('+@Month+' >= '+@ReportStartDt+' OR 
        '+@Month+' >= LEFT(CONVERT(VARCHAR(10), DATEADD(MM, -6, CampaignStartDate),112),6))
Group by  Merchant, MerchantIndexNumber,a.BAnkid,a.accountid,a.TransactionDate,a.SicCode,MerchantLookupType,InternetPhoneIdentifier , MerchantCity, MerchantState


CREATE CLUSTERED INDEX IDX ON Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @version + '  (MerchantIndexNumber,accountid,transactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_IndustryBM_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

')


		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END

	END /*End of YEAR */
	
  --Log the End Time	  
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_MERCH_DEBIT_TRANS_GATHER''')	
	END
END /*End of Procedure Begin */

