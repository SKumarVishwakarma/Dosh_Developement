USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_IND_DEBIT_TRANS_GATHER]    Script Date: 03/08/2019 16:28:18 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_IND_DEBIT_TRANS_GATHER] @CustList VARCHAR(50) --PeriodID on which Procedure Need to be Executed
	/*******************************************
    ** FILE: Step 3 Trans Gather --> Debit TPS EMT Gather Competitor Industry
    ** NAME: USP_IND_DEBIT_TRANS_GATHER
    ** DESC: THIS STORED PROCEDURE GATHER THE INDUSTRY DEBIT TRANSACTIONS (Across All Competitor) FOR DOSH, NON DOSH and DEBIT OPEN INDUSTRY
    **
    ** AUTH: SHASHIKUMAR VISHWAKARMA
    ** DATE: 22/02/2019
    **
    ***********************************************
    **       CHANGE HISTORY
    ***********************************************
    ** CREATED WITH LATEST FILES GIVEN ON 22/02/2019 BY PRACHI PANDYA, JUST ADDED DISTINCT IN CURSOR SO IT WILL FETCH ONLY UNIQUE INDUSTRY
    ** 02/26/2019 : PRACHI - REMOVING THE CURSOR AND GATHERING ALL THE MERCHANT CODES IN ONE TABLE, WHICH CAN BE SEGGREGATED LATER USING THE ALLDOSHMERCHANTLISTNEW LOOKUP TABLE	
    **************************************************/
AS
SET NOCOUNT ON
	Declare @lv_Status varchar(2);
	SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_IND_DEBIT_TRANS_GATHER'
				
				IF @lv_Status<>'C'
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_IND_DEBIT_TRANS_GATHER'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_IND_DEBIT_TRANS_GATHER'', CURRENT_TIMESTAMP, NULL,''I''');

BEGIN
	DECLARE @Month VARCHAR(MAX)
	DECLARE @PrevMonth VARCHAR(MAX)
	DECLARE @SICCODE VARCHAR(50)
	DECLARE @Merchant VARCHAR(50)
	DECLARE @indexnumber VARCHAR(4)
	DECLARE @DB VARCHAR(2)
	DECLARE @Version VARCHAR(50)
	DECLARE @Lag VARCHAR(MAX)
	DECLARE @ReportStartDt VARCHAR(MAX)
	DECLARE @ReportEndDt VARCHAR(MAX)		

	SET @Lag = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 3, @CustList)), 112), 6)
	SET @version = left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS' 
	SET @ReportStartDt = left((select Report_Start_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)
    SET @Month = @ReportStartDt
	SET @ReportEndDt=left((select Report_END_Dt from dosh.dbo.Execution_Period_Detail_DEV where PeriodID=@CustList),6)

	WHILE @Month <= @ReportEndDt --This will be @Month+12, First Month passed taking all Year 
	BEGIN /*Beginning of YEAR Block */
		SET @PrevMonth = CASE WHEN RIGHT(@Month,2)-1 < 1 THEN @Month - 89 ELSE @Month - 1 END 
		IF @PrevMonth > @Lag
		 BEGIN SET @PrevMonth= @Lag END
		SET @DB = RIGHT(@Month, 2)

			EXEC (
					'
USE  TPS_Trans_' + @Db + '

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '''
	)

/*dosh*/
drop table 
Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '  


SELECT TOp 0 
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
INTO Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '       
FROM (SELECT *
	      FROM Dosh.dbo.YG_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
 INNER JOIN 
       (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + ' 
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON B.DepositAccountid =A.accountid 
   AND B.bankid = A.bankid
Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,ArgusPermId,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '       
SELECT 
a.accountid as Accountid
,''' + @Month +''' AS Period
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
FROM (SELECT *
	      FROM Dosh.dbo.YG_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
 INNER JOIN 
       (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + ' 
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON B.DepositAccountid =A.accountid 
   AND B.bankid = A.bankid
Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,ArgusPermId,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState

CREATE NONCLUSTERED INDEX IDX1 ON Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '  (SicCode)
CREATE CLUSTERED INDEX IDX2 ON Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '  (AccountID,Arguspermid,TransactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_Dosh_Industry_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

'	)

			EXEC (
					'

/*nondosh*/

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '''
	)

drop table 
Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '  


SELECT TOp 0 
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
INTO Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '       
FROM (SELECT *
	      FROM Dosh.dbo.YG_Non_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
 INNER JOIN 
       (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + ' 
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON B.DepositAccountid =A.accountid 
   AND B.bankid = A.bankid
Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,ArgusPermId,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState


INSERT INTO Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '       
SELECT 
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
FROM (SELECT *
	      FROM Dosh.dbo.YG_Non_Dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped
		 WHERE bankid NOT IN (246,234)
	   ) B
 INNER JOIN 
       (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + ' 
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A
    ON B.DepositAccountid =A.accountid 
   AND B.bankid = A.bankid
Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,ArgusPermId,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState


CREATE NONCLUSTERED INDEX IDX1 ON Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '  (SicCode)
CREATE CLUSTERED INDEX IDX2 ON Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '  (AccountID,Arguspermid,TransactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_NonDosh_Industry_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

'
					)

			EXEC (
					'

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '''
	)


drop table Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '  


SELECT  TOP 0 
a.accountid as Accountid
,''' + @Month + 
					''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,''Null'' AS arguspermid
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
INTO Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '    
FROM 
	   (SELECT *
	      FROM Dosh.dbo.Debit_Open_Industry_' + @PrevMonth + ' B   
	   ) B
	INNER JOIN 
	 (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + '    
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A   
	on a.bankid=b.bankid and  B.DepositAccountid =A.accountid 

Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState



insert into
Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '    
SELECT 
a.accountid as Accountid
,''' + @Month + 
					''' AS Period
,a.BankId
,a.TransactionDate
,siccode
,''Null'' AS arguspermid
,CASE WHEN MerchantLookupType = 3 THEN ''PIN'' WHEN MerchantLookupType = 4 THEN ''SIG'' ELSE NULL END AS MerchantLookupType
,SUM(CASE WHEN transactionAmount > 0.00 THEN TransactionAmount  ELSE 0.00 END) As PositiveSpend
,SUM(CASE WHEN transactionAmount < 0.00 THEN TransactionAmount  ELSE 0.00 END) As NegativeSpend
,SUM(CASE WHEN transactionAmount > 0.00 THEN 1.0  ELSE 0.00 END) As PositiveTrans
,SUM(CASE WHEN transactionAmount < 0.00 THEN 1.0  ELSE 0.00 END) As NegativeTrans
,InternetPhoneIdentifier AS OnlineFlag
,MerchantCity
,MerchantState
FROM 
	   (SELECT *
	      FROM Dosh.dbo.Debit_Open_Industry_' + @PrevMonth + ' B   
	   ) B
	INNER JOIN 
	 (SELECT AccountID, BankId, TransactionDate, SicCode, MerchantLookupType, TransactionAmount, InternetPhoneIdentifier,MerchantCity,MerchantState
	      FROM TPS_Trans_' + @Db + '.dbo.DAPS_' + @Month + '    
		 WHERE MerchantLookupType in (3,4)
	       AND TransactionProfitCode in (600, 601,  602,  604,  605,  606,  608,  609,  610,  612,    614,  615)
	   ) A   
	on a.bankid=b.bankid and  B.DepositAccountid =A.accountid 

Group by  a.BAnkid,a.accountid,a.TransactionDate,siccode,MerchantLookupType, InternetPhoneIdentifier , MerchantCity, MerchantState



CREATE NONCLUSTERED INDEX IDX1 ON Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '  (SicCode)
CREATE CLUSTERED INDEX IDX2 ON Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '  (AccountID,Arguspermid,TransactionDate,MerchantLookupType)
ALTER TABLE Dosh.dbo.Trans_Debit_IndustryBM_Industry_' + @Month + '_' + @version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
'
					)


		SET @Month = CASE 
				WHEN Right(@Month, 2) = 12
					THEN @Month + 89
				ELSE @Month + 1
				END
	END /*End of YEAR */
	
	  --Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_IND_DEBIT_TRANS_GATHER''');
END           
END /*End of Procedure Begin */

