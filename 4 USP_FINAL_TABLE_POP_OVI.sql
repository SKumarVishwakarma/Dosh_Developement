USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_FINAL_TABLE_POP_OVI]    Script Date: 03/08/2019 16:28:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_FINAL_TABLE_POP_OVI] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 4. Final Scaled Table for Overall Industry
    ** NAME: USP_FINAL_TABLE_POP_OVI
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
	22/02/2019		PRACHI				I have added the INTO statement where the naming convention is as per the final deliverable of CSV file.
										Final tabke :Dosh.dbo.'+@Merchant+'_Industry_'+@CustList+'
    26/02/2019		PRACHI				Changing the columns names to meet the final client requirement for naming convention										
										
    **************************************************/
SET NOCOUNT ON
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
where PERIOD=@CustList and PROC_NAME='USP_FINAL_TABLE_POP_OVI'

IF @lv_Status<>'C' 
	Begin
--Log the Start Time
	
	EXEC ('DELETE FROM Dosh.DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_FINAL_TABLE_POP_OVI'' AND PERIOD='+@CustList+';
		   INSERT INTO Dosh.DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_FINAL_TABLE_POP_OVI'', CURRENT_TIMESTAMP, NULL,''I''');


DECLARE @Version VARCHAR(20)
DECLARE @Merchant VARCHAR(100)
DECLARE @MerchantIndexNumber INT
DECLARE @Competitor VARCHAR(100)

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	  ,MerchantIndexNumber
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' 
      --Added to Filter Merchants for Custom Industry
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	  ,MerchantIndexNumber
	  ,Competitor	--newly added
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' --Added to Filter Merchants for Custom Industry
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @Merchant
	,@MerchantIndexNumber
	,@Competitor	--Joseph R K changes on 23/04/2019

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

	--Joseph R K changes on 23/04/2019 start
	/*
	EXEC (
			'
IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''' + @Merchant + '_Industry_' + @CustList + '''
	)

DROP TABLE Dosh.dbo.' + @Merchant + '_Industry_' + @CustList + '


SELECT 
a.transactiondate as transaction_date,
''' + @merchant + ''' as merchant,
cast(a.pop as varchar(100)) as [population],   --Cast Varchar(100) has been Added as requested by Yilun
CAST(([merchant credit spend]/c.creditspend) AS NUMERIC(38,2)) as merchant_credit_spend,
CAST(([industry credit spend]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_spend,

CAST(([merchant credit transactions]/c.creditspend) AS NUMERIC(38,2)) as merchant_credit_transaction,
CAST(([industry credit transactions]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_transaction,

case when [industry credit spend] =0 then 0 else ([merchant credit spend]*d.creditspend)/(c.creditspend *[Industry Credit Spend]) end as credit_Spend_Share,
CAST((case when [merchant credit transactions] =0 then 0 else [merchant credit spend]/[merchant credit Transactions] end) AS NUMERIC(38,2)) as Merchant_Credit_Ticket_Size,
CAST((case when [industry credit transactions] =0 then 0 else [industry credit spend]/[industry credit Transactions] end) AS NUMERIC(38,2)) as Industry_Credit_Ticket_Size,

CAST(([merchant debit sig spend]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_sig_spend,
CAST(([merchant debit pin spend]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_pin_spend,
CAST(([merchant debit sig transactions]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_sig_transactions,
CAST(([merchant debit pin transactions]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_pin_transactions,

CAST(([industry debit sig spend]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_sig_spend,
CAST(([industry debit pin spend]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_pin_spend,
CAST(([industry debit sig transactions]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_sig_transactions,
CAST(([industry debit pin transactions]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_pin_transactions,


case when [Industry debit sig Spend] =0 then 0 else ([Merchant debit sig Spend]*d.debitspend)/(c.debitspend *[Industry debit sig spend]) end as debit_sig_spend_share,
case when [Industry debit pin Spend] =0 then 0 else ([Merchant debit pin Spend]*d.debitspend)/(c.debitspend *[Industry debit pin spend]) end as debit_pin_spend_share,
CAST((case when [Merchant debit sig Transactions] =0 then 0 else [Merchant debit sig Spend]/[Merchant debit sig Transactions] end) AS NUMERIC(38,2)) as merchant_debit_sig_ticket_size,
CAST((case when [Merchant debit pin Transactions] =0 then 0 else [Merchant debit pin Spend]/[Merchant debit pin Transactions] end) AS NUMERIC(38,2)) as merchant_debit_pin_ticket_size,
CAST((case when [Industry debit sig Transactions] =0 then 0 else [Industry debit sig Spend]/[Industry debit sig Transactions] end) AS NUMERIC(38,2)) as industry_debit_sig_ticket_size,
CAST((case when [Industry debit pin Transactions] =0 then 0 else [Industry debit pin Spend]/[Industry debit pin Transactions] end) AS NUMERIC(38,2)) as industry_debit_pin_ticket_size,
Industry_Id,
Industry
INTO Dosh.dbo.' + @Merchant + '_Industry_' + @CustList + '
FROM 
DOSH.dbo.Raw_Data_Output_' + @Merchant + '_OverallIndustry_' + @Version + ' a
LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
			WHERE merchant in (''Merchant'')
		  ) c
ON a.Pop = c.Pop

LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
			WHERE merchant in (''Competitor'')
		  ) d
ON a.Pop = d.Pop

--order by 1

'
			)
*/

EXEC (
	'
	IF EXISTS
		(
		SELECT * 
		FROM Dosh.INFORMATION_SCHEMA.Tables
		WHERE Table_Name = ''' + @Merchant + '_' + @Competitor + '_Industry_' + @CustList + '''
		)

	DROP TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Industry_' + @CustList + '


	SELECT 
	a.transactiondate as transaction_date,
	''' + @merchant + ''' as merchant,
	cast(a.pop as varchar(100)) as [population],   --Cast Varchar(100) has been Added as requested by Yilun
	CAST(([merchant credit spend]/c.creditspend) AS NUMERIC(38,2)) as merchant_credit_spend,
	CAST(([industry credit spend]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_spend,

	CAST(([merchant credit transactions]/c.creditspend) AS NUMERIC(38,2)) as merchant_credit_transaction,
	CAST(([industry credit transactions]/d.creditspend) AS NUMERIC(38,2)) as industry_credit_transaction,

	case when [industry credit spend] =0 then 0 else ([merchant credit spend]*d.creditspend)/(c.creditspend *[Industry Credit Spend]) end as credit_Spend_Share,
	CAST((case when [merchant credit transactions] =0 then 0 else [merchant credit spend]/[merchant credit Transactions] end) AS NUMERIC(38,2)) as Merchant_Credit_Ticket_Size,
	CAST((case when [industry credit transactions] =0 then 0 else [industry credit spend]/[industry credit Transactions] end) AS NUMERIC(38,2)) as Industry_Credit_Ticket_Size,

	CAST(([merchant debit sig spend]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_sig_spend,
	CAST(([merchant debit pin spend]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_pin_spend,
	CAST(([merchant debit sig transactions]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_sig_transactions,
	CAST(([merchant debit pin transactions]/c.debitspend) AS NUMERIC(38,2)) as merchant_debit_pin_transactions,

	CAST(([industry debit sig spend]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_sig_spend,
	CAST(([industry debit pin spend]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_pin_spend,
	CAST(([industry debit sig transactions]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_sig_transactions,
	CAST(([industry debit pin transactions]/d.debitspend) AS NUMERIC(38,2)) as industry_debit_pin_transactions,


	case when [Industry debit sig Spend] =0 then 0 else ([Merchant debit sig Spend]*d.debitspend)/(c.debitspend *[Industry debit sig spend]) end as debit_sig_spend_share,
	case when [Industry debit pin Spend] =0 then 0 else ([Merchant debit pin Spend]*d.debitspend)/(c.debitspend *[Industry debit pin spend]) end as debit_pin_spend_share,
	CAST((case when [Merchant debit sig Transactions] =0 then 0 else [Merchant debit sig Spend]/[Merchant debit sig Transactions] end) AS NUMERIC(38,2)) as merchant_debit_sig_ticket_size,
	CAST((case when [Merchant debit pin Transactions] =0 then 0 else [Merchant debit pin Spend]/[Merchant debit pin Transactions] end) AS NUMERIC(38,2)) as merchant_debit_pin_ticket_size,
	CAST((case when [Industry debit sig Transactions] =0 then 0 else [Industry debit sig Spend]/[Industry debit sig Transactions] end) AS NUMERIC(38,2)) as industry_debit_sig_ticket_size,
	CAST((case when [Industry debit pin Transactions] =0 then 0 else [Industry debit pin Spend]/[Industry debit pin Transactions] end) AS NUMERIC(38,2)) as industry_debit_pin_ticket_size,
	Industry_Id,
	Industry
	INTO Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Industry_' + @CustList + '
	FROM 
	DOSH.dbo.Raw_Data_Output_' + @Merchant + '_' + @Competitor + '_OverallIndustry_' + @Version + ' a
	LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
				WHERE merchant in (''Merchant'')
			  ) c
	ON a.Pop = c.Pop

	LEFT JOIN ( SELECT * FROM Dosh.dbo.Dosh_ScalingFactor_' + @Merchant + '  
				WHERE merchant in (''Competitor'')
			  ) d
	ON a.Pop = d.Pop
	--order by 1

	'
	)
	--Joseph R K changes on 23/04/2019 end
	
	--Added on 24-05-2019 start
	EXEC('	
	ALTER TABLE Dosh.dbo.' + @Merchant + '_' + @Competitor + '_Industry_' + @CustList + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	')
	--Added on 24-05-2019 end
	
	FETCH NEXT
	FROM MerchantCursor
	INTO @Merchant
		,@MerchantIndexNumber
		,@Competitor	--Joseph R K changes on 23/04/2019
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_FINAL_TABLE_POP_OVI''');
   
   EXEC (' UPDATE Dosh.DBO.Execution_Period_Detail_DEV SET IND_IS_PROCESSED=''Y'' WHERE  PeriodId = '+@CustList+'');           
           
END           