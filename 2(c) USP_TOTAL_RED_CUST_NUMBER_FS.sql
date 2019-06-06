USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_TOTAL_RED_CUST_NUMBER_FS] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: TotalRedeemedCustomer Frequency Segment 
    ** NAME: USP_TOTAL_RED_CUST_NUMBER_FS
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 19/03/2019
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
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_TOTAL_RED_CUST_NUMBER_FS'
				
				IF @lv_Status<>'C' 
					Begin
						
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_TOTAL_RED_CUST_NUMBER_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_TOTAL_RED_CUST_NUMBER_FS'', CURRENT_TIMESTAMP, NULL,''I''');

-- LastMonday will be lsat monday of previous month. For ex - When refreshing in February, the last monday will be 20190128.

DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantIndexNumber VARCHAR(10)
DECLARE @MerchantCreditSicCode VARCHAR(500)
DECLARE @MerchantDebitSicCode VARCHAR(500)
DECLARE @version VARCHAR(50)
DECLARE @onlineFlag VARCHAR(10)
DECLARE @CampaignStartDate VARCHAR(12)
DECLARE @Campaign_Zip_Flag VARCHAR(20)
DECLARE @DoshMerchantName VARCHAR(MAX)
DECLARE @lastMonday VARCHAR(15)
DECLARE @ReportStart VARCHAR (MAX)
DECLARE @ReportEnd VARCHAR (MAX)


--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR
FOR
SELECT Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantCreditSicCode
	,MerchantDebitSicCode
	,CampaignStartDate
	,Campaign_Zip_Flag
	,DoshMerchantName
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 
		and Competitor<>'' and CampaignStartDate is not NULL   ----AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> ''   and MerchantIndexNumber=92 Removed to Include All Merchants Including Custom Merchants
*/
DECLARE MerchantCursor CURSOR
FOR
SELECT distinct Merchant
	,OnlineFlag
	,MerchantIndexNumber
	,MerchantCreditSicCode
	,MerchantDebitSicCode
	,CampaignStartDate
	,Campaign_Zip_Flag
	,DoshMerchantName
FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 
and Competitor<>'' and CampaignStartDate is not NULL   ----AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> ''   and MerchantIndexNumber=92 Removed to Include All Merchants Including Custom Merchants
--Joseph R K changes on 23/04/2019 end		
			

OPEN MerchantCursor

FETCH NEXT
FROM MerchantCursor
INTO @merchant
	,@OnlineFlag
	,@MerchantIndexNumber
	,@MerchantCreditSicCode
	,@MerchantDebitSicCode
	,@CampaignStartDate
	,@Campaign_Zip_Flag
	,@DoshMerchantName

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
	--Shashi Changes on 06/05/2019 start 
	--replace the ‘@lastMonday’ parameter with the Monday that covers the report end date
	--SET @lastMonday=CONVERT(VARCHAR(10), DATEADD(dd, - (DATEDIFF(dd, 0, dbo.udf_GetLastDayOfMonth(DateAdd(mm, -1, @CustList )))%7), dbo.udf_GetLastDayOfMonth(DateAdd(mm, -1, @CustList ))),112) */
	SET @lastMonday=CONVERT(VARCHAR,DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, @CustList), @CustList)), 0),112)
	SET @ReportStart=LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)   --Report Start Date 1 Year Prior Running Date CustList
	SET @ReportEnd=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, -1, @CustList)), 112), 6)  --Report End Date 1 Month Before Running Date CustList
	--Shashi Changes on 06/05/2019 end

	EXEC (
			'

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''RedeemCustomers_FS_' + @Merchant + '_' + @Version + '''
	)

DROP TABLE Dosh.dbo.RedeemCustomers_FS_' + @Merchant + '_' + @Version + '


SELECT ''Linked With Transactions'' AS POP,PreFrequency,ISNULL(Campaign_zip_Flag,1) AS Campaign_zip_Flag , COUNT(DISTINCT c.ArgusPermId) AS RedeemFlag
INTO Dosh.dbo.RedeemCustomers_FS_' + @Merchant + '_' + @Version + '
FROM (SELECT DISTINCT dosh_customer_key AS Dosh_Customer_key
		FROM Dosh_Raw.dbo.Raw_customer_transactions_' + @lastMonday + '
		--SHASHI CHANGES on 31/05/2019 as Part of Changes requested by Yilun for merchants Dennys
		where merchant_name LIKE ' + @DoshMerchantName + '   --where merchant_name IN ' + @DoshMerchantName + ' 
		--Shashi Changes on 06/05/2019 start
		--Restrict the transaction date between the campaign start and report end 
		and cast(transaction_date as date)>=left(CONVERT(VARCHAR(10), cast('''+@CampaignStartDate+''' as date), 112),8) 
		and left(CONVERT(VARCHAR(10), cast(transaction_date as Date), 112) , 6)<= '+@ReportEnd+'		
		--Shashi Changes on 06/05/2019 end
		)a
		INNER JOIN Dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + ' b
On a.dosh_customer_key = b.Dosh_Customer_key
INNER JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' d
ON b.dosh_Customer_Key = d.Dosh_Customer_key
INNER JOIN Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' c
ON CAST(b.ArguspermID AS VARCHAR(MAX))= CAST(c.ArguspermID AS VARCHAR(MAX))
Group by PreFrequency , ISNULL(Campaign_zip_Flag,1)
--order by 1,2		


IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''TotalRedeemCustomers_FS_' + @Merchant + '_' + @Version + '''
	)

DROP TABLE Dosh.dbo.TotalRedeemCustomers_FS_' + @Merchant + '_' + @Version + '


SELECT a.pop,a.PreFrequency,ISNULL(a.Campaign_zip_Flag,1) AS Campaign_zip_Flag ,(COUNT(DISTINCT arguspermid))* (PostScaled /PreScaled) AS TotalCustomers , RedeemFlag
INTO Dosh.dbo.TotalRedeemCustomers_FS_' + @Merchant + '_' + @Version + '
FROM Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' a
LEFT JOIN Dosh.dbo.TotalCustomerScalingFactor_' + @CustList + ' b
ON a.pop = b.Pop
LEFT JOIN Dosh.dbo.RedeemCustomers_FS_' + @Merchant + '_' + @Version + ' c
ON a.Pop = c.Pop
and a.PreFrequency = c.PreFrequency
and ISNULL(a.Campaign_zip_Flag,1) = ISNULL(c.Campaign_zip_Flag,1)
Group by a.PreFrequency,a.pop ,PreScaled,PostScaled, c.RedeemFlag , ISNULL(a.Campaign_zip_Flag,1)
--order by 1,2
'
)

--Added on 24-05-2019 start
EXEC ('
ALTER TABLE Dosh.dbo.TotalRedeemCustomers_FS_' + @Merchant + '_' + @Version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
--Added on 24-05-2019 end

	FETCH NEXT
	FROM MerchantCursor
	INTO @merchant
		,@OnlineFlag
		,@MerchantIndexNumber
		,@MerchantCreditSicCode
		,@MerchantDebitSicCode
		,@CampaignStartDate
		,@Campaign_Zip_Flag
		,@DoshMerchantName
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor


--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_TOTAL_RED_CUST_NUMBER_FS''');
           
END           