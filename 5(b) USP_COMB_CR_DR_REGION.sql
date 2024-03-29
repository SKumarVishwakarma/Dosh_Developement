USE [Dosh]
GO
/****** Object:  StoredProcedure [dbo].[USP_COMB_CR_DR_REGION]    Script Date: 03/08/2019 16:25:29 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[USP_COMB_CR_DR_REGION] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 5.b Combined Credit and Debit Metrics for Regional Campaign
    ** NAME: USP_COMB_CR_DR_REGION
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


										
	22/02/2019		SHASHI				All the Parameters for Now Handled in the Procedure keeping CustList as HardCoded can be made dynamic
    **************************************************/
SET NOCOUNT ON

--Log the Start Time
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_COMB_CR_DR_REGION'
				
				IF @lv_Status<>'C' 
					Begin	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_COMB_CR_DR_REGION'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_COMB_CR_DR_REGION'', CURRENT_TIMESTAMP, NULL,''I''');

DECLARE @Month VARCHAR (10)
DECLARE @Version VARCHAR(20)
DECLARE @StartDate VARCHAR(10)
DECLARE @EndDate VARCHAR(10)
Declare @Merchant      VarChar(100)
DECLARE @MerchantIndexNumber INT
DECLARE @Industry_ID VARCHAR(500)
DECLARE @Competitor VARCHAR(100)

--Joseph R K changes on 23/04/2019 start
--here the distinct may not be required but kept as a safety net
/*
DECLARE MerchantCursor CURSOR FOR
	SELECT Merchant, MerchantIndexNumber, Industry_ID, Competitor
	FROM Dosh.dbo.AllDoshMerchantListNew 
	where Campaign_Zip_Flag<>'NationWide' and Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL --where MerchantIndexNumber IN (9) Removed and  MerchantIndexNumber=92     
*/	
DECLARE MerchantCursor CURSOR FOR
	SELECT distinct Merchant, MerchantIndexNumber, Industry_ID, Competitor
	FROM Dosh.dbo.AllDoshMerchantListNew 
	where Campaign_Zip_Flag<>'NationWide' and Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL --where MerchantIndexNumber IN (9) Removed and  MerchantIndexNumber=92 
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @Merchant, @MerchantIndexNumber, @Industry_ID, @Competitor

WHILE @@FETCH_STATUS=0
begin

SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'

-- the start and date should be 12 months of data from the refresh date
SET @StartDate = LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6)
SET @EndDate = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  

--Joseph R K changes on 23/04/2019 start
/*
EXEC ('

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = '''+@Merchant+'_Raw_Data_Output_'+@Version+'''
	)


DROP TABLE dosh.dbo.'+@Merchant+'_Raw_Data_Output_'+@Version+'



select a.TransactionDate, a.pop , a.Campaign_Zip_Flag ,
 a.Segment,
t.totalcustomers  as totalCustomer,
t.RedeemFlag AS RedeemedCustomers,
ISNULL(b.[dosh credit spend],0) as ''Merchant Credit Spend'',
ISNULL(d.[Dosh Credit Spend],0) as ''Industry Credit Spend'',
ISNULL(b.[Dosh Credit Transactions],0) as ''Merchant Credit Transaction'',
ISNULL(d.[Dosh Credit Transactions],0) as ''Industry Credit Transaction'',
ISNULL(b.[Dosh Credit Shoppers],0) as ''Merchant Credit Shoppers'',
ISNULL(d.[Dosh Credit Shoppers],0) as ''Industry Credit Shoppers'',
'''' AS ''Credit Spend Share'',
'''' as ''Merchant ticketsize'',
'''' as ''Industry ticketsize'',
ISNULL(c.[Dosh Debit SIG Spend],0) as ''Merchant Debit Sig Spend'',
ISNULL(c.[Dosh Debit PIN Spend],0) as ''Merchant Debit Pin Spend'',
ISNULL(e.[Dosh Debit SIG Spend],0) as ''Industry Debit Sig Spend'',
ISNULL(e.[Dosh Debit PIN Spend],0) as ''Industry Debit pin Spend'',
ISNULL(c.[Dosh Debit SIG Transactions],0) as ''Merchant Debit Sig Transactions'',
ISNULL(c.[Dosh Debit PIN Transactions],0) as ''Merchant Debit Pin Transactions'',
ISNULL(e.[Dosh Debit SIG Transactions],0) as ''Industry Debit Sig Transactions'',
ISNULL(e.[Dosh Debit PIN Transactions],0) as ''Industry Debit Pin Transactions'',
ISNULL(c.[Dosh Debit SIG Shoppers],0) as ''Merchant Debit SIG Shoppers'',
ISNULL(c.[Dosh Debit PIN Shoppers],0) as ''Merchant Debit PIN Shoppers'',
ISNULL(e.[Dosh Debit SIG Shoppers],0) as ''Industry Debit SIG Shoppers'',
ISNULL(e.[Dosh Debit PIN Shoppers],0) as ''Industry Debit PIN Shoppers'',
'''' AS ''Debit Sig Spend Share'',
'''' AS ''Debit Pin Spend Share'',
'''' as ''Merchant Debit Sig Ticket Size'',
'''' as ''Merchant Debit Pin Ticket Size'',
'''' as ''Industry Debit Sig Ticket Size'',
'''' as ''Industry Debit Pin Ticket Size'',
'''+@Competitor+''' AS Industry,
'''+@Industry_ID+''' AS Industry_Id

into dosh.dbo.'+@Merchant+'_Raw_Data_Output_'+@Version+'
from Dosh.dbo.RegionalActivitySegment a
LEFT JOIN Dosh.dbo.TotalRedeemCustomers_'+@Merchant+'_'+@Version+' t
ON a.Pop = t.POp 
and a.Segment = t.SegmentFlag
and a.Campaign_Zip_Flag = t.Campaign_Zip_Flag
left join Dosh.dbo.Credit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+' b
on a.transactiondate=b.transactiondate 
and a.segment=b.segmentflag 
and A.POP=B.POP
AND a.Campaign_Zip_Flag = b.Campaign_Zip_Flag
left join Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+' c
on a.transactiondate=c.transactiondate  
and a.Segment=c.segmentflag  
and A.POP=C.POP
AND a.Campaign_Zip_Flag = c.Campaign_Zip_Flag
left join Dosh.dbo.Credit_'+@Merchant+'_'+@Competitor+'_Aggregate_ActivitySegment_'+@Version+' d
on a.transactiondate=d.transactiondate 
and a.Segment=d.segmentflag 
and A.POP=d.POP
AND a.Campaign_Zip_Flag = d.Campaign_Zip_Flag
left join Dosh.dbo.Debit_'+@Merchant+'_'+@Competitor+'_Aggregate_ActivitySegment_'+@Version+' e
on a.transactiondate=e.transactiondate 
and a.Segment=e.segmentflag 
and A.POP=e.POP
AND a.Campaign_Zip_Flag = e.Campaign_Zip_Flag
where left(CONVERT(VARCHAR(10), cast(a.transactiondate as Date), 112) , 6) BETWEEN '''+@StartDate+'''  AND '''+@EndDate+'''

')
*/

EXEC ('

	IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = '''+@Merchant+'_'+@Competitor+'_Raw_Data_Output_'+@Version+'''
	)

	DROP TABLE dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Output_'+@Version+'

	
	select a.TransactionDate, a.pop , a.Campaign_Zip_Flag ,
	a.Segment,
	t.totalcustomers  as totalCustomer,
	t.RedeemFlag AS RedeemedCustomers,
	ISNULL(b.[dosh credit spend],0) as ''Merchant Credit Spend'',
	ISNULL(d.[Dosh Credit Spend],0) as ''Industry Credit Spend'',
	ISNULL(b.[Dosh Credit Transactions],0) as ''Merchant Credit Transaction'',
	ISNULL(d.[Dosh Credit Transactions],0) as ''Industry Credit Transaction'',
	ISNULL(b.[Dosh Credit Shoppers],0) as ''Merchant Credit Shoppers'',
	ISNULL(d.[Dosh Credit Shoppers],0) as ''Industry Credit Shoppers'',
	'''' AS ''Credit Spend Share'',
	'''' as ''Merchant ticketsize'',
	'''' as ''Industry ticketsize'',
	ISNULL(c.[Dosh Debit SIG Spend],0) as ''Merchant Debit Sig Spend'',
	ISNULL(c.[Dosh Debit PIN Spend],0) as ''Merchant Debit Pin Spend'',
	ISNULL(e.[Dosh Debit SIG Spend],0) as ''Industry Debit Sig Spend'',
	ISNULL(e.[Dosh Debit PIN Spend],0) as ''Industry Debit pin Spend'',
	ISNULL(c.[Dosh Debit SIG Transactions],0) as ''Merchant Debit Sig Transactions'',
	ISNULL(c.[Dosh Debit PIN Transactions],0) as ''Merchant Debit Pin Transactions'',
	ISNULL(e.[Dosh Debit SIG Transactions],0) as ''Industry Debit Sig Transactions'',
	ISNULL(e.[Dosh Debit PIN Transactions],0) as ''Industry Debit Pin Transactions'',
	ISNULL(c.[Dosh Debit SIG Shoppers],0) as ''Merchant Debit SIG Shoppers'',
	ISNULL(c.[Dosh Debit PIN Shoppers],0) as ''Merchant Debit PIN Shoppers'',
	ISNULL(e.[Dosh Debit SIG Shoppers],0) as ''Industry Debit SIG Shoppers'',
	ISNULL(e.[Dosh Debit PIN Shoppers],0) as ''Industry Debit PIN Shoppers'',
	'''' AS ''Debit Sig Spend Share'',
	'''' AS ''Debit Pin Spend Share'',
	'''' as ''Merchant Debit Sig Ticket Size'',
	'''' as ''Merchant Debit Pin Ticket Size'',
	'''' as ''Industry Debit Sig Ticket Size'',
	'''' as ''Industry Debit Pin Ticket Size'',
	'''+@Competitor+''' AS Industry,
	'''+@Industry_ID+''' AS Industry_Id

	into dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Output_'+@Version+'
	from Dosh.dbo.RegionalActivitySegment a
	LEFT JOIN Dosh.dbo.TotalRedeemCustomers_'+@Merchant+'_'+@Version+' t
	ON a.Pop = t.POp 
	and a.Segment = t.SegmentFlag
	and a.Campaign_Zip_Flag = t.Campaign_Zip_Flag
	left join Dosh.dbo.Credit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+' b
	on a.transactiondate=b.transactiondate 
	and a.segment=b.segmentflag 
	and A.POP=B.POP
	AND a.Campaign_Zip_Flag = b.Campaign_Zip_Flag
	left join Dosh.dbo.Debit_'+@Merchant+'_Aggregate_ActivitySegment_'+@Version+' c
	on a.transactiondate=c.transactiondate  
	and a.Segment=c.segmentflag  
	and A.POP=C.POP
	AND a.Campaign_Zip_Flag = c.Campaign_Zip_Flag
	left join Dosh.dbo.Credit_'+@Merchant+'_'+@Competitor+'_Aggregate_ActivitySegment_'+@Version+' d
	on a.transactiondate=d.transactiondate 
	and a.Segment=d.segmentflag 
	and A.POP=d.POP
	AND a.Campaign_Zip_Flag = d.Campaign_Zip_Flag
	left join Dosh.dbo.Debit_'+@Merchant+'_'+@Competitor+'_Aggregate_ActivitySegment_'+@Version+' e
	on a.transactiondate=e.transactiondate 
	and a.Segment=e.segmentflag 
	and A.POP=e.POP
	AND a.Campaign_Zip_Flag = e.Campaign_Zip_Flag
	where left(CONVERT(VARCHAR(10), cast(a.transactiondate as Date), 112) , 6) BETWEEN '''+@StartDate+'''  AND '''+@EndDate+'''

	')

--Joseph R K changes on 23/04/2019 end
	--Added on 24-05-2019 start
	EXEC('
	ALTER TABLE dosh.dbo.'+@Merchant+'_'+@Competitor+'_Raw_Data_Output_'+@Version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
	')
	--Added on 24-05-2019 end

FETCH NEXT FROM MerchantCursor INTO @Merchant, @MerchantIndexNumber, @Industry_ID, @Competitor
    
END 
CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_COMB_CR_DR_REGION''');
END