USE [Dosh]
GO
 /****** Object:  StoredProcedure [dbo].[USP_SEGMENT_ACTIVITY]    Script Date: 03/08/2019 16:28:42 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_SEGMENT_ACTIVITY]  @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************
    ** FILE: 2. Segment Activity flag
    ** NAME: USP_SEGMENT_ACTIVITY
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
	22/02/2019		SHASHI				All the Parameters for Now Handled in the Procedure keeping CustList as HardCoded can be made dynamic
    **************************************************/
SET NOCOUNT ON
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_SEGMENT_ACTIVITY'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_SEGMENT_ACTIVITY'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_SEGMENT_ACTIVITY'', CURRENT_TIMESTAMP, NULL,''I''');


/* Need to set the date range in the below code from Line 54- 58 and 2 more similar sets like below

SUM(CASE WHEN transactiondate between ''2018-11-02'' and ''2018-11-15'' THEN 1 ELSE 0 END) AS T0Segment,
SUM(CASE WHEN transactiondate between ''2018-10-17'' and ''2018-11-01'' THEN 1 ELSE 0 END) AS T14Segment,
SUM(CASE WHEN transactiondate between ''2018-09-17'' and ''2018-10-16'' THEN 1 ELSE 0 END) AS T30Segment,
SUM(CASE WHEN transactiondate between ''2018-08-18'' and ''2018-09-16'' THEN 1 ELSE 0 END) AS T60Segment,
SUM(CASE WHEN transactiondate between ''2018-07-19'' and ''2018-08-17'' THEN 1 ELSE 0 END) AS T90Segment
-- THis can be derived from the campaign date. The left side dates are all Campaign date - 120,90,60,30,14 days each. And the right side date is just 1 day
before the new segment starts.
*/

Declare @Merchant      VarChar(50)
Declare @MerchantDesc  VarChar(400)
Declare @MerchantIndexNumber  VARCHAR(10)
Declare @MerchantCreditSicCode VARCHAR (500)
Declare @MerchantDebitSicCode VARCHAR (500)
Declare @version VARCHAR(50)
Declare @onlineFlag VARCHAR (10)
Declare @CampaignStartDate VARCHAR(12)
Declare @startDate VARCHAR(12)
Declare @EndDate VARCHAR(12)
Declare @Campaign_Zip_Flag VARCHAR(20)
Declare @lastMonday VARCHAR(10)  --Shashi Changes on 28-05-2019 Added new Variable for Raw_Card and Raw_Transaction for Segment T120 Split

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR FOR
	SELECT Merchant,OnlineFlag, MerchantIndexNumber,MerchantCreditSicCode, MerchantDebitSicCode, CampaignStartDate , Campaign_Zip_Flag
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL   --	 Include all Merchants  
*/
DECLARE MerchantCursor CURSOR FOR
	SELECT distinct Merchant,OnlineFlag, MerchantIndexNumber,MerchantCreditSicCode, MerchantDebitSicCode, CampaignStartDate , Campaign_Zip_Flag
	FROM Dosh.dbo.AllDoshMerchantListNew where Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL   --	 Include all Merchants  
--Joseph R K changes on 23/04/2019 end

OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @MerchantCreditSicCode , @MerchantDebitSicCode ,@CampaignStartDate ,@Campaign_Zip_Flag

WHILE @@FETCH_STATUS=0
begin

SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'
SET @lastMonday=CONVERT(VARCHAR,DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, @CustList), @CustList)), 0),112)  --Shashi Changes on 28-05-2019 Added new Variable for Raw_Card and Raw_Transaction for Segment T120 Split


EXEC ('

IF EXISTS
	(
	SELECT * 
	FROM Dosh.INFORMATION_SCHEMA.Tables
	WHERE Table_Name = ''Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1''
	)

DROP TABLE Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1


CREATE TABLE Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1(
POP VARCHAR(30),
ARGUSPERMID int,
T0Segment Int,
T14Segment Int,
T30Segment Int,
T60Segment Int,
T90Segment INT)


---1 with trans
INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1
select ''Linked With Transactions'' as pop, a.ArguspermId 
--,CASE WHEN c.ArgusPermID is not null THEN 1 ELSE 0 END AS RedeemFlag
,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-14,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-1,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T0Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-30,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-15,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T14Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-60,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-31,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T30Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-90,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-61,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T60Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-120,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-91,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T90Segment

from 
(select arguspermid from
(
select arguspermid from Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped group by arguspermid union all
select arguspermid from Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped group by arguspermid) a
group by arguspermid) a

inner JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  p
On CAST(a.ArgusPermId AS VARCHAR(MAX)) = CAST(p.ArguspermID AS VARCHAR(MAX))
inner JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' l 
On p.dosh_customer_key = l.dosh_customer_key
left join 
(select arguspermid,segment,transactiondate from Dosh.dbo.CampaignShoppersActivity_'+@Merchant+'_'+@Version+' where segment =(''With Transaction'')) tag
on a.ArgusPermId=tag.arguspermid
where  LinkedTag = ''Linked'' and TransTag = ''WithTrans''
group by a.arguspermid 


-----2 without trans
INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1
select ''Linked Without Transactions'' as pop, a.ArguspermId 
--,CASE WHEN c.ArgusPermID is not null THEN 1 ELSE 0 END AS RedeemFlag
,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-14,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-1,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T0Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-30,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-15,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T14Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-60,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-31,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T30Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-90,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-61,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T60Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-120,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-91,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T90Segment

from 
(select arguspermid from
(
select arguspermid from Dosh.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped group by arguspermid union all
select arguspermid from Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped group by arguspermid) a
group by arguspermid) a

inner JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+'  p
On CAST(a.ArgusPermId AS VARCHAR(MAX)) = CAST(p.ArguspermID AS VARCHAR(MAX))
inner JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_'+@CustList+' l 
On p.dosh_customer_key = l.dosh_customer_key
left join 
(select arguspermid,segment,transactiondate from Dosh.dbo.CampaignShoppersActivity_'+@Merchant+'_'+@Version+' where segment =(''Without Transaction'')) tag
on a.ArgusPermId=tag.arguspermid
where  LinkedTag = ''Linked'' and TransTag = ''WithoutTrans''
group by a.arguspermid 



--3 NonDosh
insert into Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1
select ''NonDosh'' as pop, a.ArguspermId 
,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-14,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-1,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T0Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-30,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-15,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T14Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-60,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-31,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T30Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-90,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-61,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T60Segment,
SUM(CASE WHEN transactiondate between CAST(DATEADD(day,-120,'''+@CampaignStartDate+''') AS DATE) and CAST(DATEADD(day,-91,'''+@CampaignStartDate+''') AS DATE) THEN 1 ELSE 0 END) AS T90Segment
 
from 
(
select arguspermid from Dosh.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_'+@CustList+'_Open_Deduped group by arguspermid union all
select arguspermid from Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_'+@CustList+'_Deduped group by arguspermid) a

left join 
(select arguspermid,segment,transactiondate from Dosh.dbo.CampaignShoppersActivity_'+@Merchant+'_'+@Version+' where segment =(''NonDosh'')) tag
on a.ArgusPermId=tag.arguspermid
group by a.arguspermid 


				--*******************************************************
				
				--Shashi Changes on 28-05-2019 Customer Activity Segment for Segment T120 Split Starts
				
				IF EXISTS
					(
					SELECT * 
					FROM Dosh.INFORMATION_SCHEMA.Tables
					WHERE Table_Name = ''Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2''
					)

				DROP TABLE 
				Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2


				SELECT pop,a. arguspermid,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) AS Campaign_Zip_Flag, 
				CASE WHEN T0Segment <>0 THEN  ''T0''
					  WHEN T14Segment <>0 THEN ''T14''
					  WHEN T30Segment <>0 THEN ''T30''
					  WHEN T60Segment <>0 THEN ''T60''
					  WHEN T90Segment <> 0 THEN ''T90''
					  ELSE ''T120'' END AS SegmentFlag
				INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2			
				FROM Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step1 a
				LEFT JOIN (SELECT * FROM Dosh.dbo.ActiveCustomers_Dosh_'+@CustList+' UNION ALL 
						   SELECT * FROM Dosh.dbo.ActiveCustomers_NonDosh_'+@CustList+') b
				ON a.ARGUSPERMID = b.ArgusPermID
				group by pop, a.ArgusPermID,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) ,  
				CASE WHEN T0Segment <>0 THEN  ''T0''
					  WHEN T14Segment <>0 THEN ''T14''
					  WHEN T30Segment <>0 THEN ''T30''
					  WHEN T60Segment <>0 THEN ''T60''
					  WHEN T90Segment <> 0 THEN ''T90''
					  ELSE ''T120'' END
					  
					  
					  
				  IF EXISTS
				(
				SELECT * 
				FROM Dosh.INFORMATION_SCHEMA.Tables
				WHERE Table_Name = '''+@Merchant+'_TotalDoshCustomers''
				)

					DROP TABLE 
					Dosh.dbo.'+@Merchant+'_TotalDoshCustomers
					  
					  
				--1) INSERTING ALL THE CUSTOMERS IN A STAGING TABLE WITH THE CARD TYPE AND BIN NUMBER INFORMATION. 
				--THIS IS APPLIED ONLY TO T120 AND LINKED WITH TRANSACTION POPULATION.

				----Shashi : Added campaign_Zip_Flag from the Table step2 of Customer Activity Segment instead of Hardcoding it 
				
				SELECT a.arguspermid, b.Dosh_Customer_Key, c.card_type,c.bin_number, a.campaign_Zip_Flag 
				INTO Dosh.dbo.'+@Merchant+'_TotalDoshCustomers
				FROM Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2 a
				INNER JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_'+@CustList+' b
				ON CAST(a.arguspermid AS VARCHAR(MAX)) = CAST(b.argusPermID AS VARCHAR(MAX))
				LEFT JOIN  (SELECT a.dosh_customer_key, card_type , bin_number
						FROM Dosh_Raw.dbo.Raw_customer_transactions_'+@lastMonday+' a
						LEFT JOIN Dosh_Raw.dbo.Raw_Card_'+@lastMonday+' b
						ON a.dosh_customer_key = b.dosh_customer_key
						AND A.card_identifier = b.card_identifier
						GROUP BY a.dosh_customer_key, card_type ,bin_number
					) c
				ON b.Dosh_Customer_Key = c.dosh_customer_key 	
				WHERE SegmentFlag= ''T120'' and pop = ''Linked with Transactions''
				Group by a.arguspermid, b.Dosh_Customer_Key, c.card_type,c.bin_number, a.campaign_Zip_Flag

				
				
				--Creating the Final Customer Activity Segment and given the Length to column SegmentFlag as 50
				IF EXISTS
					(
					SELECT * 
					FROM Dosh.INFORMATION_SCHEMA.Tables
					WHERE Table_Name = ''Customer_ActivitySegment_'+@Merchant+'_'+@Version+'''
					)

				DROP TABLE 
				Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'
				
				
				CREATE TABLE Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'(
							[pop] [varchar](30) ,
							[arguspermid] [int] ,
							[Campaign_Zip_Flag] [int] ,
							[SegmentFlag] [varchar](10)
						)
				
				
				

				--1) INSERTING EVERYTHING EXCEPT SEGMENT ''Linked With Transactions''
				INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'
				select POP, arguspermid, campaign_Zip_Flag, SegmentFlag
				FROM Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2
				--WHERE SegmentFlag <> ''T120''	--joseph changed 
				where pop <> ''Linked With Transactions''
				
				
				
				--INSERTING EVERYTHING EXCEPT T120 FROM ''LINKED WITH TRANSACSTIONS''
					INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'
					select POP, arguspermid, campaign_Zip_Flag, SegmentFlag
					from Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2
					where pop = ''Linked With Transactions''
					and SegmentFlag <> ''T120''
				
				
				
				--2) INCLUDING ONLY THOSE CUSTOMERS IN T120 WHICH HAVE VALID CARD TYPE LINKED WITH DOSH WITHIN ARGUS CONSORTIA
				INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'
				SELECT ''Linked With Transactions'' AS POP, arguspermid, a.campaign_Zip_Flag, ''T120'' AS SegmentFlag
				FROM (SELECT arguspermid , campaign_Zip_Flag
					FROM Dosh.dbo.'+@Merchant+'_TotalDoshCustomers a
				INNER JOIN (SELECT * FROM Dosh.dbo.PP_BinBankList_Credit UNION ALL 
							SELECT * FROM Dosh.dbo.PP_BinBankList_Debit) b
				ON a.bin_number = b.bin_number
				WHERE BankID NOT IN (234,246,999)) a
				GROUP BY arguspermid, a.campaign_Zip_Flag


				--3) REST OF THE CUSTOMERS FALL IN EXCLUDED SEGMENT
				INSERT INTO Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'
				SELECT ''Linked With Transactions'' AS POP, arguspermid, campaign_Zip_Flag, ''Excluded'' AS SegmentFlag
				FROM  Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+'_step2
				WHERE arguspermid NOT IN (SELECT arguspermid FROM  Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+')
				AND pop = ''Linked with Transactions''
				AND SegmentFlag = ''T120''
				GROUP BY arguspermid, campaign_Zip_Flag
				
				
				--*******************************************************
				
				--Shashi Changes on 28-05-2019 Customer Activity Segment for Segment T120 Split Ends


')
--Added on 24-05-2019 start
EXEC ('

ALTER TABLE Dosh.dbo.Customer_ActivitySegment_'+@Merchant+'_'+@Version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
--Added on 24-05-2019 end

FETCH NEXT FROM MerchantCursor INTO @merchant ,@OnlineFlag , @MerchantIndexNumber, @MerchantCreditSicCode , @MerchantDebitSicCode ,@CampaignStartDate , @Campaign_Zip_Flag
    
END 
CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_SEGMENT_ACTIVITY''');
           END