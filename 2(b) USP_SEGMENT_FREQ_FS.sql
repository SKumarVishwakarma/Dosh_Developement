USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_SEGMENT_FREQ_FS] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: 3. Segment Frequency Table
    ** NAME: USP_SEGMENT_FREQ_FS
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
DECLARE @lv_Status VARCHAR(2)
SET @lv_Status = ''
				Select @lv_Status=[STATUS] from DBO.Performance_Monitoring
				where PERIOD=@CustList and PROC_NAME='USP_SEGMENT_FREQ_FS'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_SEGMENT_FREQ_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_SEGMENT_FREQ_FS'', CURRENT_TIMESTAMP, NULL,''I''');
		   

DECLARE @Month VARCHAR(10)
DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantIndexNumber VARCHAR(10)
DECLARE @MerchantCreditSicCode VARCHAR(500)
DECLARE @MerchantDebitSicCode VARCHAR(500)
DECLARE @version VARCHAR(50)
DECLARE @onlineFlag VARCHAR(10)
DECLARE @CampaignStartDate VARCHAR(12)
DECLARE @startDate VARCHAR(12)
DECLARE @EndDate VARCHAR(12)
DECLARE @Campaign_Zip_Flag VARCHAR(20)
DECLARE @ReportStart VARCHAR(MAX)
DECLARE @ReportEnd VARCHAR(MAX)
Declare @lastMonday VARCHAR(10)  --Shashi Changes on 28-05-2019 Added new Variable for Raw_Card and Raw_Transaction for Segment T120 Split

SET @ReportStart = LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6) --Report Start Date 1 Year Prior Running Date CustList
SET @ReportEnd = LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6) --Report End Date 1 Month Before Running Date CustList

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
FROM Dosh.dbo.AllDoshMerchantListNew
WHERE Is_Active = 1
	AND Competitor <> ''  and CampaignStartDate is not NULL  --AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> '' Removed to Include All Merchants Including Custom Merchants
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
FROM Dosh.dbo.AllDoshMerchantListNew
WHERE Is_Active = 1
AND Competitor <> ''  and CampaignStartDate is not NULL  --AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> '' Removed to Include All Merchants Including Custom Merchants
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

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @version = LEFT(DATENAME(MM, DATEADD(mm, - 1, @CustList)), 3) + RIGHT(LEFT(@CustList, 4), 2) + 'TPS'
	SET @startDate = CAST(DATEADD(MM, -6, @CampaignStartDate) AS DATE) -- 180 days before the campaign
	SET @EndDate = CAST(DATEADD(DD, - 1, @CampaignStartDate) AS DATE) -- 1 day before the Campaign
	SET @Month = LEFT(CONVERT(VARCHAR(10), CONVERT(DATE, @startDate, 121), 112), 6) -- Get month from Start Date
	SET @lastMonday=CONVERT(VARCHAR,DATEADD(WEEK, DATEDIFF(WEEK, 0,DATEADD(DAY, 6 - DATEPART(DAY, @CustList), @CustList)), 0),112)  --Shashi Changes on 28-05-2019 Added new Variable for Raw_Card and Raw_Transaction for Segment T120 Split

	EXEC(
			'  --Create Table here 
		USE Dosh

		IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+''' 
			)

			DROP TABLE  Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+'
			
			CREATE TABLE Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+'(
			POP VARCHAR(30),
			ARGUSPERMID int,
			PreFrequency INT)
			



'
			)
			
			
			EXEC('
			
			--Insert Data To Above created Table With Trans 
			
				USE Dosh

				INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+'
				select ''Linked With Transactions'' as pop, a.ArguspermId
				,case when tag.PreFrequency is null then 0 else tag.PreFrequency end as PreFrequency
				from 
				(select a.arguspermid
				from

				(
				select arguspermid from DOSH.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped group by arguspermid 
				union 
				select arguspermid from Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped group by arguspermid) a

				inner JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
				On cast(a.ArgusPermId as varchar) = cast(p.ArguspermID as varchar)
				inner JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
				On p.dosh_customer_key = l.dosh_customer_key
				where  LinkedTag = ''Linked'' and TransTag = ''WithTrans''
				group by a.arguspermid) a
				left join 
				(select * from Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
				where segment =(''Linked With Transactions'')) tag
				on cast(a.ArgusPermId as varchar)=cast(tag.arguspermid as varchar)
				
				
				
			--Insert Data To above created Table without Trans 
			
				INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+'
				select ''Linked Without Transactions'' as pop, a.ArguspermId
				,case when tag.PreFrequency is null then 0 else tag.PreFrequency end as PreFrequency
				from 
				(select a.arguspermid
				from

				(
				select arguspermid from DOSH.dbo.YG_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped group by arguspermid 
				union 
				select arguspermid from Dosh.dbo.YG_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped group by arguspermid) a

				inner JOIN dosh.dbo.YG_Dosh_Customer_DoshKey_Perm_' + @CustList + '  p
				On cast(a.ArgusPermId as varchar) = cast(p.ArguspermID as varchar)
				inner JOIN Dosh.dbo.YG_Dosh_Customer_Tag_List_' + @CustList + ' l 
				On p.dosh_customer_key = l.dosh_customer_key
				where  LinkedTag = ''Linked'' and TransTag = ''WithoutTrans''
				group by a.arguspermid) a
				left join 
				(select * from Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
				where segment =(''Linked Without Transactions'')) tag
				on cast(a.ArgusPermId as varchar)=cast(tag.arguspermid as varchar)
			
			--Insert Data To Above table Non Dosh 
			
				insert into Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+'
				select ''NonDosh'' as pop, a.ArguspermId
				,case when tag.PreFrequency is null then 0 else tag.PreFrequency end as PreFrequency
				from 
				(select arguspermid from
				(
				select arguspermid from DOSH.dbo.YG_Non_DOSH_PermIdTradeIdAcctId_Yearly_' + @CustList + '_Open_Deduped group by arguspermid 
				union
				select arguspermid from Dosh.dbo.YG_Non_dosh_DAPS_PermIDAcctID_' + @CustList + '_Deduped group by arguspermid) a
				group by arguspermid) a
				left join 
				(select * from Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + ' where segment =(''NonDosh'')) tag
				on cast(a.ArgusPermId as varchar)=cast(tag.arguspermid as varchar)
			
			
			 ')
			
			
			
					
			 EXEC (
			'  
				/*USE Dosh

				IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''Customer_FrequencySegment_' + @Merchant + '_' + @Version + '''  
			)

			DROP TABLE  Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + '
			
					select pop,a.arguspermid, a.PreFrequency,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) AS Campaign_Zip_Flag
					 
				into Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + '
				from Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+' a
				LEFT JOIN (SELECT * FROM Dosh.dbo.ActiveCustomers_Dosh_'+@CustList+' UNION ALL 
						   SELECT * FROM Dosh.dbo.ActiveCustomers_NonDosh_'+@CustList+') b
				ON a.ARGUSPERMID = b.ArgusPermID
				group by pop,a.arguspermid, a.PreFrequency,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) */
					 
					 	--*******************************************************
				
				--Shashi Changes on 28-05-2019 Customer Frequency Segment for Segment PreFrequency 0 Split Starts
				
				IF EXISTS
					(
					SELECT * 
					FROM Dosh.INFORMATION_SCHEMA.Tables
					WHERE Table_Name = ''Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2''
					)

				DROP TABLE 
				Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2


				select pop,a.arguspermid, a.PreFrequency,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) AS Campaign_Zip_Flag
					 
				into Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + '_step2
				from Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_Step1_'+@Version+' a
				LEFT JOIN (SELECT * FROM Dosh.dbo.ActiveCustomers_Dosh_'+@CustList+' UNION ALL 
						   SELECT * FROM Dosh.dbo.ActiveCustomers_NonDosh_'+@CustList+') b
				ON a.ARGUSPERMID = b.ArgusPermID
				group by pop,a.arguspermid, a.PreFrequency,
				ISNULL(CASE WHEN '''+@Campaign_zip_Flag+''' = ''NationWide'' THEN 1 
					 ELSE '+@Campaign_zip_Flag+'CampaignFlag END,1) 
					  
					  
					  
				  IF EXISTS
				(
				SELECT * 
				FROM Dosh.INFORMATION_SCHEMA.Tables
				WHERE Table_Name = '''+@Merchant+'_TotalDoshCustomersPreFrequency''
				)

					DROP TABLE 
					Dosh.dbo.'+@Merchant+'_TotalDoshCustomersPreFrequency
					  
					  
				--1) INSERTING ALL THE CUSTOMERS IN A STAGING TABLE WITH THE CARD TYPE AND BIN NUMBER INFORMATION. 
				--THIS IS APPLIED ONLY TO PreFrequency 0 AND LINKED WITH TRANSACTION POPULATION.

				----Shashi : Added campaign_Zip_Flag from the Table step2 of Customer Frequency Segment instead of Hardcoding it 
				
				SELECT a.arguspermid, b.Dosh_Customer_Key, c.card_type,c.bin_number, a.campaign_Zip_Flag 
				INTO Dosh.dbo.'+@Merchant+'_TotalDoshCustomersPreFrequency
				FROM Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2 a
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
				WHERE PreFrequency= ''0'' and pop = ''Linked with Transactions''
				Group by a.arguspermid, b.Dosh_Customer_Key, c.card_type,c.bin_number, a.campaign_Zip_Flag

				
				
				--Creating the Final Customer Activity Segment and given the Length to column PreFrequency as 50
				IF EXISTS
					(
					SELECT * 
					FROM Dosh.INFORMATION_SCHEMA.Tables
					WHERE Table_Name = ''Customer_FrequencySegment_'+@Merchant+'_'+@Version+'''
					)

				DROP TABLE 
				Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
				
				
				CREATE TABLE Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'(
							[pop] [varchar](30) ,
							[arguspermid] [int] ,
							[Campaign_Zip_Flag] [int] ,
							[PreFrequency] [varchar](10)
						)
				
				
				

				--1) INSERTING EVERYTHING EXCEPT SEGMENT ''Linked With Transactions''
				INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
				select POP, arguspermid, campaign_Zip_Flag, PreFrequency
				FROM Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2
				WHERE pop <> ''Linked With Transactions''
				
				
				--JOSEPH ADDED START
				--INSERTING EVERYTHING EXCEPT PreFrequency 0 FROM ''LINKED WITH TRANSACSTIONS''
					INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
					select POP, arguspermid, campaign_Zip_Flag, PreFrequency
					from Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2
					where pop = ''Linked With Transactions''
					and PreFrequency <> ''0''
				--JOSEPH ADDED END
				
				
				--2) INCLUDING ONLY THOSE CUSTOMERS IN PreFrequency 0 WHICH HAVE VALID CARD TYPE LINKED WITH DOSH WITHIN ARGUS CONSORTIA
				INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
				SELECT ''Linked With Transactions'' AS POP, arguspermid, a.campaign_Zip_Flag, ''0'' AS PreFrequency
				FROM (SELECT arguspermid , campaign_Zip_Flag
					FROM Dosh.dbo.'+@Merchant+'_TotalDoshCustomersPreFrequency a
				INNER JOIN (SELECT * FROM Dosh.dbo.PP_BinBankList_Credit UNION ALL 
							SELECT * FROM Dosh.dbo.PP_BinBankList_Debit) b
				ON a.bin_number = b.bin_number
				WHERE BankID NOT IN (234,246,999)) a
				GROUP BY arguspermid, a.campaign_Zip_Flag


				--3) REST OF THE CUSTOMERS FALL IN EXCLUDED SEGMENT
				INSERT INTO Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
				SELECT ''Linked With Transactions'' AS POP, arguspermid, campaign_Zip_Flag, ''Excluded'' AS PreFrequency
				FROM  Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'_step2
				WHERE arguspermid NOT IN (SELECT arguspermid FROM  Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+')
				AND pop = ''Linked with Transactions''
				AND PreFrequency = ''0''
				GROUP BY arguspermid, campaign_Zip_Flag
				
				
				--*******************************************************
				
				--Shashi Changes on 28-05-2019 Customer Frequency Segment for Segment PreFrequency 0 Split Ends
			
			



'
			)


	--Added on 24-05-2019 start
	EXEC ('

	ALTER TABLE Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE) 
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
END

CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_SEGMENT_FREQ_FS''');
           
           END