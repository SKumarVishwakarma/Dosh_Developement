USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_FREQ_DITRIBUTION_FS] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: 1. Frequency Distribution
    ** NAME: USP_FREQ_DITRIBUTION_FS
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
				where PERIOD=@CustList and PROC_NAME='USP_FREQ_DITRIBUTION_FS'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_FREQ_DITRIBUTION_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_FREQ_DITRIBUTION_FS'', CURRENT_TIMESTAMP, NULL,''I''');
		   

DECLARE @Month VARCHAR(10)
DECLARE @PrevMonth VARCHAR(8)
DECLARE @NextMonth VARCHAR(8)
DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantDesc VARCHAR(400)
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
DECLARE @Cut VARCHAR(MAX)

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
	AND Competitor <> ''  and CampaignStartDate is not NULL  ----AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> '' and   MerchantIndexNumber=92 removed this to Include All Merchants Including Custom Merchants.
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
AND Competitor <> ''  and CampaignStartDate is not NULL  ----AND CompetitorCreditSicCode <> '' AND CompetitorDebitSicCode <> '' and   MerchantIndexNumber=92 removed this to Include All Merchants Including Custom Merchants.
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

	EXEC (
			'  --Create Table here 
		USE Dosh

		IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''CustomerFrequencySegment_Perm_' + @Merchant + '_' + @Version + ''' 
			)

			DROP TABLE  Dosh.dbo.CustomerFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
		
			select 
			a.arguspermid,a.segment,
			case when b.prefreq is not null then b.prefreq else 0 end as PreFreq
			into 
			Dosh.dbo.CustomerFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			from 
			(
			select arguspermid,segment
			from 
			Dosh.dbo.CampaignShoppersFrequencySegment_' + @Merchant + '_' + @Version + ' 
			group by arguspermid,segment
			) a  --perm
			left join 
			(
			select arguspermid,segment,sum(1.0) as PreFreq
			from 
			Dosh.dbo.CampaignShoppersFrequencySegment_' + @Merchant + '_' + @Version + '
			where CAST(transactiondate AS DATE) between ''' + @startdate + ''' and ''' + @enddate + '''
			group by arguspermid,segment)b 
			on a.ArgusPermId=b.arguspermid and a.segment=b.segment



'
			)
			
			
			EXEC('
			
			USE Dosh

				IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''Temp_'+@merchant+'''  
			)

			DROP TABLE  Dosh.dbo.Temp_'+@merchant+'
			
			--SHASHI Changes on 04/06/2019 Start
			--select '''+@merchant+'''as Merchant, Segment,max(prefreq) as Max_Valid_Frequency_Segment
			select '''+@merchant+'''as Merchant, Segment,max(CAST(prefreq as INT)) as Max_Valid_Frequency_Segment  --Added Cast to PreFreq in Case of No records , merchant like LornaJane
			--SHASHI Changes on 04/06/2019 End
			INTO Dosh.dbo.Temp_'+@merchant+'
			from
			(select Segment, PreFreq,sum(1.0) as customercount 
			from Dosh.dbo.CustomerFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			group by Segment, PreFreq
			having sum(1.0)>250
			)a
			group by 
			 Segment
			 ')
			
			
			
			DECLARE @SQL nvarchar(max) = N'select @Cut=Min(Max_Valid_Frequency_Segment)+1
			from Dosh.dbo.Temp_'+@merchant+''
			
			exec sp_executesql @SQL, N'@Cut varchar(max) out',@Cut out
			
			
			
			EXEC (
			'  
			USE Dosh

			IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '''  
			)

			DROP TABLE  Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
		
			--SHASHI Changes on 04/06/2019 Start
			--select arguspermid, segment, case when PreFreq <= '''+@Cut+''' then PreFreq else '''+@Cut+''' end as PreFrequency 
			select arguspermid, segment, case when CAST(PreFreq as INT) <= '''+@Cut+''' then CAST(PreFreq as INT) else '''+@Cut+''' end as PreFrequency 
			--SHASHI Changes on 04/06/2019 End --Added Cast to PreFreq in Case of No records , merchant like LornaJane
			into Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
			from
			Dosh.dbo.CustomerFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			where CAST(PreFreq as INT)>0 --Added Cast to PreFreq in Case of No records , merchant like LornaJane
			
			DROP Table Dosh.dbo.Temp_'+@merchant+'



			'
			)
			
	--Added on 24-05-2019 start
	EXEC ('
	ALTER TABLE Dosh.dbo.CustomerFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '  REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE) 
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
           AND PROC_NAME = ''USP_FREQ_DITRIBUTION_FS''');
           
           END