USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[USP_FREQ_DITRIBUTION_POST_FS] @CustList VARCHAR(50)
	
AS
/*******************************************
    ** FILE: Frequency Distribution
    ** NAME: USP_FREQ_DITRIBUTION_POST_FS
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 08/04/2019
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
				where PERIOD=@CustList and PROC_NAME='USP_FREQ_DITRIBUTION_POST_FS'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_FREQ_DITRIBUTION_POST_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_FREQ_DITRIBUTION_POST_FS'', CURRENT_TIMESTAMP, NULL,''I''');
		   

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
	SET @startDate = CAST(@CampaignStartDate AS DATE)   --For Post Frequency StartDate will be Campaign Date as Provided by Yilun 
	SET @EndDate =  case when LEFT(CONVERT(VARCHAR(10), (DATEADD(MM, 6, @CampaignStartDate)), 112), 6)<=LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)
					then Cast(DATEADD(MM, 6, @CampaignStartDate) as Date)
					else Cast(CONVERT(VARCHAR(10), dbo.udf_GetLastDayOfMonth((DATEADD(month, - 1, @CustList))), 112) as Date)   --Report End Date	
					end    --End Date will be 6 month from the Campaign Date or Max Transaction available whichever is lesser
	SET @Month = LEFT(CONVERT(VARCHAR(10), CONVERT(DATE, @startDate, 121), 112), 6) -- Get month from Start Date

	EXEC (
			'  --Create Table here 
		USE Dosh

		IF EXISTS
			(
			SELECT * 
			FROM Dosh.INFORMATION_SCHEMA.Tables
			WHERE Table_Name = ''CustomerPostFrequencySegment_Perm_' + @Merchant + '_' + @Version + ''' 
			)

			DROP TABLE  Dosh.dbo.CustomerPostFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
		
			select 
			a.arguspermid,a.segment,
			case when b.PostFreq is not null then b.PostFreq else 0 end as PostFreq
			into 
			Dosh.dbo.CustomerPostFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			from 
			(
			select arguspermid,segment
			from 
			Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + ' 
			group by arguspermid,segment
			) a  --perm
			left join 
			(
			select arguspermid,segment,sum(1.0) as PostFreq
			from 
			Dosh.dbo.CampaignShoppersPostFrequencySegment_' + @Merchant + '_' + @Version + '
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
			--select '''+@merchant+'''as Merchant, Segment,max(PostFreq) as Max_Valid_Frequency_Segment
			select '''+@merchant+'''as Merchant, Segment,max(CAST(PostFreq as INT)) as Max_Valid_Frequency_Segment --Added Cast to PostFreq in Case of No records , merchant like LornaJane
			--SHASHI Changes on 04/06/2019 End
			INTO Dosh.dbo.Temp_'+@merchant+'
			from
			(select Segment, PostFreq,sum(1.0) as customercount 
			from Dosh.dbo.CustomerPostFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			group by Segment, PostFreq
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
			WHERE Table_Name = ''CustomerPostFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '''  
			)

			DROP TABLE  Dosh.dbo.CustomerPostFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
		
			--SHASHI Changes on 04/06/2019 Start
			--select arguspermid, segment, case when PostFreq <= '''+@Cut+''' then PostFreq else '''+@Cut+''' end as PostFrequency 
			select arguspermid, segment, case when CAST(PostFreq as INT) <= '''+@Cut+''' then CAST(PostFreq as INT) else '''+@Cut+''' end as PostFrequency 
			--SHASHI Changes on 04/06/2019 End
			--Added Cast to PostFreq in Case of No records , merchant like LornaJane
			into Dosh.dbo.CustomerPostFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + '
			from
			Dosh.dbo.CustomerPostFrequencySegment_Perm_' + @Merchant + '_' + @Version + '
			where CAST(PostFreq as INT)>0  --Added Cast to PostFreq in Case of No records , merchant like LornaJane
			
			DROP Table Dosh.dbo.Temp_'+@merchant+'

			'
			)

	--Added on 24-05-2019 start
	EXEC ('
	ALTER TABLE Dosh.dbo.CustomerPostFrequencySegment_Perm_Cut_' + @Merchant + '_' + @Version + ' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE) 
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
           AND PROC_NAME = ''USP_FREQ_DITRIBUTION_POST_FS''');
           
           END