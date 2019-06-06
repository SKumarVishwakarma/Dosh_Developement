USE [Dosh]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[USP_NATION_DRIVER_FS] @CustList VARCHAR(50)
	/*Above will be taken from outside cursor */
AS
/*******************************************s
    ** FILE: NationWide Frequency Report Driver Table 
    ** NAME: USP_NATION_DRIVER_FS
    ** DESC: 
    **
    ** AUTH: Shashikumar Vishwakarma
    ** DATE: 20/03/2019
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
				where PERIOD=@CustList and PROC_NAME='USP_NATION_DRIVER_FS'
				
				IF @lv_Status<>'C' 
					Begin
--Log the Start Time
	
	EXEC  ('DELETE FROM DBO.Performance_Monitoring WHERE  PROC_NAME=''USP_NATION_DRIVER_FS'' AND PERIOD='+@CustList+';
		   INSERT INTO DBO.Performance_Monitoring
		   SELECT '+@CustList+', ''USP_NATION_DRIVER_FS'', CURRENT_TIMESTAMP, NULL,''I''');



DECLARE @Merchant VARCHAR(50)
DECLARE @MerchantIndexNumber VARCHAR(10)
DECLARE @MerchantCreditSicCode VARCHAR(500)
DECLARE @MerchantDebitSicCode VARCHAR(500)
DECLARE @version VARCHAR(50)
DECLARE @startDate VARCHAR(12)
DECLARE @EndDate VARCHAR(12)
DECLARE @Campaign_Zip_Flag VARCHAR(20)
DECLARE @ReportStart VARCHAR(MAX)
DECLARE @ReportEnd VARCHAR(MAX)
DECLARE @pop varchar (max)
DECLARE @FrequencySegment varchar (max)
DECLARE @date varchar (max)

--Joseph R K changes on 23/04/2019 start
/*
DECLARE MerchantCursor CURSOR FOR  --here I need Cursor to Pick Merchants from AllDoshMerchantListNew @Merchant, @version, 
	SELECT Merchant
	FROM Dosh.dbo.AllDoshMerchantListNew   
	where Campaign_Zip_Flag='NationWide' and Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL 
								--and CompetitorDebitSicCode<>'' Select Every Merchants including Custom and CompetitorCreditSicCode<>''
*/
DECLARE MerchantCursor CURSOR FOR  --here I need Cursor to Pick Merchants from AllDoshMerchantListNew @Merchant, @version, 
SELECT distinct Merchant
FROM Dosh.dbo.AllDoshMerchantListNew   
where Campaign_Zip_Flag='NationWide' and Is_Active=1 and Competitor<>'' and CampaignStartDate is not NULL 
--Joseph R K changes on 23/04/2019 end						
	
OPEN MerchantCursor

FETCH NEXT FROM MerchantCursor INTO @Merchant

WHILE @@FETCH_STATUS=0
begin

SET @version=left(datename(MM, dateadd(mm,-1,@CustList)), 3) + right(left(@CustList,4),2) + 'TPS'


SET @ReportStart = DATEADD(month, DATEDIFF(month, 0, DATEADD(Year, - 1, @CustList)), 0) 			--LEFT(CONVERT(VARCHAR(10), (DATEADD(Year, - 1, @CustList)), 112), 6) --First Date of the ReportStartDate Month 
SET @ReportEnd 	 = DATEADD(d,-1, DATEADD(dd, - (DATEDIFF(dd, 4, dbo.udf_GetLastDayOfMonth(DateAdd(mm, 1, @CustList )))%7), dbo.udf_GetLastDayOfMonth(DateAdd(mm, 1, @CustList ))))
--TO Get the 1 Day Prior to the Next Monthly Run 
--DATEADD(d, -1, DATEADD(m, DATEDIFF(m, 0, DATEADD(month, - 1, @CustList)) + 1, 0)) --LEFT(CONVERT(VARCHAR(10), (DATEADD(month, - 1, @CustList)), 112), 6)  --Last Date of the ReportStartDate Month 

EXEC ('

			IF EXISTS
				(
				SELECT * 
				FROM Dosh.INFORMATION_SCHEMA.Tables
				WHERE Table_Name = ''FrequencySegment_National_'+@Merchant+'_'+@Version+'''
				)

		DROP TABLE dosh.dbo.FrequencySegment_National_'+@Merchant+'_'+@Version+'

			create table dosh.dbo.FrequencySegment_National_'+@Merchant+'_'+@Version+'(
			transactiondate VARCHAR(20),
			pop varchar (50),
			Campaign_Zip_Flag INT,
			PreFrequency VARCHAR(10))
')



	EXEC ('DECLARE PopSeg CURSOR FOR
		SELECT pop,prefrequency as FrequencySegment
		FROM Dosh.dbo.Customer_FrequencySegment_' + @Merchant + '_' + @Version + ' --Dosh.dbo.PP_DunkinDonut_FrequencySegment_Jan19TPS 
		group by pop,prefrequency
		--order by 1,2 
		')
		
		--gather combination of population & frequency for each merchant for each iteration (with monthly refresh)
		--THIS TABLE WILL BE FROM STEP 3 Dosh.dbo.Customer_FrequencySegment_'+@Merchant+'_'+@Version+'
	
OPEN PopSeg

FETCH NEXT FROM PopSeg INTO @pop,@FrequencySegment 

WHILE @@FETCH_STATUS=0
begin


		SET @DATE = cast(@ReportStart as date)   --  first date of the ReportStart Month SELECT DATEADD(month, DATEDIFF(month, 0, @mydate), 0) AS StartOfMont

		WHILE @DATE <= cast(@ReportEnd as date)  --    last date of the ReportEnd Date 
BEGIN
		
		EXEC  		
		('insert into dosh.dbo.FrequencySegment_National_'+@Merchant+'_'+@Version+'
					select cast('''+@DATE+''' as varchar),'''+@pop+''',1 as Campaign_Zip_Flag,'''+@FrequencySegment+'''
					')

SET @date =  cast(DATEADD(day, 1,@Date) as date) 
end

--Added on 24-05-2019 start
EXEC ('
ALTER TABLE dosh.dbo.FrequencySegment_National_'+@Merchant+'_'+@Version+' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
')
--Added on 24-05-2019 end

 
FETCH NEXT FROM PopSeg INTO @pop,@FrequencySegment
END 
CLOSE PopSeg

DEALLOCATE PopSeg

FETCH NEXT FROM MerchantCursor INTO @Merchant
    
END 
CLOSE MerchantCursor

DEALLOCATE MerchantCursor

--Log the End Time
   
   EXEC  (' UPDATE DBO.Performance_Monitoring SET EndTime = CURRENT_TIMESTAMP, STATUS=''C'' WHERE  Period = '+@CustList+'
           AND PROC_NAME = ''USP_NATION_DRIVER_FS''');
END