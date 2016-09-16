

#load library to use for reading data to r object
#library("openxlsx")
#-------------------
#DO only once
#install.packages("XLConnect")
#install.packages("RODBC")
#install.packages("RODBCext")
#install.packages("gridExtra")
#------------------
rm(list=ls())
library("XLConnect")
#specify file to read, this will read all work sheets to the object
wb <- loadWorkbook("Daily Center Report.xlsx") 
lst <- readWorksheet(wb, sheet = getSheets(wb))
#convert data from list to dataframe
#does not read the last sheet
for(sheets in 1:(length(lst)-1)){
  tmp <-as.data.frame(lst[[sheets]])
  
  if (sheets == 1){
    df <-  as.data.frame(tmp)
    df$Oppty.Closed..Date. <- strptime(df$Oppty.Closed..Date., "%Y-%m-%d")
    #N.a data type is logical at the moment
  }else{
    names(tmp) <- names(df)
    tmp$Oppty.Closed..Date. <- strptime(tmp$Oppty.Closed..Date., "%Y-%m-%d")
    #N.a data type is logical at the moment
    df <- rbind(df, as.data.frame(tmp))
  }
}
#read adta from OC
wb <- loadWorkbook("Daily Center Report - OC.xlsx") 
df_oc <- readWorksheet(wb, sheet = "OC", header = TRUE)
df_oc$Oppty.Closed..Date. <- strptime(df_oc$Oppty.Closed..Date., "%Y-%m-%d")
df <- rbind(df, df_oc) 




df$City <- substring(df$Center, 1, 2)

#create connection
library(RODBC)
dbhandle <- odbcDriverConnect('driver={SQL Server};server=JOSHUASHAPIRO\\SQLEXPRESS;database=Center Management Reporting;trusted_connection=true')

#insert data
#big insert
#Date Logged = Today.s.Date
library(RODBCext)
for(row in 1:nrow(df)){
  query <- paste0("INSERT INTO [Center Management Reporting].[dbo].[Daily Center Report](
                  [Center], 
                  [Date Logged], 
                  [Opportunity Number],", 
                  ###[Oppty Closed (Date)], 
                  ###[Onboarding Complete (Date)], 
                  ###[Account Name],
                  "[Recvd Date - 1stDelivery],
                  [Box Delivery (Activity)],
                  [Prep (Activity)],
                  [Scan (Activity)],
                  [QC (Activity)],
                  [Output (Activity)],
                  [Shred / Return (Activity)],
                  [Job Status], 
                  [Comments], 
                  [Date Complete]) 
                  VALUES ( 
                  ", ifelse(is.na(df[row, 1]), "''", paste0("'",df[row, 1],"'")), ",
                  ", ifelse(is.na(df[row, 2]), "'01-01-2000'", paste0("'",df[row, 2],"'")), ",
                  ", ifelse(is.na(df[row, 3]), "''", paste0("'",df[row, 3],"'")), ",
                  "#, ifelse(is.na(df[row, 4]), "'01-01-2000'", paste0("'",df[row, 4],"'")), ",
                  #", ifelse(is.na(df[row, 5]), "'01-01-2000'", paste0("'",df[row, 5],"'")), ",
                  #", ifelse(is.na(df[row, 6]), "''", paste0("'",df[row, 6],"'")), ",
                  , ifelse(is.na(df[row, 7]), "'01-01-2000'", paste0("'",df[row, 7],"'")), ",
                  ", ifelse(is.na(df[row, 8]), "0", paste0("",df[row, 8],"")), ",
                  ", ifelse(is.na(df[row, 9]), "0", paste0("",df[row, 9],"")), ",
                  ", ifelse(is.na(df[row,10]), "0", paste0("",df[row,10],"")), ",
                  ", ifelse(is.na(df[row,11]), "0", paste0("",df[row,11],"")), ",
                  ", ifelse(is.na(df[row,12]), "0", paste0("",df[row,12],"")), ",
                  ", ifelse(is.na(df[row,13]), "0", paste0("",df[row,13],"")), ",
                  ", ifelse(is.na(df[row,14]), "''", paste0("'",df[row,14],"'")), ",
                  ", ifelse(is.na(df[row,15]), "''", paste0("'",df[row,15],"'")), ",
                  ", ifelse(is.na(df[row,16]), "'01-01-2000'", paste0("'",df[row,16],"'")), ")")
  #print(query)
  sqlQuery(dbhandle, query)
}


#box_inv <- sqlQuery(dbhandle, "  SELECT * FROM [daily].[dbo].[Box Inventory]
#  WHERE [Recvd Date - 1stDelivery] IN (SELECT max([Recvd Date - 1stDelivery]) FROM [daily].[dbo].[Box Inventory]);")
#the table keep only lastest values
box_inv <- sqlQuery(dbhandle, "  SELECT * FROM [Center Management Reporting].[dbo].[Box Inventory]")
names(box_inv) <-  c("Center", "AccountName","OpportunityNumber",  "TotalUnitsRecvd",         
                     "PendingTotal", "PrepTotal",   "ScanTotal",  "QCTotal",                
                     "OutputTotal", "ReturnTotal", "RecvdDate", "CompleteDate", "ClosedDate")  
trim <- function(x) gsub("^\\s+|\\s+$", "", x)
box_inv[,3] <- lapply(box_inv[3], as.character)
box_inv[,1] <- lapply(box_inv[1], as.character)
box_inv[,3] <- trim(box_inv[,3])
box_inv[,1] <- trim(box_inv[,1])
#create empty data frames
invt <- read.csv(text="center, ac_name, opp_number, load, prep, scan, QC, output, return, received_date,complete_date, closed_date, pct_load, pct_prep, pct_scan, pct_QC, pct_output, box_deliver")
actv <- read.csv(text="date, center, load, prep, scan, QC, output, pct_load, pct_prep, pct_scan, pct_QC, pct_output")
row <- 0
#Initialize the working variables
#Let W_PendingTotal = Inv_PendingTotal
#Let W_PrepTotal = Inv_PrepTotal
#Let W_ScanTotal = Inv_ScanTotal
#Let W_QCTotal = Inv_QCTotal 
for (city in unique(df$City)){
  tmp_sub_city <- subset(df, df$City == city,)
  returnback <- 0
  for (op in unique(tmp_sub_city$Opportunity.Number)){
    tmp <- subset(tmp_sub_city, tmp_sub_city$Opportunity.Number == op,)
    tmp_w <- subset(box_inv, box_inv$Center==city,)
    W_PendingTotal <- ifelse(is.numeric(box_inv[,5]), box_inv[,5], 0)
    W_PrepTotal <- ifelse(is.numeric(box_inv[,6]), box_inv[,6], 0)
    W_ScanTotal <- ifelse(is.numeric(box_inv[,7]), box_inv[,7], 0)  
    W_QCTotal <- ifelse(is.numeric(box_inv[,8]), box_inv[,8], 0)
    W_OutputTotal <- ifelse(is.numeric(box_inv[,9]), box_inv[,9], 0)
    total <- sum(tmp$Box.Delivery..Activity., na.rm = TRUE)+sum(tmp$Prep..Activity., na.rm = TRUE)+sum(tmp$Scan..Activity., na.rm = TRUE)+sum(tmp$QC..Activity., na.rm = TRUE)+sum(tmp$Output..Activity., na.rm = TRUE) 
    #run the algorithm for inventory
    #(i) IF A_BoxIntake IS NOT ZERO THEN 
    if(sum(tmp$Box.Delivery..Activity., na.rm = TRUE) != 0){
      W_PendingTotal <- W_PendingTotal + sum(tmp$Box.Delivery..Activity., na.rm = TRUE)
    }
    if(sum(tmp$Prep..Activity., na.rm = TRUE) != 0){
      #(ii) IF A_PrepComplete IS NOT ZERO THEN
      W_PendingTotal <- W_PendingTotal - sum(tmp$Prep..Activity., na.rm = TRUE)
      W_PrepTotal <- W_PrepTotal + sum(tmp$Prep..Activity., na.rm = TRUE)
    }
    if(sum(tmp$Scan..Activity., na.rm = TRUE) != 0){
      #(iii) IF A_ScanComplete IS NOT ZERO THEN
      W_PrepTotal <- W_PrepTotal - sum(tmp$Scan..Activity., na.rm = TRUE) 
      W_ScanTotal <- W_ScanTotal + sum(tmp$Scan..Activity., na.rm = TRUE)
    }
    if(sum(tmp$QC..Activity., na.rm = TRUE) != 0){
      W_ScanTotal <- W_ScanTotal - sum(tmp$QC..Activity., na.rm = TRUE)
      W_QCTotal <- W_QCTotal + sum(tmp$QC..Activity., na.rm = TRUE) 
    }
    if(sum(tmp$Output..Activity., na.rm = TRUE) != 0){
      W_QCTotal <- W_QCTotal - sum(tmp$QOutput..Activity., na.rm = TRUE)
      W_OutputTotal <- W_OutputTotal + sum(tmp$Output..Activity., na.rm = TRUE) 
    }
    
    
    row <- row + 1
    box_received <- sum(tmp$Box.Delivery..Activity., na.rm = TRUE) + unlist(ifelse(nrow(subset(box_inv, box_inv$Center==city & box_inv$OpportunityNumber==op,)) == 0, 0,subset(box_inv, box_inv$Center==city & box_inv$OpportunityNumber==op,TotalUnitsRecvd)))
    invt_total <- W_PendingTotal + W_PrepTotal + W_ScanTotal + W_QCTotal + W_OutputTotal
    invt[row,] <- list(city, tmp$Account.Name[1], tmp$Opportunity.Number[1], W_PendingTotal, W_PrepTotal, W_ScanTotal, W_QCTotal, W_OutputTotal,
                       sum(tmp$Shred...Return..Activity., na.rm = TRUE), as.character(strptime(tmp$Date.Logged[1], "%Y-%m-%d")), as.character(strptime(tmp$Date.Complete[1], "%Y-%m-%d")),as.character(strptime(tmp$Oppty.Closed..Date.[1], "%Y-%m-%d")) ,
                       W_PendingTotal/invt_total, W_PrepTotal/invt_total, W_ScanTotal/invt_total, W_QCTotal/invt_total, W_OutputTotal/invt_total, box_received)
    #activity
    actv[row,] <- list(as.character(strptime(tmp$Date.Logged[1], "%Y-%m-%d")), city, sum(tmp$Box.Delivery..Activity., na.rm = TRUE), sum(tmp$Prep..Activity., na.rm = TRUE),
                       sum(tmp$Scan..Activity., na.rm = TRUE), sum(tmp$QC..Activity., na.rm = TRUE), sum(tmp$Output..Activity., na.rm = TRUE),
                       sum(tmp$Box.Delivery..Activity., na.rm = TRUE)/total, sum(tmp$Prep..Activity., na.rm = TRUE)/total,
                       sum(tmp$Scan..Activity., na.rm = TRUE)/total, sum(tmp$QC..Activity., na.rm = TRUE)/total, sum(tmp$Output..Activity., na.rm = TRUE)/total)
    
  }
}

#empty table
sqlQuery(dbhandle, "DELETE FROM [Center Management Reporting].[dbo].[Box Inventory]")
for(row in 1:nrow(invt)){
  #print(row)
  query <- paste0("INSERT INTO [Center Management Reporting].[dbo].[Box Inventory](
                  [Center],
                  [Opportunity Number],
                  [Account Name],
                  [Total Units Recvd],
                  [Pending Total],
                  [Prep Total],
                  [Scan Total],
                  [QC Total],
                  [Output Total],
                  [Shred / Return Total],
                  [Recvd Date - 1stDelivery],
                  [Onboarding Complete Date],
                  [Oppty Closed Date]) 
                  VALUES ( 
                  ", ifelse(is.na(invt[row, 1]), "''", paste0("'",invt[row, 1],"'")), ", 
                  ", ifelse(is.na(invt[row, 3]), "''", paste0("'",invt[row, 3],"'")), ",
                  ", ifelse(is.na(invt[row, 2]), "''", paste0("'",invt[row, 2],"'")), ", 
                  ", ifelse(is.na(invt[row, 18]), "0", paste0("",invt[row, 18],"")), ",
                  ", ifelse(is.na(invt[row, 4])||invt[row, 4]<0, "0", paste0("",invt[row, 4],"")), ",
                  ", ifelse(is.na(invt[row, 5]), "0", paste0("",invt[row, 5],"")), ",
                  ", ifelse(is.na(invt[row, 6]), "0", paste0("",invt[row, 6],"")), ",
                  ", ifelse(is.na(invt[row, 7]), "0", paste0("",invt[row, 7],"")), ",
                  ", ifelse(is.na(invt[row, 8]), "0", paste0("",invt[row, 8],"")), ",
                  ", ifelse(is.na(invt[row, 9]), "0", paste0("",invt[row, 9],"")), ",
                  ", ifelse(is.na(invt[row, 10]), "'01-01-2000'", paste0("'",invt[row, 10],"'")), ",
                  ", ifelse(is.na(invt[row, 11]), "'01-01-2000'", paste0("'",invt[row, 11],"'")), ",
                  ", ifelse(is.na(invt[row, 12]), "'01-01-2000'", paste0("'",invt[row, 12],"'")),  ")")
  #print(query)
  sqlQuery(dbhandle, query)
}


#writing reports - excel file
#library(dataframes2xls)
invt$date <- strptime(tmp$Date.Logged[1], "%Y-%m-%d")
invt$date <- as.character(invt$date)
actv$date <- strptime(tmp$Date.Logged[1], "%Y-%m-%d")
actv$date <- as.character(actv$date)
#invt$date <-as.character(invt$date)
#dataframes2xls::write.xls(c(invt,actv), "report.xls",sh.names = "inventory,activity")
#http://stackoverflow.com/questions/9699690/list-of-dataframes-to-pdf

for( i in 1:5){
  invt[,i+3] <- paste(invt[,i+3], round(invt[,i+12], digits=3),sep=', ')
  actv[,i+2] <- paste(actv[,i+2], round(actv[,i+7], digits=3),sep=', ')
}

library(gridExtra)
pdf(file="inventory.pdf", width=20)
grid.newpage()
#grid.draw(tableGrob(head(invt[,1:8], 10), name="test"))
dev.off()
pdf(file="activity.pdf", width=20)
grid.newpage()
#grid.draw(tableGrob(head(actv[,1:8], 10), name="test"))
dev.off()
