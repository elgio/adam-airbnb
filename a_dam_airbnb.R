library(tidyverse)
library(data.world)
library(DataExplorer)
library(Hmisc)
library(sqldf)
library(lubridate)
#import dataframe from data.world
sql_stmt <- qry_sql("SELECT * from unit_1_project_dataset")
df <- data.world::query(sql_stmt,"lg414/project1")

setwd("C:/Users/lauren/Documents/GA Data Analysis")
clean_city <- read.csv("City Translations.csv",header=TRUE) 
colnames(clean_city)[which(colnames(clean_city) == 'ï..city')] <- 'city'
clean_nhood <- read.csv("Neighborhood Translations.csv",header=TRUE)
colnames(clean_nhood)[which(colnames(clean_nhood) == 'neighbourhood_cleansed')] <- 'neighbourhood.translated'
colnames(clean_nhood)[which(colnames(clean_nhood) == 'ï..neighbourhood')] <- 'neighbourhood_cleansed'
clean_state <- read.csv("State Translations.csv",header=TRUE)
colnames(clean_state)[which(colnames(clean_state) == 'ï..state')] <- 'state'

# DATA CLEANING -----------------------------------------------------------
#DUPLICATES
#dplyr::filter(df, duplicated(df)) %>% View() 
#DEAL WITH DUPLICATES
#run to view all duplicates
dupes <- df[duplicated(df$id)|duplicated(df$id, fromLast = TRUE),] %>% arrange(desc(id))
#confirmed all duplicates
#remove duplicate rows from df
df <- df[!duplicated(df),] #20 rows removed
#Per instructions, remove all rows with zero reviews
#NaN values harming data, only ~14, all 2 or fewer reviews; no strong effect on data - review later as overall rating exists
df <- subset(df,is.na(df$review_scores_rating)<1 & is.nan(df$review_scores_accuracy)<1 & is.na(df$beds)<1)
#honestly we're cutting so many corners with this data anyway
#
#CLEAN NAs
#create subset of na_by_col only containing cells with na values - easier to review
#na_cols_full <- subset(as.data.frame(colSums(is.na(df))),`colSums(is.na(df))`>0) %>% view()
      #state and zip should not be blank
      #bath,bed,rooms na?
      #imput HRR to zero, no response ?OR? -1.00?
      #ratings impute na values as avg across rows (retains mean)
      #N/A vs 0?

#handle na beds and baths - investigate first
# na_bed_bath <- subset(df,is.na(df$bathrooms)|is.na(df$bedrooms)) %>% select ("property_type","room_type","accommodates","beds","bed_type","guests_included","extra_people","bedrooms","bathrooms")
#unable to assume much - zero bathrooms doesn't make sense (even a shared room should have access to a bathroom) zero bedrooms makes sense if no dedicated bedroom (i.e. Studios)
#ignore these rows - only 49
df <- subset(df,is.na(bedrooms)<1 &is.na(bathrooms)<1)
# How to handle properly?

#impute NA host_response_rate to 0
df$host_response_rate <- ifelse(is.na(df$host_response_rate),0,df$host_response_rate)

# FEATURE ENGINEERING (AND MINOR ADDITIONAL DATA CLEANING)-----------------------------------------------------------
#clean_state
df$state_cleaned <- clean_state$state.translated[match(df$state, clean_state$state)]
#na state_cleaned to "North Holland" after review
df$state_cleaned <- ifelse(is.na(df$state_cleaned),"North Holland",df$state_cleaned)
#clean_neighborhood
df$neighborhood_cleaned <- clean_nhood$neighbourhood.translated[match(df$neighbourhood_cleansed, clean_nhood$neighbourhood_cleansed)]
#clean_city
df$city_cleaned <- clean_city$city.translated[match(df$city, clean_city$city)] 
#na city_cleaned to "Amsterdam" after review
df$city_cleaned <- ifelse(is.na(df$city_cleaned),"Amsterdam",df$city_cleaned)
###USE *_cleaned values instead of * locations

#impute missing ratings vales as mean
row_mean <- df %>% select (review_scores_accuracy,review_scores_checkin,review_scores_cleanliness,review_scores_communication,review_scores_location,review_scores_value) %>% rowMeans()
df <- df %>% mutate (row_mean)
df$review_scores_accuracy <- ifelse(is.na(df$review_scores_accuracy),df$row_mean,df$review_scores_accuracy)
df$review_scores_cleanliness <- ifelse(is.na(df$review_scores_cleanliness),df$row_mean,df$review_scores_cleanliness)
df$review_scores_checkin <- ifelse(is.na(df$review_scores_checkin),df$row_mean,df$review_scores_checkin)
df$review_scores_communication <- ifelse(is.na(df$review_scores_communication),df$row_mean,df$review_scores_communication)
df$review_scores_location <- ifelse(is.na(df$review_scores_location),df$row_mean,df$review_scores_location)
df$review_scores_value <- ifelse(is.na(df$review_scores_value),df$row_mean,df$review_scores_value)

df <- subset(df,is.na(row_mean)<1)

#clean zipcodes by removing alpha characters
df <- df %>% dplyr::mutate(Zip_no = substring(df$zipcode,1,4)) 
#get MODE of zip by neighborhood
zip_mode_by_nhood <- df %>% group_by(neighborhood_cleaned,Zip_no) %>% tally() %>% slice(which.max(n))
#*#impute na Zip to MODE for neighborhood
df$zip_mode <- zip_mode_by_nhood$Zip_no[match(df$neighborhood_cleaned,zip_mode_by_nhood$neighborhood_cleaned)] 
df$Zip_no <- ifelse(is.na(df$Zip_no),df$zip_mode,df$Zip_no)
#### Use Zip_no instead of zipcod


host_date_info <- t(as.data.frame(df$host_since_anniversary %>% gsub("POINT\\(","", .) %>% gsub("\\)","", .) %>% gsub(".0","", .) %>% strsplit(" ") %>% as.list()))
df$host_since_month <- host_date_info[,2] 
df$host_since_month <- ifelse(as.numeric(df$host_since_month)>12,as.numeric(df$host_since_month)-10,df$host_since_month)
df$host_since_date <- host_date_info[,1] 
#if date not provided, assume 1st of the month
df$host_since_date[df$host_since_date==""] <- 1

df$host_since <- paste(df$host_since_year, df$host_since_month, df$host_since_date, sep="/") %>% ymd() %>% as.Date()
#need to resolve remaining date issues, work around by ignoring those rows.
df %>% subset(!(is.na(df$host_since)))

#Get full host_since date / host_since_year is DBL / host_since_anniversary is CHR
#df2$host_since <- mutate(host_since = ymd(str_c(host_since_year,host_since_anniversary)))


#IGNORE IRRELEVANT DATA
names(df2)
#keep 1 36 14-24 
#ignore  2 9-13
#other (combine 4&5 = host_since),(6,7,8 - clean per instructions (VLOOKUP)) (compare row_mean*10, no decimals to host response rate, impute na?) (27 *2 as Est_stays (EC Add Est_stays = 0.5 where $27 =0 ))
#add/impute 1.revenue column based on parameters (2 guests unless max = 1)


# SELECT WORKING COLUMNS --------------------------------------------------

names(df)

dfworking <- df[c(1,5,43,36,14:24,28,34,49)] %>% drop_na() #something broke and it was tacking on full rows of NAs

# Calculating revenue
df$rev_accom <- ifelse(df$accommodates==1,1,2) %>% as.data.frame() #number of people accommodated per requiremtns
df$rev_guest_incl <-ifelse(df$guests_included==1,1,2)
df$rev_cost_night<- ifelse(df$guests_included<df$accommodates,df$price+df$extra_people,df$price)
df$rev_cost_book <- df$rev_cost_night*df$minimum_nights
df$rev_est_stays <- 2*df$number_of_reviews
df$revenue <- df$rev_cost_book*df$rev_est_stays

#export data
write.csv(df[c(1:49)],'df_clean.csv')
write.csv(dfworking,"dfworking_clean.csv")



#Get a subset of df with only rows containing na 
# doesn't work:na_rows <- df[rowsums(is.na(df)) > 0]
na_rows <- subset(df,rowSums(is.na(df))>0)


#Get column names and types
#DataExplorer::create_report(df)
#DataExplorer::create_report(df)


#not needed 
#init_summary <- summary(df)
#select(init_summary,"NA's")
#nn a_cols_imputed_test <- impute(na_cols_full,0)

names(df2)
config <- list("introduce" = list(), "plot_str" = list("type" = "diagonal", "fontSize" = 35, "width" = 1000, "margin" = list("left" = 350, "right" = 250)),
               "plot_missing" = list(),
               "plot_histogram" = list(),
               "plot_qq" = list(sampled_rows = 1000L),
               "plot_bar" = list(),
               "plot_correlation" = list("cor_args" = list("use" = "pairwise.complete.obs")),
               #"plot_prcomp" = list(),
               "plot_boxplot" = list(),
               "plot_scatterplot" = list(sampled_rows = 1000L)
)
#report where bathrooms is not na 
DataExplorer::create_report(subset(df2,is.na(df$bathrooms)<1) %>% dplyr::select(1,36,14:24), y="bathrooms", config=config) #1,3:9,14:24,27,36





########
df_zero_reviews <- subset(df,is.na(df$review_scores_rating)>0)
df_no_zero_reviews <- subset(df,is.na(df$review_scores_rating)<1)

names(df)
head(df)