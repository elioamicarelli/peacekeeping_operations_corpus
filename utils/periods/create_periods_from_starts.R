library(dplyr)

starts <- read.csv('mission_start.csv')
working_starts <- starts
working_starts$date <- paste(working_starts$year,'-',working_starts$month,'-1', sep = '')

periods <- working_starts %>% 
  arrange(mission, month) %>% 
  mutate(date = as.Date(date, format = '%Y-%m-%d'),
         period = 1) %>%   
  transmute(mission, dates = purrr::map2(date, as.Date('2019-7-1'), seq, by = "1 month")) %>%
  tidyr::unnest() %>% 
  distinct() %>% 
  group_by(mission) %>% 
  mutate(period = 1:n()) %>% 
  ungroup() %>%
  mutate(key_period = paste(mission,lubridate::year(dates), months(dates), sep = ""))

country_mtd <- starts %>% 
  select(mission, iso3c, iso3n)

periods <- merge(periods, country_mtd, 'mission')

write.table(starts$mission, file = 'missions.txt', sep = ',',
            row.names = FALSE, col.names = FALSE, quote = FALSE)

write.table(periods, file = 'missions_periods.txt', sep = ',',
            row.names = FALSE, col.names = FALSE, quote = FALSE)

write.table(starts, file = 'missions_master.txt', sep = ',',
            row.names = FALSE, col.names = FALSE, quote = FALSE)