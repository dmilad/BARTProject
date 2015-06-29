date_hour = 'bob 02'
str(int(date_hour.split(' ')[1]))
date_hour = date_hour.split(' ')[0] + '_' + str(int(date_hour.split(' ')[1]))
print date_hour