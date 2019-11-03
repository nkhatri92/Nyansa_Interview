import sys
from pyspark import SparkContext
import re
sc =SparkContext()

text = sc.textFile(sys.argv[1])

# get values from the text file 
def list_values(file_data):
    re1='.*?'
    re2='((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(?![\\d])'	# Match possible combinations of device id: Ipv4
    re3='.*?'
    re4='((?:[a-z][a-z]+))'	# Device Type
    re5='.*?'	
    re6='(\\d+)'	# Score

    rg = re.compile(re1+re2+re3+re4+re5+re6)
    m = rg.search(file_data)
    if m:
            idd=m.group(1)
            make=m.group(2)
            score=m.group(3)
    return((idd+'-'+make, int(score)))

data = text.map(list_values)

#Compute the sum of scores in var (a) and the count of devies in var b
aTuple = (0,0)
sum_count = data.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                       lambda a,b: (a[0] + b[0], a[1] + b[1]))

#Division to get average values for each device id
average = sum_count.mapValues(lambda v: v[0]/v[1])

#Get poor devices by filtering values less than equal to 50 
poor_devices = average.filter(lambda x: x[1] <=50)

# get total counts and poor device count
poor_devices_id = poor_devices.map(lambda x:((x[0].split('-'))[1],x[1]))
id_device_type = average.map(lambda x:((x[0].split('-'))[1],(x[0].split('-'))[0]))

poor_device_count = sorted(poor_devices_id.countByKey().items())
total_count = sorted(id_device_type.countByKey().items())

#create rdd's to later join them
rdd1 = sc.parallelize(list(poor_device_count))
rdd2 = sc.parallelize(list(total_count))

#get poor ratio, sort and select the first
ans_final = rdd1.join(rdd2).mapValues(lambda x: x[0] / x[1]).takeOrdered(1, key = lambda x: -x[1])

# print the device type corresponding to the higest ratio
for m, n in ans_final:
    print(m)
