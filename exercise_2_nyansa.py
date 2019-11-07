import sys
from pyspark import SparkContext
import re
sc = SparkContext()

text = sc.textFile(sys.argv[1])

# get values from the text file 
def list_values(file_data):
    re1='.*?'
    re2='((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))(?![\\d])'	# Match possible combinations of device id: Ipv4
    re3='.*?'
    re4='((?:[a-z]+))'	# Device Type
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

aTuple = (0,0)
#group by key and sum corresponding score
sum_count = data.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                        lambda a,b: (a[0] + b[0], a[1] + b[1]))

#compute average score
average = sum_count.map(lambda v:((v[0].split('-'))[1],(v[1][0]/v[1][1],1,1))) 

#filter score, maintain count of filtered device type and compute poor-ratio 
ans = average.reduceByKey(lambda x,y:(x[1]+y[1] if x[0] < 50 else x[1], x[1]+y[1]))\
             .mapValues(lambda x: x[0]/ x[1]) 

ans.persist()   
              
#get the highest poor ratio
ans_first = ans.takeOrdered(1, key = lambda x: -x[1])
poorest_ratio = ans_first[0][1]

#print all the devices with the same poor ratio
ans_list = ans.filter(lambda x:x[1]==poorest_ratio).collect()

for result in ans_list:
    print(result[0])

