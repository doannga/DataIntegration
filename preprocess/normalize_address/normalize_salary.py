import os
import json
import re
with open('/home/nga/Documents/Project/DataIntegration/CrawlData/vieclam24h1.json', encoding='utf-8-sig') as f1:
    X = json.loads(f1.read())
    f1.close()

# with open('../job_crawl/careerbuilder.json', encoding='utf-8-sig') as f2:
#     Y = json.loads(f2.read())
#     f2.close()
d = {'-':'','–':''}
x_split_usd = []
for x in X:
	x = x.get("salary").lower()
	if x == 'cạnh tranh':
		minSalary = 'cạnh tranh'
		maxSalary = 'cạnh tranh'
	elif x == 'thương lượng':
		minSalary = 'thương lượng'
		maxSalary = 'thương lượng'
	
	elif x == 'thỏa thuận':
		minSalary = 'thỏa thuận'
		maxSalary = 'thỏa thuận'
	elif x != 'cạnh tranh' or x != 'thương lượng' or x != 'thỏa thuận': 
		x_split = x.replace(' - ',' ').replace(' – ',' ').replace('trên ','').replace(' trở lên','').replace('.','').replace(',','').split(' ')
		print(x_split)
		if x_split is not None and 'triệu' in x_split:
			x_split.remove('triệu')
			if(len(x_split) == 1):
				minSalary = int(x_split[0])*1000000
				maxSalary = minSalary
			else:
				minSalary = int(x_split[0])*1000000
				maxSalary = int(x_split[1])*1000000
		elif x_split is not None and 'usd' in x_split:
			x_split.remove('usd')
			if(len(x_split) == 1):
				minSalary = int(x_split[0])*23000
				maxSalary = minSalary
			else:
				minSalary = int(x_split[0])*23000
				maxSalary = int(x_split[1])*23000
		elif x_split is not None and 'vnđ' in x_split:
			x_split.remove('vnđ')
			if(len(x_split) == 1):
				minSalary = int(x_split[0])
				maxSalary = minSalary
			else:
				minSalary = int(x_split[0])
				maxSalary = int(x_split[1])
		elif x_split is not None and 'usd/tháng' in x_split:
			x_split.remove('usd/tháng')
			if(len(x_split) == 1):
				minSalary = int(x_split[0])
				maxSalary = minSalary
			else:
				minSalary = int(x_split[0])
				maxSalary = int(x_split[1])
		elif x_split is not None and 'vnd/tháng' in x_split:
			x_split.remove('vnd/tháng')
			if(len(x_split) == 1):
				minSalary = int(x_split[0])
				maxSalary = minSalary
			else:
				minSalary = int(x_split[0])
				maxSalary = int(x_split[1])
		else:
			if x_split is not None:
				for x_ in x_split:
					if x_ is not None and '$' in x_:
						x_split_usd.extend(x_.replace('$',''))
						x_split.remove(x_)
					else: 
						if len(x_split) == 1:
							minSalary = int(x_split[0])
							maxSalary = minSalary
						elif len(x_split) == 2:
							minSalary = int(x_split[0])
							maxSalary = int(x_split[1])
			else:
				if len(x_split_usd) == 1:
					minSalary = int(x_split_usd[0])*23000
					maxSalary = minSalary
				elif len(x_split_usd) == 2: 
					minSalary = int(x_split_usd[0])*23000
					maxSalary = int(x_split_usd[1])*23000
	print (minSalary)
	print(maxSalary)
