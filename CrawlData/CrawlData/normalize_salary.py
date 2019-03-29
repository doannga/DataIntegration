import os
import json
import re


# with open('../job_crawl/careerbuilder.json', encoding='utf-8-sig') as f2:
#     Y = json.loads(f2.read())
#     f2.close()
class Normalize_salary:
	"""docstring for normalize_salary"""
	
	def normalize_salary(self, x):
		salary = []
		d = {'-':'','–':''}
		x_split_usd = []
		
		x = x.lower()
		if x == 'cạnh tranh':
			minSalary = 'Cạnh tranh'
			maxSalary = 'Cạnh tranh'
		elif x == 'thương lượng':
			minSalary = 'Thương lượng'
			maxSalary = 'Thương lượng'
	
		elif x == 'thỏa thuận':
			minSalary = 'Thỏa thuận'
			maxSalary = 'Thỏa thuận'
		elif x != 'cạnh tranh' or x != 'thương lượng' or x != 'thỏa thuận': 
			x_split = x.replace(' - ',' ').replace(' – ',' ').replace('trên ','').replace('-',' ').replace(' trở lên','').replace('.','').replace(',','').split(' ')
			
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
			elif x_split is not None and 'vnd' in x_split:
				x_split.remove('vnd')
				if(len(x_split) == 1):
					minSalary = int(x_split[0])
					maxSalary = minSalary
				else:
					minSalary = int(x_split[0])
					maxSalary = int(x_split[1])
			elif x_split is not None and 'usd/tháng' in x_split:
				x_split.remove('usd/tháng')
				if(len(x_split) == 1):
					minSalary = int(x_split[0])*23000
					maxSalary = minSalary
				else:
					minSalary = int(x_split[0])*23000
					maxSalary = int(x_split[1])*23000
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
		salary.extend([minSalary,maxSalary])
		return salary
		
		
		
