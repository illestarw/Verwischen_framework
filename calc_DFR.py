
import ast
import configparser



if __name__ == "__main__":
	'''
	conf = configparser.ConfigParser()
	conf.read('config.ini')

	QI_POS = ast.literal_eval(conf['params']['QI_POS'])
	# check if the interpreted data is a list
		if not isinstance(QI_POS, list):
			raise SyntaxError
		for element in QI_POS:
			if not isinstance(element, int):
				raise SyntaxError
	'''
	glucose = [70, 100, 125]
	systolic = [90, 120, 140]
	diastolic = [60, 90]


	data = []
	with open("dataset.csv") as f:
		for record in f:
			data.append([x.strip() for x in record.split(',')])
	
	
	fail_glucose = fail_systolic = fail_diastolic = 0
	total_tuples = 0

	with open("output_tuple.txt") as f:
		for output in f:
			lis = ast.literal_eval(output)
			
			total_tuples += 1

			for record in data:
				if lis[-1] == record[-1]:
					for i in glucose:
						if lis [1][0] < i < lis[1][1]:
							fail_glucose += 1
					for i in systolic:
						if lis [1][0] < i < lis[1][1]:
							fail_systolic += 1
					for i in diastolic:
						if lis [1][0] < i < lis[1][1]:
							fail_diastolic += 1
		
		print("DFR_glucose: ", fail_glucose / total_tuples)
		print("DFR_systolic: ", fail_systolic / total_tuples)
		print("DFR_diastolic: ", fail_diastolic / total_tuples)
