from Verwischen import *

if __name__ == "__main__":
	
	# wipe file
	open("output_delay.txt", "w").close()
	open("output_tuple.txt", "w").close()
	
	setExperimentMode()
	stream_input_file("dataset4.csv")


	'''

	[
		[3, [1, 2, 3, 5, 6, 'a', 1589761561.1837153], ['-1', 0, 0, 0]]
	]

	'''
