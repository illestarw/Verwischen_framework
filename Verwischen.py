#!/usr/bin/env python3

import configparser
import random
import ast
import time
import os

##! Configurable Parameter (EDITABLE IN config.ini)

# quasi-identifier data position(s) in the inputs. must be integer indicating position (starting from 0)
QI_POS = [1, 2]

# sensitive data position(s) in the inputs. must be integer indicating position (starting from 0)
SI_POS = [3, 4]

# basic range for generalizing
GENERALIZE_RANGE = 5

# accumulation delay allowed (integer, multiplier of incoming tuple frequency)
ACCUMULATION_DELAY_TOLERANCE = 5

# Timer for force flushing all ECs and start over by scratch (avoid overfitting)
REFRESH_TIMER = 3600

# k-anonymity k
THRESHOLD_K = 5

# maximum members (records) allowed to covered by a single EC. All ECs will be wiped and refreshed when any EC reached this limit.
EC_MAX_HOLDING_MEMBERS = 100


##! Internally Used Variables (DO NOT alter)

# EC_list : 2 dimensional list of dict
EC_list = []

# the list storing accumulated tuples
# [counter, original value, QI_EC_indicator]
Accumulated_list = []

# dictionary for storing the compromised range for essential publication
Compromised_range_dict = {}

# dictionary for recording EC change during expiring tuple resolution process
EC_alter_log = {}

# The timer for EC refreshing
Init_timer = 0

# For research paper use only
EXPERIMENT_MODE = False


def read_config():
	'''
	Load the configuration from config.ini
	'''

	global QI_POS, SI_POS, GENERALIZE_RANGE, ACCUMULATION_DELAY_TOLERANCE, REFRESH_TIMER, THRESHOLD_K, EC_MAX_HOLDING_MEMBERS

	# try-catch block for reading config file
	try:
		conf = configparser.ConfigParser()
		conf.read('config.ini')
		
		QI_POS = ast.literal_eval(conf['params']['QI_POS'])		
		# check if the interpreted data is a list
		if not isinstance(QI_POS, list):
			raise SyntaxError
		for element in QI_POS:
			if not isinstance(element, int):
				raise SyntaxError

		SI_POS = ast.literal_eval(conf['params']['SI_POS'])		
		# check if the interpreted data is a list
		if not isinstance(QI_POS, list):
			raise SyntaxError
		for element in QI_POS:
			if not isinstance(element, int):
				raise SyntaxError

		GENERALIZE_RANGE = float(conf['params']['GENERALIZE_RANGE'])
		ACCUMULATION_DELAY_TOLERANCE = int(conf['params']['ACCUMULATION_DELAY_TOLERANCE'])
		REFRESH_TIMER = float(conf['params']['REFRESH_TIMER'])
		THRESHOLD_K = int(conf['params']['THRESHOLD_K'])
		EC_MAX_HOLDING_MEMBERS = int(conf['params']['EC_MAX_HOLDING_MEMBERS'])

	except Exception:
		raise Exception("Error: Invalid configuration parameters detected.")
	

def initialize():
	'''
	Initialize global variables
	'''

	global EC_list, Accumulated_list, Compromised_range_dict, EC_alter_log, Init_timer

	# initialize EC list
	EC_list = []

	for i in range(max(QI_POS) + 1):
		EC_list.append([])

	# initialize the accumulated tuple list
	Accumulated_list = []

	# initialize dictionary for compromised range
	Compromised_range_dict = {}

	# initialize dictionary for EC changes
	EC_alter_log = {}

	# initialize timer
	Init_timer = time.time()



def publish(rawstring, QI_EC_indicator, compmode):
	'''
	Publish data for transmission (ready to leave the device)
	'''

	# normal mode
	if not compmode:
		# for each QI position in the raw input tuple
		for n in QI_POS:
			# find the belonged EC
			ec = EC_list[n][QI_EC_indicator[n]]
			# replace actual QI value with generalized range
			rawstring[n] = [ec.get("lbound"), ec.get("ubound")]

	# compromised mode
	else:
		#debug
		#print("comp mode")
		
		for n in QI_POS:
			if n in Compromised_range_dict:
				# rewrite with compromised range
				rawstring[n] = Compromised_range_dict[n]
			else:
				# find the belonged EC
				ec = EC_list[n][QI_EC_indicator[n]]
				# replace actual QI value with generalized range
				rawstring[n] = [ec.get("lbound"), ec.get("ubound")]

		# reset the compromised record dictionary
		Compromised_range_dict.clear()


	# discard SI fields
	jump = 0
	for si in SI_POS:
		# pop SI element
		rawstring.pop(si - jump)
		
		# apply change of list order (due to element pop) to next SI
		jump += 1


	# output
	if EXPERIMENT_MODE:
		# simulate output for transmission by printing the message to console
		# in EXPERIMENT_MODE, last element of rawstring is the attached arrival timestamp of the tuple
		print("Transmitted : ", rawstring[:-1])

		# For research paper use only
		with open("output_tuple.txt", "a") as f:
			f.write( str(rawstring[:-1]) + '\n' )
		with open("output_delay.txt", "a") as f:
			f.write( str(time.time() - rawstring[-1]) + '\n' )
	else:
		# simulate output for transmission by printing the message to console.
		# should be replaced by actual mechanisms of the underlying device while being deployed to WMD
		print("Transmitted : ", rawstring)

	
	

# Unused
def purturbate(lbound, ubound, data):
	'''
	Perturbate leaf nodes
	'''
	# purturbate de-identified range
	seed = random.random()

	while True:
		if data > lbound - seed and data < ubound - seed:
			break

		# reseed
		seed = random.random() * 10 - 5

	return [lbound, ubound]




def create_EC(qi, lb, ub):
	global EC_list

	# the newly created EC will be the (EC_position)th EC of the QI
	EC_position = len(EC_list[qi])

	# init new EC
	ec = {
		'number': EC_position,
		'member': 1,
		'lbound': lb,
		'ubound': ub,
		'deprecated': False
	}

	#debug
	#print("createec-bef: ", EC_list[qi])
	
	# add to EC list of the QI
	EC_list[qi].append(ec)
	
	#debug
	#print("createec-aft: ", EC_list[qi])

	# return the EC position in list 
	return EC_position


def extend_EC(qi, ecn1, ecn2, original_value):
	global EC_list

	# sort the two EC
	if EC_list[qi][ecn1].get("ubound") > EC_list[qi][ecn2].get("ubound"):
		# swap
		ecn1, ecn2 = ecn2, ecn1

	# get average
	avg = ( EC_list[qi][ecn2].get("lbound") + EC_list[qi][ecn1].get("ubound") ) / 2

	# replace the original boundaries
	EC_list[qi][ecn1]["ubound"] = EC_list[qi][ecn2]["lbound"] = avg

	# return the new EC that the value falls in 
	if original_value > avg:
		return ecn2
	else:
		return ecn1



def generalize(qi, data):
	'''
	Prepare a generalized range for the given data point
	'''
	global EC_list

	# define lower and higher bound of generalized value 
	left_padding = random.random() * GENERALIZE_RANGE

	lb_new = data - left_padding
	ub_new = lb_new + GENERALIZE_RANGE
	overlap = []

	# check if overlap with existing ECs
	def review_overlap(f):
		nonlocal lb_new, ub_new, overlap

		if f == 0:
			overlap = []
		QIEC = EC_list[qi]

		# check over all ECs of the given QI
		for i in range(len(QIEC)):
			# if EC deprecated, skip
			if QIEC[i].get("deprecated"):
				continue
			# [...]: existed EC ; |...| new generalized range
			# | .. [ .. | .. ]
			if lb_new < QIEC[i].get("lbound") < ub_new:
				# 0 for lower bound overlays
				msg = [i, 0, QIEC[i].get("lbound")]
				
				if EXPERIMENT_MODE:
					print("f=",f, ", msg=", msg)
			# [ .. | .. ] .. |
			elif lb_new < QIEC[i].get("ubound") < ub_new:
				# 1 represents upper bound
				msg = [i, 1, QIEC[i].get("ubound")]

				if EXPERIMENT_MODE:
					print("f=",f, ", msg=", msg)

				# re-adjust
				overlap.append(msg)
			# other possibilities:
			# if [ .. | .. | .. ] => should already able to fit in existed EC, hence impossible.
			# if | .. [ .. ] .. | => the size of EC will only be larger or equal to GENERALIZE_RANGE. The new generalized range equals exactly to GENERALIZE_RANGE, so existed EC will never be a subset of it.
			# if [ .. ] .. | .. | => no issue on creating new EC.


		# evaluate EC overlays

		if len(overlap) == 0 or (len(overlap) == 1 and f == 1):
			return True

		elif len(overlap) == 1 and f == 0:
			if overlap[0][1] == 0:
				# if new range overlayed with the lower bound of an EC, update the upper bound of new range to the lower bound of existed EC.
				ub_new = overlap[0][2]
				lb_new = ub_new - GENERALIZE_RANGE
			elif overlap[0][1] == 1:
				# if new range overlayed with the upper bound of an EC, update the lower bound of new range to the upper bound of existed EC.
				lb_new = overlap[0][2]
				ub_new = lb_new + GENERALIZE_RANGE
			else:
				raise Exception("Internal Logic Error detected in func generalize().")
			# run the range asessment again
			return review_overlap(1)

		# overlap 2 or overlap 1 on each side indicate the available range for creating a new EC is smaller than GENERALIZE_RANGE => extend existing EC instead
		elif len(overlap) == 2:
			return False

		else:
			raise Exception("Internal Logic Error detected in func generalize().")

	createNewEC = review_overlap(0)

	if createNewEC:
		pos = create_EC(qi, lb_new, ub_new)
	else:
		pos = extend_EC(qi, overlap[0][0], overlap[1][0], data)

	# return the position of the EC landed within EC_list[qi]
	return pos



def extend_EC_force(qi, sensor_value_qi, ecn):
	'''
	Force enlarge EC to accomodate a record
	'''

	global EC_list, EC_alter_log

	EC_list[qi][ecn]["deprecated"] = True

	lb_new = ub_new = 0
	dist = closest_ecn = closest_ecn_alt = -1

	# find closest nondeprecated EC
	for ec in EC_list[qi]:
		if not ec.get("deprecated"):
			dist_tmp = min( abs(ec.get("ubound") - sensor_value_qi), abs(ec.get("lbound") - sensor_value_qi) )
			if dist > 0 and dist_tmp < dist:
				dist = dist_tmp
				closest_ecn = ec.get("number")
			elif dist < 0:
				dist = dist_tmp
				closest_ecn = ec.get("number")
			elif dist > 0 and dist == dist_tmp:
				closest_ecn_alt = ec.get("number")

	# return a random padding
	def get_padding():
		pad = random.random() * GENERALIZE_RANGE / 3
		return pad

	# check if overlap with existing ECs
	def review_overlap(ecn):
		nonlocal lb_new, ub_new

		change = False
		QIEC = EC_list[qi]

		# check over all ECs of the given QI
		for i in range(len(QIEC)):
			# if EC deprecated, skip
			if QIEC[i].get("deprecated") or i == ecn:
				continue
			# [...]: existed EC ; |...| new generalized range
			# | .. [ .. | .. ]
			if lb_new <= QIEC[i].get("lbound") < ub_new:
				# if new range overlayed with the lower bound of another EC, update the upper bound of new range to the lower bound of that EC.
				ub_new = QIEC[i].get("lbound")
				change = True

			# [ .. | .. ] .. |
			elif lb_new <= QIEC[i].get("ubound") < ub_new:
				# if new range overlayed with the upper bound of another EC, update the lower bound of new range to the upper bound of that EC.
				lb_new = QIEC[i].get("ubound")
				change = True
		return change

	# make compromises : publish with "parent node" (does not count as member of the EC)
	def compromise():
		global Compromised_range_dict

		# find closest nondeprecated and matured EC
		dist = closest_ecn = -1
		for ec in EC_list[qi]:
			if not ec.get("deprecated") and ec.get("member") > THRESHOLD_K:
				dist_tmp = min( abs(ec.get("ubound") - sensor_value_qi), abs(ec.get("lbound") - sensor_value_qi) )
				if dist > 0 and dist_tmp < dist:
					dist = dist_tmp
					closest_ecn = ec.get("number")
				elif dist < 0:
					dist = dist_tmp
					closest_ecn = ec.get("number")

		# if no mature EC available (may occur when a new user started)
		if closest_ecn == -1:
			Compromised_range_dict[qi] = [EC_list[qi][ecn].get("lbound"), EC_list[qi][ecn].get("ubound")]
		# if the actual value is higher than the upper bound of the closest mature EC
		elif sensor_value_qi > EC_list[qi][closest_ecn].get("ubound"):
			Compromised_range_dict[qi] = [EC_list[qi][closest_ecn].get("lbound"), sensor_value_qi + get_padding()]
		# else the actual value must be lower than the lower bound of the closest mature EC
		else:
			Compromised_range_dict[qi] = [sensor_value_qi - get_padding(), EC_list[qi][closest_ecn].get("ubound")]


	# if the EC will become a mature one for publishing after this record joins
	if EC_list[qi][closest_ecn].get("member") >= THRESHOLD_K - 1:
		# use this EC
		if sensor_value_qi > EC_list[qi][closest_ecn].get("ubound"):
			EC_list[qi][closest_ecn]["ubound"] = sensor_value_qi + get_padding()
			lb_new = EC_list[qi][closest_ecn].get("lbound")
			ub_new = EC_list[qi][closest_ecn].get("ubound")

			if review_overlap(closest_ecn):
				EC_list[qi][closest_ecn]["lbound"] = lb_new
				EC_list[qi][closest_ecn]["ubound"] = ub_new
			EC_list[qi][closest_ecn]["member"] += 1

		else:
			EC_list[qi][closest_ecn]["lbound"] = sensor_value_qi - get_padding()
			lb_new = EC_list[qi][closest_ecn].get("lbound")
			ub_new = EC_list[qi][closest_ecn].get("ubound")

			if review_overlap(closest_ecn):
				EC_list[qi][closest_ecn]["lbound"] = lb_new
				EC_list[qi][closest_ecn]["ubound"] = ub_new
			EC_list[qi][closest_ecn]["member"] += 1

		# record the EC change
		EC_alter_log[qi] = [ecn, closest_ecn]
		return closest_ecn

	# check alternative
	elif closest_ecn_alt != -1:
		if EC_list[qi][closest_ecn_alt].get("member") >= THRESHOLD_K - 1:
			# use the alternative EC
			if sensor_value_qi > EC_list[qi][closest_ecn_alt].get("ubound"):
				EC_list[qi][closest_ecn_alt]["ubound"] = sensor_value_qi + get_padding()
				lb_new = EC_list[qi][closest_ecn_alt].get("lbound")
				ub_new = EC_list[qi][closest_ecn_alt].get("ubound")

				if review_overlap(closest_ecn_alt):
					EC_list[qi][closest_ecn_alt]["lbound"] = lb_new
					EC_list[qi][closest_ecn_alt]["ubound"] = ub_new
				EC_list[qi][closest_ecn_alt]["member"] += 1

			else:
				EC_list[qi][closest_ecn_alt]["lbound"] = sensor_value_qi - get_padding()
				lb_new = EC_list[qi][closest_ecn_alt].get("lbound")
				ub_new = EC_list[qi][closest_ecn_alt].get("ubound")

				if review_overlap(closest_ecn_alt):
					EC_list[qi][closest_ecn_alt]["lbound"] = lb_new
					EC_list[qi][closest_ecn_alt]["ubound"] = ub_new
				EC_list[qi][closest_ecn_alt]["member"] += 1

			# record the EC change
			EC_alter_log[qi] = [ecn, closest_ecn_alt]
			return closest_ecn_alt

		else:
			# make compromises : publish with "parent node" (does not count as member of the EC)
			compromise()
			# revive the deprecated EC
			EC_list[qi][ecn]["deprecated"] = False
			return -1
	else:
		# make compromises : publish with "parent node" (does not count as member of the EC)
		compromise()
		# revive the deprecated EC
		EC_list[qi][ecn]["deprecated"] = False
		return -1
		

def _apply_EC_change():
	'''
	apply alternation of EC to other accumulated tuples
	after EC enlargement, check if other QI entries in accumulated tuples match the new enlarged EC
	'''

	global Accumulated_list, EC_alter_log

	# dict EC_alter_log
	#	{ qi : [original_ec_number, new_ec_number] }

	if EC_alter_log:
		
		for qi in EC_alter_log:
			for tuples in Accumulated_list:
				# tuples = [counter, sensor_value, QI_EC_indicator]
				# QI_EC_indicator = tuples[2]
				# if QI_EC_indicator[qi] equals to original_ec_number, replace it with the new_ec_number
				if tuples[2][qi] == EC_alter_log[qi][0]:
					tuples[2][qi] = EC_alter_log[qi][1]

	# clear the change after applying
	EC_alter_log.clear()


def _flush_tuple():
	'''
	Immediately publish the longest accumulated tuple and pop it from accumulation queue
	'''
	global Accumulated_list

	# flag inidcating if this record needs compromising for publication
	isCompromisedMode = False

	# naming respresentation
	counter = Accumulated_list[0][0]
	sensor_value = Accumulated_list[0][1]
	QI_EC_indicator = Accumulated_list[0][2]

	# check all entries in this tuple to see if the EC fitted is ready for publication
	for qi in QI_POS:
		# QI_EC_indicator[qi] : EC pos of the QI
		if EC_list[qi][QI_EC_indicator[qi]].get("member") < THRESHOLD_K:
			# In order to publish the expiring tuple immediately, extend existed EC for this QI
			QI_EC_indicator[qi] = extend_EC_force(qi, sensor_value[qi], QI_EC_indicator[qi])
			if QI_EC_indicator[qi] == -1:
				isCompromisedMode = True

	# publish the tuple
	publish(sensor_value, QI_EC_indicator, isCompromisedMode)
	# pop the published tuple out of accumulation queue
	Accumulated_list.pop(0)
	# apply the modifications of EC to other accumulating tuples
	_apply_EC_change()


def _check_refesh_EC():
	global Init_timer
	
	# flag indicating if a refresh is to be executed
	flush_flag = False

	# Check 1 : Timer reached
	# get current time
	current_time = time.time()

	if current_time - Init_timer > REFRESH_TIMER:
		flush_flag = True

	# Check 2 : EC has too many members
	# for each quasi-identifier
	for qi in QI_POS:
		# for ECs of the selected quasi-identifier
		for ec in EC_list[qi]:
			if ec.get("member") > EC_MAX_HOLDING_MEMBERS:
				flush_flag = True
		
	
	if flush_flag:
		if EXPERIMENT_MODE:
			print("############## Refresh ##############")

		# force output all tuples accumulated
		while Accumulated_list:
			_flush_tuple()

		# wipe all ECs and reset timer
		initialize()



def _tuple_delay_update(latest_counter):
	'''
	Check timeout for accumulated tuples
	'''
	global Accumulated_list

	# if there are tuples accumulating
	if Accumulated_list:
		# get the counter of the longest accumulated tuple
		counter = Accumulated_list[0][0]

		# if about to overtime, update the delay tolerance of accumulated tuples
		if counter <= latest_counter - ACCUMULATION_DELAY_TOLERANCE:
			_flush_tuple()

		# evaluate if other accumulated tuples are ready to publish 
		for tup in Accumulated_list:
			ready = True
			for qi in QI_POS:
				# tup[2][qi] : EC pos of the QI
				if EC_list[qi][tup[2][qi]].get("member") < THRESHOLD_K:
					ready = False

			if ready:
				publish(tup[1], tup[2], False)
				try:
					Accumulated_list.remove(tup)
				except ValueError: # internal error
					raise Exception("Internal Logic Error detected in func _tuple_delay_update().")

	return


def process(counter, sensor_value):
	'''
	The main processing procedure for incoming tuples
	'''
	global Accumulated_list

	# the list for indicating which EC in EC_list does each QI fall in
	QI_EC_indicator = []
	for i in range(max(QI_POS) + 1):
		# default -1: no EC
		QI_EC_indicator.append("-1")

	# flag indicating if the QI values could be accommodated by any EC
	fitEC = False

	# flag indicating if the tuple needs to be accumulated
	toAccumulate = False

	# flag indicating if any QI does not satisfy privacy threshold
	notSatisfied = False

	# go over each quasi-identifier
	for qi in QI_POS:
		# refresh flag for each quasi-identifier
		fitEC = False

		# for ECs of the selected quasi-identifier
		for ec in EC_list[qi]:
			# if there are already matched EC satisfying privacy condition
			if ec.get("lbound") <= sensor_value[qi] and ec.get("ubound") > sensor_value[qi] and not ec.get("deprecated") :
				# record the serial number of the EC
				QI_EC_indicator[qi] = ec.get("number")
				# one new tuple joining the EC
				ec["member"] += 1
				# successfully fits an EC
				fitEC = True
					
		# when no EC could accommodate this QI value
		if not fitEC:
			# create a new EC or extend existed EC based on the new generalized range
			ec_pos = generalize(qi, sensor_value[qi])
			# record the serial number of the EC
			QI_EC_indicator[qi] = ec_pos
			# if new EC created, the tuple definitely needs to be accumulated (until the EC has more than THRESHOLD_K members)
			toAccumulate = True

	#debug
	#print(EC_list)
	
	# for each QI position in the raw input tuple
	for n in QI_POS:
		if QI_EC_indicator[n] == "-1":
			raise Exception("Internal Logic Error detected in func process().")
		elif EC_list[n][QI_EC_indicator[n]].get("member") < THRESHOLD_K:
			toAccumulate = True
			break

	if toAccumulate:
		Accumulated_list.append([counter, sensor_value, QI_EC_indicator])
	else:
		publish(sensor_value, QI_EC_indicator, False)


	# check the necessity of refeshing ECs
	_check_refesh_EC()

	# process tuples accumulated overtime
	_tuple_delay_update(counter)
	
	

def setExperimentMode():
	'''
	Only for experiments used in research paper 
	'''
	global EXPERIMENT_MODE

	EXPERIMENT_MODE = True


def stream_input_file(filepath):
	'''
	Simulate inputs by reading tuples one by one from a given file
	Desgined for experiments in research paper
	'''

	# load config
	read_config()

	# init global variables
	initialize()

	# do parser
	try:

		with open(filepath) as f:
			tuple_counter = 0

			for sensor_tuple in f:

				# interpret string
				#tup = ast.literal_eval(sensor_tuple)
				
				# split data and strip whitespace
				tup = [x.strip() for x in sensor_tuple.split(',')]

				
				# check if the interpreted data is a list
				if not isinstance(tup, list):
					raise SyntaxError

				# designated quasi-identifier position is out of list range
				if max(QI_POS) > len(tup) - 1:
					raise SyntaxError
				
				# interpret QI fields as float numbers
				for qi in QI_POS:
					tup[qi] = float(tup[qi])

				# in order to evaluate average delay of tuple anonymization, attach arrival timestamp to raw data
				# remove later in publish() function
				if EXPERIMENT_MODE:
					tup.append(time.time())

				# process incoming tuple
				process(tuple_counter, tup)

				print("Syslog: Finish reading line ", sensor_tuple)
				
				if EXPERIMENT_MODE:
					print("tup counter: ", tuple_counter)
					print("======== EC_list ========")
					print(EC_list)
					print("=========================")
					if tuple_counter == 420:
						input("** Execution halted: 420th tuple processed! **")

				# incremental counter
				tuple_counter += 1

				# sleep to simulate actual sensor routines	
				time.sleep(1)

	except (ValueError, SyntaxError):
		raise Exception("Error: Invalid input information detected.")



def stream_input(sensor_tuple):
	'''
	The function to call for actual medical devices
	'''

	# load config
	read_config()

	# init global variables
	initialize()
	
	try:
		# split data and strip whitespace
		tup = [x.strip() for x in sensor_tuple.split(',')]

		
		# check if the interpreted data is a list
		if not isinstance(tup, list):
			raise SyntaxError

		# designated quasi-identifier position is out of list range
		if max(QI_POS) > len(tup) - 1:
			raise SyntaxError
		
		# interpret QI fields as float numbers
		for qi in QI_POS:
			tup[qi] = float(tup[qi])


		if EXPERIMENT_MODE:
			tup.append(time.time())

		# process incoming tuple
		process(tuple_counter, tup)

		print("Syslog: Finish reading line ", sensor_tuple)

		# incremental counter
		tuple_counter += 1

	except (ValueError, SyntaxError):
		raise Exception("Error: Invalid input information detected.")