


if __name__ == "__main__":

	total_sum = 0
	lines = 0

	with open("output_delay.txt") as f:
		for record in f:
			total_sum += float(record)
			lines += 1
			print(total_sum)

	print("Average delay time: ", total_sum / lines)
