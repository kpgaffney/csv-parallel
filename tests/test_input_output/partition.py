import sys

# Specify input file name and partition number (number of workers)
input_path = sys.argv[1]
partition_num = int(sys.argv[2])



# Create the output file for each partition
partition_files = list() 
for i in range(partition_num):
    pf = open("p{}".format(i), 'w')
    partition_files.append(pf)

# Write each line of the input file to the corresponding output partition file
input_file = open(input_path)
for line in input_file:
    # Use the first entry to decide the partition of the line
    num = int([i.strip() for i in line.split(',')][0])
    # Determine which partition/worker the line is assigned to 
    index = num % partition_num
    partition_files[index].write(line) 

# Close the input and output files
input_file.close()
for ph in partition_files:
    ph.close()
