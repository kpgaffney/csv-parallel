import sys 

input_file_path = sys.argv[1]
partition_num = int(sys.argv[2])
partition_size = int(sys.argv[3])

input_file = open(input_file_path, 'w')
for i in range(partition_num):
    for k in range(partition_size):
        input_file.write("{},{}\n".format(i, k))

input_file.close()

