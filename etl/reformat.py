import sys
import os

i_f = sys.argv[1]
o_dir = sys.argv[2]

with open(i_f, 'r') as i_file:
    with open(os.path.join(o_dir, i_f.split('/')[-1]), "w") as o_file:
        for line in i_file:
            print(line.replace(",", ";").replace("(null)", ""), end="", file=o_file)
