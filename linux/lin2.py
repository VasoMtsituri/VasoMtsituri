import os
stream = os.popen('ls -la')
output = stream.readlines()
print(len(output))
print(output)
print(output[1])

with open('ls.txt', 'w') as f:
    for line in output:
        f.write(line + "\n")

print('Done')