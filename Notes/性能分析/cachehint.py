# Python
total_access = 0
cache_hits = 0

with open('qps1.log', 'r') as file:
    for line in file:
        if "scan线程访问page数:" in line:
            total_access += int(line.split(":")[1].strip())
        elif "缓存命中数:" in line:
            cache_hits = int(line.split(":")[1].strip())

percentage = (cache_hits / total_access) * 100

print("qps1:")
print(f"总访问数: {total_access}")
print(f"缓存命中数: {cache_hits}")
print(f"百分比: {percentage}%")

total_access = 0
cache_hits = 0

with open('qps2.log', 'r') as file:
    for line in file:
        if "scan线程访问page数:" in line:
            total_access += int(line.split(":")[1].strip())
        elif "缓存命中数:" in line:
            cache_hits = int(line.split(":")[1].strip())

percentage = (cache_hits / total_access) * 100

print("qps2:")
print(f"总访问数: {total_access}")
print(f"缓存命中数: {cache_hits}")
print(f"百分比: {percentage}%")

total_access = 0
cache_hits = 0

with open('qps3.log', 'r') as file:
    for line in file:
        if "scan线程访问page数:" in line:
            total_access += int(line.split(":")[1].strip())
        elif "缓存命中数:" in line:
            cache_hits = int(line.split(":")[1].strip())

percentage = (cache_hits / total_access) * 100

print("qps3:")
print(f"总访问数: {total_access}")
print(f"缓存命中数: {cache_hits}")
print(f"百分比: {percentage}%")