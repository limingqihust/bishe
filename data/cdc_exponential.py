import matplotlib.pyplot as plt

# 读取文件并解析数据
with open('cdc_exponential.txt', 'r') as file:
    lines = file.readlines()
    map_times = []
    shuffle_times = []
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            shuffle_times.append(float(parts[1]))

# 计算比值
ratios = [m / s for m, s in zip(map_times, shuffle_times)]
# 绘制折线图
plt.figure(figsize=(10, 5))
plt.plot(ratios, marker='o')
plt.title('Ratio of MAP to SHUFFLE Stage Execution Times')
plt.xlabel('Operation Index')
plt.ylabel('MAP/SHUFFLE Ratio')
plt.grid(True)

# 在显示图表前保存到本地文件
plt.savefig('cdc_exponential_ratio.png', dpi=300, bbox_inches='tight')

total_time = 0
for i in range(len(map_times)):
    total_time += map_times[i] + shuffle_times[i]
print(total_time) # 1637348.660000001