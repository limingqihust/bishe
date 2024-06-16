import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties 
font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14) 
# 读取文件并解析数据
with open('mapreduce_normal.txt', 'r') as file:
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
# print(ratios)
# 绘制折线图
plt.figure(figsize=(10, 5))
plt.plot(ratios, marker='o')
plt.title('基于均匀分布的网络情况下通信开销和计算开销的比值情况', fontproperties=font)
plt.xlabel('计算任务索引', fontproperties=font)
plt.ylabel('通信开销/计算开销', fontproperties=font)
plt.grid(True)

# 在显示图表前保存到本地文件
plt.savefig('mapreduce_normal_ratio.png', dpi=300, bbox_inches='tight')
total_time = 0
for i in range(len(map_times)):
    total_time += map_times[i] + shuffle_times[i]
print(total_time) # 4669495.685000003