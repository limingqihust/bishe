import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties 


font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc") 
# 读取文件并解析数据
uniform_map_times = []
uniform_shuffle_times = []
exponential_map_times = []
exponential_shuffle_times = []
pareto_map_times = []
pareto_shuffle_times = []

with open('mapreduce_normal.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            uniform_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            uniform_shuffle_times.append(float(parts[1]))
            
with open('mapreduce_exponential.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            exponential_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            exponential_shuffle_times.append(float(parts[1]))
    
with open('mapreduce_pareto.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            pareto_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            pareto_shuffle_times.append(float(parts[1]))

# 计算比值
uniform_ratios = [m / s for m, s in zip(uniform_shuffle_times, uniform_map_times)]
exponential_ratios = [m / s for m, s in zip(exponential_shuffle_times, uniform_map_times)]
pareto_ratios = [m / s for m, s in zip(pareto_shuffle_times, pareto_map_times)]

# 绘制折线图
plt.figure(figsize=(10, 5))
plt.plot(exponential_ratios, marker='x',)
plt.xlabel('任务次数', fontproperties=font, fontsize = 14)
plt.ylabel('通信开销/计算开销', fontproperties=font, fontsize = 14)
plt.grid(True)
plt.savefig('mapreduce_exponential_ratio.png', dpi=300, bbox_inches='tight')

plt.figure(figsize=(10, 5))
plt.plot(pareto_ratios, marker='x')
plt.xlabel('任务次数', fontproperties=font, fontsize = 14)
plt.ylabel('通信开销/计算开销', fontproperties=font, fontsize = 14)
plt.grid(True)
plt.savefig('mapreduce_pareto_ratio.png', dpi=300, bbox_inches='tight')


plt.figure(figsize=(10, 5))
plt.plot(uniform_ratios, marker='x')
plt.xlabel('任务次数', fontproperties=font, fontsize = 14)
plt.ylabel('通信开销/计算开销', fontproperties=font, fontsize = 14)
plt.grid(True)
plt.savefig('mapreduce_uniform_ratio.png', dpi=300, bbox_inches='tight')