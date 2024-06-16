import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties 
font = FontProperties(fname=r"c:\windows\fonts\simsun.ttc", size=14) 
# 读取文件并解析数据

cdc_uniform_map_times = []
cdc_uniform_shuffle_times = []
static_cdc_uniform_map_times = []
static_cdc_uniform_shuffle_times = []
    
with open('cdc_normal.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            cdc_uniform_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            cdc_uniform_shuffle_times.append(float(parts[1]))
    
with open('static_cdc_normal.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.strip().split()
        if parts[0] == 'MAP':
            static_cdc_uniform_map_times.append(float(parts[1]) * 0.5 * 1e7)
        elif parts[0] == 'SHUFFLE':
            static_cdc_uniform_shuffle_times.append(float(parts[1]))

# 计算比值
cdc_uniform_ratios = [m / s for m, s in zip(cdc_uniform_map_times, cdc_uniform_shuffle_times)]
static_cdc_uniform_ratios = [m / s for m, s in zip(static_cdc_uniform_map_times, static_cdc_uniform_shuffle_times)]


# 绘制折线图
plt.figure(figsize=(10, 5))
plt.plot(cdc_uniform_ratios, marker='x', label = "DCDC方案")
plt.plot(static_cdc_uniform_ratios, marker='s', label = "CDC方案")
# plt.legend(loc='best', prop=font)
plt.legend(loc='best', fontsize=10, prop=font)
# plt.title('网络带宽呈uniform分布时编码分布式计算方案和动态编码分布式计算方案的通信开销和计算开销的比值', fontproperties=font)
plt.xlabel('任务次数', fontproperties=font)
plt.ylabel('通信开销/计算开销', fontproperties=font)
plt.grid(True)

# 在显示图表前保存到本地文件
plt.savefig('static_dynamic_uniform_ratio.png', dpi=300, bbox_inches='tight')

mean = sum(cdc_uniform_ratios) / len(cdc_uniform_ratios)
variance = sum((x - mean) ** 2 for x in cdc_uniform_ratios) / (len(cdc_uniform_ratios) - 1) 
print(variance)
mean = sum(static_cdc_uniform_ratios) / len(static_cdc_uniform_ratios)
variance = sum((x - mean) ** 2 for x in static_cdc_uniform_ratios) / (len(static_cdc_uniform_ratios) - 1) 
print(variance)