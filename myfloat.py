import struct

def float_to_float16(value):
    """
    将整数或浮点数转换为 IEEE 754 半精度浮点数 (16位浮点数)
    :param value: int 或 float
    :return: int 表示的16位浮点数
    """
    # 特殊值处理
    if value == 0:
        return 0  # 正负零均为 0
    if value != value:  # NaN
        return 0b1111110000000000  # NaN表示
    if value == float('inf'):  # 正无穷
        return 0b0111110000000000
    if value == float('-inf'):  # 负无穷
        return 0b1111110000000000

    # 获取符号位
    sign = 0 if value > 0 else 1
    abs_value = abs(value)

    # 提取指数和尾数
    import math
    exponent = int(math.floor(math.log2(abs_value)))  # 计算以2为底的指数
    mantissa = abs_value / (2 ** exponent) - 1  # 去掉隐含的 1

    # 转换为半精度浮点数的格式
    bias = 15  # 偏置值
    half_exponent = exponent + bias

    # 次正规数处理
    if half_exponent <= 0:
        # 将超小值转为次正规数
        mantissa = abs_value / (2 ** (-14))
        half_mantissa = int(mantissa * (2 ** 10))  # 转为10位尾数
        return (sign << 15) | half_mantissa

    # 正常范围处理
    if half_exponent >= 0b11111:  # 指数过大，表示无穷大
        return (sign << 15) | 0b1111100000000000

    # 转换为正常范围浮点数
    half_mantissa = int(mantissa * (2 ** 10))  # 转换为10位尾数
    return (sign << 15) | (half_exponent << 10) | half_mantissa


def float16_to_binary(value):
    """
    将 16位浮点数格式化为二进制表示
    :param value: int 表示的16位浮点数
    :return: str 表示的二进制数
    """
    return f"{value:016b}"

# 测试示例
if __name__ == "__main__":
    bits = struct.unpack('L', struct.pack('f', 3.141592653589793))[0]
    print(bits)
    print(format(bits, '032b'))