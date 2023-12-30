import time

class Timer:
    def eclapse(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            time_cost = end_time - start_time
            print(f"Function {func.__name__} eclapsed {time_cost} seconds.")
            return result
        return wrapper

def square_unit_transform(value, unit):
    '''
    单位转换函数, 将value从unit单位转换到m^2单位.
    '''
    if unit == "m^2":
        return value
    elif unit == "mm^2":
        return value / 1000000
    elif unit == "cm^2":
        return value / 10000
    elif unit == "km^2":
        return value * 1000000
    elif unit == "in^2":
        return value * 0.00064516
    elif unit == "ft^2":
        return value * 0.092903
    elif unit == "yd^2":
        return value * 0.836127
    else:
        raise ValueError("Unsupported unit: ", unit)

