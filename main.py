from sys import argv
from data_pool import DataPool
from time import time


def main():
    time_start = time()
    if len(argv) != 2:
        print("参数无效")
    if argv[1] == "-f":
        DataPool.init(by_file=True)
        table_names = DataPool.read_file()
    else:
        DataPool.init(by_file=False)
        table_names = DataPool.read_instruction(argv[1])

    DataPool.normal_fit_(table_names)
    # DataPool.multi_process_fit(table_names)
    time_end = time()
    DataPool.save2db()
    print('耗时', time_end - time_start, 's')


if __name__ == '__main__':
    main()