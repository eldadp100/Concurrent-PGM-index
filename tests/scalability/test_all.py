import subprocess
import os
import time

import tqdm


load = 100000
repetitions = 10


def run_test_get_elapsed_time(test_path, k_threads):
    results = []
    for _ in tqdm.tqdm(range(repetitions)):
        try:
            f = open('./tmp_ret.txt', 'w')
            subprocess.Popen([test_path, str(k_threads), str(load)], stdout=f).communicate(timeout=30)
            f.close()
            f = open('./tmp_ret.txt', 'r')
            res_str = f.read()
            results.append(int(res_str[7:-1]))
        except:
            results.append(-1)
    a = sorted(results)[repetitions // 2]
    return a, load / a


if (os.path.exists('./tmp_ret.txt')):
    os.remove('./tmp_ret.txt')

results = [[0 for _ in range(8)] for _ in range(4)]
for i in [1, 2, 3]:
    test_path = r"/home/eldad/Desktop/OOOOOO/Concurrent-PGM-index/cmake-build-debug/tests/scalability/example_test{}".format(
        i)
    for ki, k in enumerate([1, 2, 3, 4, 5, 6, 7, 8]):
        a, b = run_test_get_elapsed_time(test_path, k)
        print("test {} threads {} - {}".format(i, k, b))
        results[i-1][ki] = b

out_path = "./results.csv"
if (os.path.exists(out_path)):
    os.remove(out_path)
with open(out_path, "w") as f:
    f.write("\n".join([", ".join([str(x) for x in results[i]]) for i in range(10)]))
