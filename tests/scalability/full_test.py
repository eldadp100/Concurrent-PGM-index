import shutil
import json
import os
import argparse
import subprocess
import tqdm
import matplotlib.pyplot as plt
import csv
from subprocess import check_output

plt.ioff()  # turns off matplotlib interactive window
_load = 100000
_repetitions = 3
_n_threads = [1, 2, 4, 8, 12, 16, 24, 28]

test_base_path = "/home/eldad/Desktop/OOOOOO/Concurrent-PGM-index/cmake-build-debug/tests/scalability/example_test"
tests_paths = [test_base_path + str(i) for i in range(1, 11)]
results_path = "./pgm_results"

# make dir - create an empty results folder
if (os.path.exists(results_path)):
    shutil.rmtree(results_path)
os.mkdir(results_path)


def test(tests_path, n_threads, repetitions, load):
    results_table = [[0] * len(_n_threads)] * 10  # results_table[test_number][n_threads]

    if (os.path.exists('./tmp_ret.txt')):
        os.remove('./tmp_ret.txt')

    for test_num, test_path in enumerate(tests_path):
        for k_index, k in enumerate(n_threads):
            print(str(k) + " threads - test number " + str(test_num + 1))
            results = []
            for _ in tqdm.tqdm(range(repetitions)):
                try:
                    f = open('./tmp_ret.txt', 'w')
                    subprocess.Popen([test_path, str(k), str(load)], stdout=f).communicate(timeout=10)
                    f.close()
                    f = open('./tmp_ret.txt', 'r')
                    res_str = f.read()
                    results.append(load / int(res_str[7:-1]))
                    f.close()
                except:
                    results.append(-1)

                os.remove('tmp_ret.txt')

            results = sorted(results)
            results_table[test_num][k_index] = {
                "results_list": results,
                "median": results[len(results) // 2],
                "average": sum(results) / len(results)
            }

    return results_table


def plot_graph(results_table_row, n_threads, load, out_path, test_name=None, function="median"):
    x = n_threads
    y = [load / x[function] for x in results_table_row]
    plt.plot(x, y, "o-")
    plt.xticks(x)
    if test_name != None:
        plt.title("Scalability Results for {tn}".format(tn=test_name))
    plt.xlabel("num. of threads")
    plt.ylabel("operations / ms")
    plt.savefig(os.path.join(out_path, "scaling_chart_{}_{}.png".format(function, test_name)))


def export_to_csv(results_table, out_path, function="median"):
    results = [[x[function] for x in row] for row in results_table]
    out_path = os.path.join(out_path, "results_{}.csv".format(function))
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(results)


if __name__ == '__main__':
    _results_table = test(tests_paths, _n_threads, _repetitions, _load)
    for _test_num in range(len(tests_paths)):
        _test_name = "test " + str(_test_num)
        plot_graph(_results_table[_test_num], _n_threads, _load, results_path, test_name=_test_name)
        export_to_csv(_results_table, results_path)
