import shutil
import json
import os
import argparse
import subprocess

import matplotlib.pyplot as plt


plt.ioff() # turns off matplotlib interactive window
load = 100000

def test(test_path, n_threads = [1, 2, 4, 8, 12, 16, 24, 28], repeatitions=15):
    info = {}
    info["number_of_threads_tested"] = n_threads

    if (os.path.exists('./tmp_ret.txt')):
        os.remove('tmp_ret.txt')
    for k in n_threads:
        results = []
        for _ in range(repeatitions):
            f = open('./tmp_ret.txt', 'w')
            subprocess.Popen([test_path, str(k), str(load)], stdout=f).wait()
            f.close()
            f = open('./tmp_ret.txt', 'r')
            results.append(load / int(f.read()[7:-1]))
            f.close()
            os.remove('tmp_ret.txt')

        print("Done for " + str(k))
        info["{_k}_threads_results_sorted".format(_k=k)] = sorted(results)

    return info


def export_test_results(out_path, info, test_name=None):
    """ This function exports the scalablity results as a json 
        containing all executions information and a visual graph. 
        This export overrides out_path and deletes all existing files in it. """

    if (os.path.exists(out_path)):
        shutil.rmtree(out_path)
    os.mkdir(out_path)

    # export json of info
    with open(os.path.join(out_path, "full_info.json"), "w") as jf:
        json.dump(info, jf)

    # calculate and export a visual graph
    n_threads = info["number_of_threads_tested"]
    graph_info = []
    for k in n_threads:
        # the assiciated value of k is the median of the executions done on k threads.
        res_lst = info["{_k}_threads_results_sorted".format(_k=k)]
        graph_info.append([k, res_lst[len(res_lst) // 2]])

    x, y = zip(*graph_info)
    plt.plot(x, y, "o-")
    plt.xticks(x)
    if test_name != None:
        plt.title("Scalability Results for {tn}".format(tn=test_name))
    plt.xlabel("num. of threads")
    plt.ylabel("operations / ms")
    plt.savefig(os.path.join(out_path, "scale.png"))
    # plt.close()


if __name__ == "__main__":
    # terminal args
    parser = argparse.ArgumentParser()
    parser.add_argument('test_path', type=str, default=None)
    parser.add_argument('test_name', type=str, default=None)
    parser.add_argument('out_path', type=str, default="./test_results")
    args = parser.parse_args()

    # execute
    if args.test_path is None:
        exit()
    info = test(args.test_path)
    export_test_results(args.out_path, info, test_name=args.test_name)
