set(examples
        test1
        test2
        test3
        test4
        test5
        test6
        test7
        test8
        test9
        test10
        )

foreach (example ${examples})
    add_executable(example_${example} ${example}.cpp)
    target_link_libraries(example_${example} pgmindexlib)
endforeach (example)

set(_code "#include <immintrin.h>
           unsigned long long a;
           int main() { auto b = _pext_u64(a, a); return 0; }")
check_cxx_source_compiles("${_code}" HAS_BMI2)

