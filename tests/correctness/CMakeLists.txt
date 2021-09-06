set(examples
        ctest1
        ctest2
        ctest3
        ctest4
        ctest5
        )

foreach (example ${examples})
    add_executable(example_${example} ${example}.cpp)
    target_link_libraries(example_${example} pgmindexlib)
endforeach (example)

set(_code "#include <immintrin.h>
           unsigned long long a;
           int main() { auto b = _pext_u64(a, a); return 0; }")
check_cxx_source_compiles("${_code}" HAS_BMI2)

