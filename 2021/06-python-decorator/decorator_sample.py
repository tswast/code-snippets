# Copyright 2021 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import functools


def memoize(func):
    answers = {}
    
    @functools.wraps(func)
    def memoized(n):
        if n not in answers:
            answers[n] = func(n)
        return answers[n]

    return memoized


@memoize
def fibonacci(n):
    if n <= 2:
        return 1
    return fibonacci(n - 1) + fibonacci(n - 2)

start = time.perf_counter()
fib = fibonacci(40)
end = time.perf_counter()

print(fib)
print(end - start)

