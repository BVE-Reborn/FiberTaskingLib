/**
 * FiberTaskingLib - A tasking library that uses fibers for efficient task switching
 *
 * This library was created as a proof of concept of the ideas presented by
 * Christian Gyrling in his 2015 GDC Talk 'Parallelizing the Naughty Dog Engine Using Fibers'
 *
 * http://gdcvault.com/play/1022186/Parallelizing-the-Naughty-Dog-Engine
 *
 * FiberTaskingLib is the legal property of Adrian Astley
 * Copyright Adrian Astley 2015 - 2018
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ftl/atomic_counter.h"
#include "ftl/task_scheduler.h"

#include <gtest/gtest.h>

namespace FutureTests {
	std::size_t doubler(ftl::TaskScheduler* scheduler, std::size_t const v) {
		return v * 2;
	}

	void double_by_ref(ftl::TaskScheduler* scheduler, int& v) {
		v = v * 2;
	}

	int& return_by_ref(ftl::TaskScheduler* scheduler) {
		static int v = 2;
		return v;
	}

	template<class A, class B>
	auto multiplier(A&& a, B&& b) -> decltype(a * b) {
		return a * b;
	}
	
	void main(ftl::TaskScheduler* scheduler, void*) {
		ftl::Future<std::size_t> f1 = scheduler->AddTypedTask(doubler, 2);

		ASSERT_EQ(f1.get(), 4);

		int double_ref_val = 3;
		ftl::Future<void> f2 = scheduler->AddTypedTask(double_by_ref, std::ref(double_ref_val));

		f2.get();
		ASSERT_EQ(double_ref_val, 6);

		ftl::Future<int&> f3 = scheduler->AddTypedTask(return_by_ref);
		int& ref_val = f3.get();
		ASSERT_EQ(&ref_val, &return_by_ref(scheduler));

		float a = 4;
		float b = 2;
		ftl::Future<float> template_by_lambda = scheduler->AddTypedTask([&](ftl::TaskScheduler*) { return multiplier(a, b); });

		ASSERT_EQ(template_by_lambda.get(), 8);

		std::vector<std::size_t> data;
		std::vector<ftl::Future<std::size_t>> futures;
		std::vector<std::size_t> result;
		data.reserve(10000);
		futures.reserve(10000);
		int val = 0;
		std::generate_n(std::back_inserter(data), 10000, [&]{ return ++val; });

		for (std::size_t v : data) {
			futures.emplace_back(scheduler->AddTypedTask(doubler, v));
		}

		std::transform(futures.begin(), futures.end(), std::back_inserter(result),
			[](ftl::Future<std::size_t>& f){ return f.get(); });

		for (std::size_t i = 0; i < 10000; ++i) {
			ASSERT_EQ(data[i] * 2, result[i]);
		}
	}
}

TEST(FunctionalTests, Futures) {
	ftl::TaskScheduler taskScheduler;
	taskScheduler.Run(400, FutureTests::main);
}
