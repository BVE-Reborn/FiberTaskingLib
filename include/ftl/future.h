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

#pragma once

#ifndef FTL_INCLUDED_TASK_SCHEDULER_H
	#error "Cannot include future.h directly, include task_scheduler.h"
#endif

#include <atomic>
#include <functional>

#include "ftl/atomic_counter.h"

namespace ftl {
	template<class T>
	class Future;

	template<class T>
	class Promise;

	inline void wait_on_counter_forwarder(TaskScheduler* s, AtomicCounter *counter, uint value, bool pinToCurrentThread = false);

	//////////////////////////////
	// ftl::detail::SharedState //
	//////////////////////////////

	namespace detail {
		template<class T>
		struct SharedState {
			std::function<T(TaskScheduler*)> m_functor;
			T m_data;
			std::exception_ptr m_exception;
			std::unique_ptr<AtomicCounter> m_counter;
			std::atomic<std::uint8_t> m_refs;
		};

		template<>
		struct SharedState<void> {
			std::function<void(TaskScheduler*)> m_functor;
			std::exception_ptr m_exception;
			std::unique_ptr<AtomicCounter> m_counter;
			std::atomic<std::uint8_t> m_refs;
		};
	}

	/////////////////
	// ftl::Future //
	/////////////////

	namespace detail {
		template<class T>
		class PromiseBase;

		template<class T>
		class FutureBase {
		public:
			FutureBase() = default;
			FutureBase(FutureBase const&) = delete;
			FutureBase(FutureBase&& that) noexcept
			     : m_state(that.m_state),
			       m_scheduler(that.m_scheduler),
			       m_counter(that.m_counter),
			       m_wait_val(that.m_wait_val) {
				that.m_state = nullptr;
				that.m_scheduler = nullptr;
				that.m_counter = nullptr;
			};
			FutureBase& operator=(FutureBase const&) = delete;
			FutureBase& operator=(FutureBase&& that) noexcept {
				swap(*this, that);
			};

			bool valid() const noexcept {
				return m_state != nullptr;
			}

			bool ready() const noexcept {
				return valid() && m_state->m_refs.load(std::memory_order_acquire) == 1;
			}

			void wait(bool const pinToThread = false) const {
				if (!valid()) {
					throw std::logic_error("Future must have state in order to wait() or get()");
				}
				if (!ready()) {
					wait_on_counter_forwarder(m_scheduler, m_counter, m_wait_val, pinToThread);
				}
			}

			// get() in children

			~FutureBase() {
				if (m_state) {
					std::uint8_t const old = m_state->m_refs.fetch_sub(1, std::memory_order_acq_rel);
					if (old == 1) {
						// Work is done, promise doesn't exist, just delete.
						discard_state();
					}
					else {
						// Work isn't done don't delete anything. Promise will delete it.
					}
				}
			}

			template<class FB_T>
			friend void swap(FutureBase<FB_T>& lhs, FutureBase<FB_T>& rhs) noexcept;

		protected:
			FutureBase(SharedState<T>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
				: m_state(state),
			      m_scheduler(scheduler),
			      m_counter(counter),
			      m_wait_val(waitVal) {}

			void conditional_throw() {
				if (m_state && m_state->m_exception != nullptr) {
					std::rethrow_exception(m_state->m_exception);
				}
			}

			void discard_state() {
				delete this->m_state;
				this->m_state = nullptr;
			}

			SharedState<T>* m_state;
			TaskScheduler* m_scheduler;
			AtomicCounter* m_counter;
			std::size_t m_wait_val;
		};

		template<class FB_T>
		void swap(FutureBase<FB_T>& lhs, FutureBase<FB_T>& rhs) noexcept {
			using std::swap;

			swap(lhs.m_state, rhs.m_state);
			swap(lhs.m_scheduler, rhs.m_scheduler);
			swap(lhs.m_counter, rhs.m_counter);
			swap(lhs.m_wait_val, rhs.m_wait_val);
		}
	}

	template<class T>
	class Future : public detail::FutureBase<T> {
	public:
		T get(bool pinToThread = false) {
			// Work in progress, wait
			this->wait(pinToThread);

			this->conditional_throw();

			T tmp = std::move(this->m_state->m_data);

			// We are the only owners of the shared state because wait() will only return after the entire task is done.
			this->discard_state();

			return std::move(tmp);
		}

	private:
		friend detail::PromiseBase<T>;

		Future(detail::SharedState<T>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: detail::FutureBase<T>(state, scheduler, counter, waitVal) {}
	};

	template<class T>
	class Future<T&> : public detail::FutureBase<T&> {
	public:
		T& get(bool pinToThread = false) {
			// Work in progress, wait
			this->wait(pinToThread);

			this->conditional_throw();

			T& tmp = this->m_state->m_data;

			// We are the only owners of the shared state because wait() will only return after the entire task is done.
			this->discard_state();

			return tmp;
		}

	private:
		friend detail::PromiseBase<T&>;

		Future(detail::SharedState<T&>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: detail::FutureBase<T&>(state, scheduler, counter, waitVal) {}


	};

	template<>
	class Future<void> : public detail::FutureBase<void> {
	public:
		void get(bool pinToThread = false) {
			// Work in progress, wait
			this->wait(pinToThread);

			this->conditional_throw();

			// We are the only owners of the shared state because wait() will only return after the entire task is done.
			this->discard_state();
		}

	private:
		friend detail::PromiseBase<void>;

		Future(detail::SharedState<void>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: FutureBase<void>(state, scheduler, counter, waitVal) {}
	};

	//////////////////
	// ftl::Promise //
	//////////////////

	namespace detail {
		template<class T>
		class PromiseBase {
		public:
			PromiseBase() = default;
			PromiseBase(PromiseBase const&) = delete;
			PromiseBase(PromiseBase&& that) noexcept
				: m_state(that.m_state),
				  m_scheduler(that.m_scheduler),
				  m_counter(that.m_counter),
				  m_wait_val(that.m_wait_val) {
				that.m_state = nullptr;
				that.m_scheduler = nullptr;
				that.m_counter = nullptr;
			};
			PromiseBase& operator=(PromiseBase const&) = delete;
			PromiseBase& operator=(PromiseBase&& that) noexcept {
				swap(*this, that);
			};

		protected:
			bool valid() const noexcept {
				return m_state != nullptr;
			}

			void assert_state() const {
				if (!valid()) {
					throw std::logic_error("Promises must have state to set a value");
				}
			}

			void clear_state() {
				std::uint8_t const old = m_state->m_refs.fetch_sub(1, std::memory_order_acq_rel);
				if (old == 1) {
					// No future waiting for result, just kill state off.
					delete m_state;
				}
				m_state = nullptr;
			}

		public:
			Future<T> get_future() {
				assert_state();

				std::uint8_t const old = m_state->m_refs.fetch_add(1, std::memory_order_acq_rel);
				assert(old == 1);

				return Future<T>(m_state, m_scheduler, m_counter, m_wait_val);
			}

			// set_value in children

			void set_exception(std::exception_ptr e_ptr) {
				assert_state();

				m_state->m_exception = e_ptr;

				clear_state();
			}

			std::function<T(TaskScheduler*)> function() const {
				assert_state();

				return m_state->m_functor;
			}

			AtomicCounter* counter() const {
				assert_state();

				return m_counter;
			}

			void wait_val(std::size_t v) {
				m_wait_val = v;
			}

			~PromiseBase() {
				if (valid()) {
					clear_state();
				}
			}

			template<class PB_T>
			friend void swap(PromiseBase<PB_T>& lhs, PromiseBase<PB_T>& rhs) noexcept;

		protected:
			PromiseBase(SharedState<T>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
				: m_state(state),
				m_scheduler(scheduler),
				m_counter(counter),
				m_wait_val(waitVal) {}

			SharedState<T>* m_state;
			TaskScheduler* m_scheduler;
			AtomicCounter* m_counter;
			std::size_t m_wait_val;
		};

		template<class PB_T>
		void swap(PromiseBase<PB_T>& lhs, PromiseBase<PB_T>& rhs) noexcept {
			using std::swap;

			swap(lhs.m_state, rhs.m_state);
			swap(lhs.m_scheduler, rhs.m_scheduler);
			swap(lhs.m_counter, rhs.m_counter);
			swap(lhs.m_wait_val, rhs.m_wait_val);
		}

		// Forward decl for friend functions below.
		template<class F, class... Args>
		auto create_promise(TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal, F& func, Args&&... args)
			-> Promise<decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))>*;
	}



	template<class T>
	class Promise : public detail::PromiseBase<T> {
	public:
		void set_value(T const& value) {
			this->assert_state();

			this->m_state->m_data = value;

			this->clear_state();
		}
		void set_value(T&& value) {
			this->assert_state();

			this->m_state->m_data = std::move(value);

			this->clear_state();
		}
	private:
		template<class F, class... Args>
		friend auto detail::create_promise(TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal, F& func, Args&&... args)
			-> Promise<decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))>*;

		Promise(detail::SharedState<T>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: detail::PromiseBase<T>(state, scheduler, counter, waitVal) {}
	};

	template<class T>
	class Promise<T&> : public detail::PromiseBase<T&> {
	public:
		void set_value(T& value) {
			this->assert_state();

			this->m_state->m_data = value;

			this->clear_state();
		}
	private:
		template<class F, class... Args>
		friend auto detail::create_promise(TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal, F& func, Args&&... args)
			-> Promise<decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))>*;

		Promise(detail::SharedState<T&>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: detail::PromiseBase<T&>(state, scheduler, counter, waitVal) {}
	};

	template<>
	class Promise<void> : public detail::PromiseBase<void> {
	public:
		void set_value() {
			this->assert_state();

			this->clear_state();
		}
	private:
		template<class F, class... Args>
		friend auto detail::create_promise(TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal, F& func, Args&&... args)
			-> Promise<decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))>*;

		Promise(detail::SharedState<void>* state, TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal)
			: detail::PromiseBase<void>(state, scheduler, counter, waitVal) {}
	};

	namespace detail {
		template<class F, class... Args>
		auto create_promise(TaskScheduler* scheduler, AtomicCounter* counter, std::size_t waitVal, F& func, Args&&... args)
			-> Promise<decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))>* {
			using RetVal = decltype(func(std::declval<TaskScheduler*>(), std::forward<Args>(args)...));

			SharedState<RetVal>* state = new SharedState<RetVal>();
			state->m_refs.store(1, std::memory_order_release);
			state->m_functor = std::bind(func, std::placeholders::_1, std::forward<Args>(args)...);

			AtomicCounter* used_counter;
			if (!counter) {
				state->m_counter = std::unique_ptr<AtomicCounter>(new AtomicCounter{scheduler, 0, 1, nullptr});
				used_counter = state->m_counter.get();
			}
			else {
				used_counter = counter;
			}

			return new Promise<RetVal>(state, scheduler, used_counter, waitVal);
		}


		template<class T>
		void TypeSafeTask(TaskScheduler* scheduler, void *promise_vptr) {
			auto* promise_ptr = reinterpret_cast<Promise<T>*>(promise_vptr);
			auto& promise = *promise_ptr;

			try {
				promise.set_value(promise.function()(scheduler));
			}
			catch (...) {
				promise.set_exception(std::current_exception());
			}

			delete promise_ptr;
		}

		template<>
		inline void TypeSafeTask<void>(TaskScheduler* scheduler, void *promise_vptr) {
			auto* promise_ptr = reinterpret_cast<Promise<void>*>(promise_vptr);
			auto& promise = *promise_ptr;

			try {
				promise.function()(scheduler);
				promise.set_value();
			}
			catch (...) {
				promise.set_exception(std::current_exception());
			}

			delete promise_ptr;
		}
	}
}
