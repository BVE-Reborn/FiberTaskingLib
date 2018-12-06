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

#define FTL_INCLUDED_TASK_SCHEDULER_H

#include "ftl/fiber.h"
#include "ftl/future.h"
#include "ftl/task.h"
#include "ftl/thread_abstraction.h"
#include "ftl/typedefs.h"
#include "ftl/wait_free_queue.h"

#include <atomic>
#include <climits>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

namespace ftl {

class AtomicCounter;

enum class EmptyQueueBehavior {
	// Spin in a loop, actively searching for tasks
	Spin,
	// Same as spin, except yields to the OS after each round of searching
	Yield,
	// Puts the thread to sleep. Will be woken when more tasks are added to the remaining awake threads.
	Sleep
};

/**
 * A class that enables task-based multithreading.
 *
 * Underneath the covers, it uses fibers to allow cores to work on other tasks
 * when the current task is waiting on a synchronization atomic
 */
class TaskScheduler {
public:
	TaskScheduler();
	~TaskScheduler();

private:
	enum : size_t {
		FTL_INVALID_INDEX = UINT_MAX
	};

	std::size_t m_numThreads;
	std::vector<ThreadType> m_threads;	
	
	std::size_t m_fiberPoolSize;
	/* The backing storage for the fiber pool */
	Fiber *m_fibers;
	/**
	 * An array of atomics, which signify if a fiber is available to be used. The indices of m_waitingFibers
	 * correspond 1 to 1 with m_fibers. So, if m_freeFibers[i] == true, then m_fibers[i] can be used.
	 * Each atomic acts as a lock to ensure that threads do not try to use the same fiber at the same time
	 */
	std::atomic<bool> *m_freeFibers;
	
	std::atomic<bool> m_initialized;
	std::atomic<bool> m_quit;

	std::atomic<EmptyQueueBehavior> m_emptyQueueBehavior;
	
	enum class FiberDestination {
		None = 0,
		ToPool = 1,
		ToWaiting = 2,
	};

	/**
	* Holds a task that is ready to to be executed by the worker threads
	* Counter is the counter for the task(group). It will be decremented when the task completes
	*/
	struct TaskBundle {
		Task TaskToExecute;
		AtomicCounter *Counter;
	};

	struct PinnedWaitingFiberBundle {
		PinnedWaitingFiberBundle(std::size_t fiberIndex, AtomicCounter *counter, uint targetValue)
			: FiberIndex(fiberIndex), 
			  Counter(counter), 
			  TargetValue(targetValue) {
		}

		std::size_t FiberIndex;
		AtomicCounter *Counter;
		uint TargetValue;
	};

	struct ThreadLocalStorage {
		ThreadLocalStorage()
			: ThreadFiber(),
			  CurrentFiberIndex(FTL_INVALID_INDEX),
			  OldFiberIndex(FTL_INVALID_INDEX),
			  OldFiberDestination(FiberDestination::None),
			  TaskQueue(),
			  LastSuccessfulSteal(1),
			  OldFiberStoredFlag(nullptr), 
			  FailedQueuePopAttempts(0) {
		}

	public:
		/**
		* The current fiber implementation requires that fibers created from threads finish on the same thread where they started
		*
		* To accommodate this, we have save the initial fibers created in each thread, and immediately switch
		* out of them into the general fiber pool. Once the 'mainTask' has finished, we signal all the threads to
		* start quitting. When they receive the signal, they switch back to the ThreadFiber, allowing it to 
		* safely clean up.
		*/
		Fiber ThreadFiber;
		/* The index of the current fiber in m_fibers */
		std::size_t CurrentFiberIndex;
		/* The index of the previously executed fiber in m_fibers */
		std::size_t OldFiberIndex;
		/* Where OldFiber should be stored when we call CleanUpPoolAndWaiting() */
		FiberDestination OldFiberDestination;
		/* The queue of waiting tasks */
		WaitFreeQueue<TaskBundle> TaskQueue;
		/* The last queue that we successfully stole from. This is an offset index from the current thread index */
		std::size_t LastSuccessfulSteal;
		std::atomic<bool> *OldFiberStoredFlag;
		std::vector<std::pair<std::size_t, std::atomic<bool> *> > ReadyFibers;
		std::atomic_flag ReadFibersLock;
		uint32 FailedQueuePopAttempts;
		/**
		* This lock is used with the CV below to put threads to sleep when there
		* is no work to do. It also protects accesses to FailedQueuePopAttempts.
		*
		* We *could* use an atomic for FailedQueuePopAttempts, however, we still need
		* to lock when changing the value, because spurious wakes of the CV could
		* cause a thread to fail to wake up. See https://stackoverflow.com/a/36130475
		* So, if we need to lock, there is no reason to have the overhead of an atomic as well.
		*/
		std::mutex FailedQueuePopLock;
		std::condition_variable FailedQueuePopCV;

	private:
		/* Cache-line pad */
		char pad[64];
	};
	/**
	 * c++ Thread Local Storage is, by definition, static/global. This poses some problems, such as multiple TaskScheduler
	 * instances. In addition, with the current fiber implementation, we have no way of telling the compiler to disable TLS optimizations, so we
	 * have to fake TLS anyhow. 
	 *
	 * During initialization of the TaskScheduler, we create one ThreadLocalStorage instance per thread. Threads index into
	 * their storage using m_tls[GetCurrentThreadIndex()]
	 */
	ThreadLocalStorage *m_tls;

	/** 
	 * We friend AtomicCounter so we can keep AddReadyFiber() private
	 * This makes the public API cleaner
	 */
	friend class AtomicCounter;


public:
	/**
	 * Initializes the TaskScheduler and then starts executing 'mainTask'
	 *
	 * NOTE: Run will "block" until 'mainTask' returns. However, it doesn't block in the traditional sense; 'mainTask' is created as a Fiber.
	 * Therefore, the current thread will save it's current state, and then switch execution to the the 'mainTask' fiber. When 'mainTask'
	 * finishes, the thread will switch back to the saved state, and Run() will return.
	 *
	 * @param fiberPoolSize     The size of the fiber pool. The fiber pool is used to run new tasks when the current task is waiting on a counter
	 * @param mainTask          The main task to run
	 * @param mainTaskArg       The argument to pass to 'mainTask'
	 * @param threadPoolSize    The size of the thread pool to run. 0 corresponds to NumHardwareThreads()
	 */
	void Run(uint fiberPoolSize, TaskFunction mainTask, void *mainTaskArg = nullptr, uint threadPoolSize = 0, EmptyQueueBehavior behavior = EmptyQueueBehavior::Spin);

	/**
	 * Adds a task to the internal queue.
	 *
	 * @param task       The task to queue
	 * @param counter    An atomic counter corresponding to this task. Initially it will be set to 1. When the task completes, it will be decremented.
	 */
	uint AddTask(Task task, AtomicCounter* counter = nullptr);
	/**
	 * Adds a group of tasks to the internal queue
	 *
	 * @param numTasks    The number of tasks
	 * @param tasks       The tasks to queue
	 * @param counter     An atomic counter corresponding to the task group as a whole. Initially it will be set to numTasks. When each task completes, it will be decremented.
	 */
	void AddTasks(uint numTasks, Task *tasks, AtomicCounter *counter = nullptr);

	template<class F, class... Args>
	auto AddTypedTask(F& f, Args&&... args)
	    -> Future<decltype(f(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))> {
		using RetVal = decltype(f(std::declval<TaskScheduler*>(), std::forward<Args>(args)...));

		Promise<RetVal>* promise = detail::create_promise(this, nullptr, 0, f, std::forward<Args>(args)...);

		Future<RetVal> future = promise->get_future();

		AddTask(Task{detail::TypeSafeTask<RetVal>, promise}, promise->counter());

		return future;
	}

	template<class F, class... Args>
	auto AddTypedTask(AtomicCounter* counter, F& f, Args&&... args) 
	    -> Future<decltype(f(std::declval<TaskScheduler*>(), std::forward<Args>(args)...))> {
		if (!counter) {
			return AddTypedTask(f, std::forward<Args>(args)...);
		}

		using RetVal = decltype(f(std::forward<Args>(args)...));

		Promise<RetVal>* promise = detail::create_promise(this, nullptr, 0, f, std::forward<Args>(args)...);

		Promise<RetVal> future = promise->get_future();

		uint const old_val = AddTask(Task{detail::TypeSafeTask<RetVal>, promise}, promise->counter());

		promise->wait_val(old_val);

		return std::move(future);
	}

	/**
	 * Yields execution to another task until counter == value
	 *
	 * @param counter             The counter to check
	 * @param value               The value to wait for
	 * @param pinToCurrentThread  If true, the task invoking this call will not resume on a different thread
	 */
	void WaitForCounter(AtomicCounter *counter, uint value, bool pinToCurrentThread = false);

	/**
	 * Gets the 0-based index of the current thread
	 * This is useful for m_tls[GetCurrentThreadIndex()]
	 *
	 * @return    The index of the current thread
	 */
	FTL_NOINLINE_POSIX std::size_t GetCurrentThreadIndex();

	/**
	 * Set the behavior for how worker threads handle an empty queue
	 *
	 * @param behavior    
	 * @return     
	 */
	void SetEmptyQueueBehavior(EmptyQueueBehavior behavior) {
		m_emptyQueueBehavior.store(behavior, std::memory_order_relaxed);
	}

private:
	/**
	 * Pops the next task off the queue into nextTask. If there are no tasks in the
	 * the queue, it will return false.
	 *
	 * @param nextTask    If the queue is not empty, will be filled with the next task
	 * @return            True: Successfully popped a task out of the queue
	 */
	bool GetNextTask(TaskBundle *nextTask);
	/**
	 * Gets the index of the next available fiber in the pool
	 *
	 * @return    The index of the next available fiber in the pool
	 */
	std::size_t GetNextFreeFiberIndex();
	/**
	 * If necessary, moves the old fiber to the fiber pool or the waiting list
	 * The old fiber is the last fiber to run on the thread before the current fiber
	 */
	void CleanUpOldFiber();

	/**
	 * Add a fiber to the "ready list". Fibers in the ready list will be resumed the next time a fiber goes searching for a new task
	 *
	 * @param pinnedThreadIndex    The index of the thread this fiber is pinned to. If not pinned, this will equal std::numeric_limits<std::size_t>::max()
	 * @param fiberIndex           The index of the fiber to add
	 * @param fiberStoredFlag      A flag used to signal if the fiber has been successfully switched out of and "cleaned up"
	 */
	void AddReadyFiber(std::size_t pinnedThreadIndex, std::size_t fiberIndex, std::atomic<bool> *fiberStoredFlag);

	/**
	 * The threadProc function for all worker threads
	 *
	 * @param arg    An instance of ThreadStartArgs
	 * @return       The return status of the thread
	 */
	static FTL_THREAD_FUNC_DECL ThreadStart(void *arg);
	/**
	* The fiberProc function that wraps the main fiber procedure given by the user
	*
	* @param arg    An instance of TaskScheduler
	*/
	static void MainFiberStart(void *arg);
	/**
	 * The fiberProc function for all fibers in the fiber pool
	 *
	 * @param arg    An instance of TaskScheduler
	 */
	static void FiberStart(void *arg);
};

inline void wait_on_counter_forwarder(TaskScheduler* s, AtomicCounter *counter, uint const value, bool const pinToCurrentThread) {
	s->WaitForCounter(counter, value, pinToCurrentThread);
}

} // End of namespace ftl
