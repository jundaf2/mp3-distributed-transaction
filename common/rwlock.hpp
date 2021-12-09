//
// Created by ubuntu on 12/7/21.
//
#ifndef MP3_DISTRIBUTED_TRANSACTIONS_RWLOCK_HPP
#define MP3_DISTRIBUTED_TRANSACTIONS_RWLOCK_HPP
// file: rwlock.hpp
#pragma once

#include <mutex>
#include <condition_variable>

namespace rwlock
{
    class ReadWriteLock {
        private:
            int readWaiting = 0;
            int writeWaiting = 0;
            int reading = 0;
            int writing = 0;
            std::mutex mx;
            std::condition_variable cond;
        public:
            ReadWriteLock() = default;
            ~ReadWriteLock() = default;
            ReadWriteLock(const ReadWriteLock &) = delete;
            ReadWriteLock & operator=(const ReadWriteLock &) = delete;

            ReadWriteLock(const ReadWriteLock &&) = delete;
            ReadWriteLock & operator=(const ReadWriteLock &&) = delete;



            void readLock() {
                std::unique_lock<std::mutex>lock(mx);
                ++readWaiting;
                cond.wait(lock,[&](){return writing <= 0;});
                ++reading;
                --readWaiting;
            }

            void writeLock() {
                std::unique_lock<std::mutex>lock(mx);
                ++writeWaiting;
                cond.wait(lock, [&]() {return readWaiting <=0 && reading <= 0 && writing <= 0; });
                ++writing;
                --writeWaiting;
            }

            void readUnLock() {
                std::unique_lock<std::mutex>lock(mx);
                --reading;
                if(reading<=0)
                    cond.notify_one();
            }

            void writeUnLock() {
                std::unique_lock<std::mutex>lock(mx);
                --writing;
                cond.notify_all();
            }
    };
}


#endif //MP3_DISTRIBUTED_TRANSACTIONS_RWLOCK_HPP
