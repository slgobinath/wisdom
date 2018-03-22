package com.javahelps.wisdom.core;

import java.util.concurrent.locks.ReentrantLock;

public class ThreadBarrier {

    private ReentrantLock lock = new ReentrantLock();

    public void pass() {
        if (lock.isLocked()) {
            lock.lock();
            lock.unlock();
        }
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }
}
