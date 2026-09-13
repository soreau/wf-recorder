#pragma once

#include <array>
#include <mutex>
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <thread>
#include <type_traits>

/* Live capture (Miracast) must not queue tens of frames — that is seconds of
 * lag. Cap the pool and drop queued frames when the encoder falls behind. */
#define MAX_BUFFERS 8
#define INITIAL_BUFFERS_SIZE 2
/* Keep old name as alias for any external references. */
#define MAX_FRAME_FAILURES MAX_BUFFERS

class buffer_pool_buf
{
public:
    bool ready_capture() const
    {
        return released;
    }

    bool ready_encode() const
    {
        return available;
    }

    std::atomic<bool> released{true}; // if the buffer can be used to store new pending frames
    std::atomic<bool> available{false}; // if the buffer can be used to feed the encoder
};

template <class T, int N>
class buffer_pool
{
public:
    static_assert(std::is_base_of<buffer_pool_buf, T>::value, "T must be subclass of buffer_pool_buf");

    buffer_pool()
    {
        for (size_t i = 0; i < bufs_size; ++i) {
            bufs[i] = new T;
        }
    }

    ~buffer_pool()
    {
        for (size_t i = 0; i < bufs_size; ++i) {
            delete bufs[i];
        }
    }

    size_t size() const
    {
        return bufs_size;
    }

    const T* at(size_t i) const
    {
        return bufs[i];
    }

    T& capture()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return *bufs[capture_idx];
    }

    T& encode()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return *bufs[encode_idx];
    }

    // Signal that the current capture buffer has been successfully obtained
    // from the compositor and select the next buffer to capture in.
    T& next_capture()
    {
        bool warned_drop = false;
        bool warned_wait = false;
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(mutex);
                int next = (capture_idx + 1) % static_cast<int>(bufs_size);
                if (!bufs[next]->ready_capture())
                {
                    if (bufs_size < MAX_BUFFERS)
                    {
                        bufs_size++;
                        std::cerr << "bufs_size: " << bufs_size << std::endl;
                        bufs[bufs_size - 1] = new T;
                        next = (capture_idx + 1) % static_cast<int>(bufs_size);
                    }
                    else
                    {
                        /* Drop oldest *queued* encode frames (available, not yet
                         * taken by the writer) to keep live latency low. */
                        int guard = static_cast<int>(bufs_size);
                        while (!bufs[next]->ready_capture() && guard-- > 0)
                        {
                            if (encode_idx == capture_idx)
                                break;
                            if (!bufs[encode_idx]->ready_encode())
                                break; // writer owns this slot
                            if (!warned_drop)
                            {
                                std::cerr << "buffer pool full; dropping queued frames"
                                          << std::endl;
                                warned_drop = true;
                            }
                            bufs[encode_idx]->available = false;
                            bufs[encode_idx]->released = true;
                            encode_idx = (encode_idx + 1) % static_cast<int>(bufs_size);
                            next = (capture_idx + 1) % static_cast<int>(bufs_size);
                        }
                        if (!bufs[next]->ready_capture())
                        {
                            if (!warned_wait)
                            {
                                std::cerr << "buffer pool full; brief wait for encoder"
                                          << std::endl;
                                warned_wait = true;
                            }
                            next = -1;
                        }
                    }
                }
                if (next >= 0)
                {
                    bufs[capture_idx]->released = false;
                    bufs[capture_idx]->available = true;
                    capture_idx = next;
                    bufs[capture_idx]->released = true;
                    bufs[capture_idx]->available = false;
                    return *bufs[capture_idx];
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    // Signal that the encode buffer has been submitted for encoding
    // and select the next buffer for encoding.
    T& next_encode()
    {
        std::lock_guard<std::mutex> lock(mutex);
        bufs[encode_idx]->available = false;
        bufs[encode_idx]->released = true;
        encode_idx = (encode_idx + 1) % static_cast<int>(bufs_size);
        return *bufs[encode_idx];
    }

private:
    std::mutex mutex;
    std::array<T*, MAX_BUFFERS> bufs;
    size_t bufs_size = INITIAL_BUFFERS_SIZE;
    int capture_idx = 0; // head
    int encode_idx = 0; // tail
};
