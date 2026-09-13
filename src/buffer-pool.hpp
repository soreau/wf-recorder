#pragma once

#include <array>
#include <mutex>
#include <atomic>
#include <functional>
#include <iostream>
#include <type_traits>

/* Live Miracast: keep this tiny. A large queue is multi-second lag; blocking
 * in next_capture() stalls the Wayland/ICC cycle and makes the software
 * cursor on the captured output feel delayed. */
#define MAX_BUFFERS 4
#define INITIAL_BUFFERS_SIZE 2
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
    // Never blocks: if the encoder is behind, drop oldest *queued* frames so
    // the ICC/Wayland loop can continue (critical for local cursor feel).
    T& next_capture()
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
                /* Drop queued encode frames until `next` is free. Do not wait —
                 * blocking here stalls wl_display_dispatch / ICC and lags the
                 * software cursor on the captured monitor. */
                static bool warned = false;
                int guard = static_cast<int>(bufs_size) + 1;
                while (!bufs[next]->ready_capture() && guard-- > 0)
                {
                    if (encode_idx == capture_idx)
                        break;
                    /* Prefer dropping a queued (available) slot. If the writer
                     * still holds encode_idx, force-release anyway — better a
                     * glitch than multi-second cursor lag / abort. */
                    if (!warned)
                    {
                        std::cerr << "buffer pool full; dropping queued frames"
                                  << std::endl;
                        warned = true;
                    }
                    bufs[encode_idx]->available = false;
                    bufs[encode_idx]->released = true;
                    encode_idx = (encode_idx + 1) % static_cast<int>(bufs_size);
                    next = (capture_idx + 1) % static_cast<int>(bufs_size);
                }
                if (!bufs[next]->ready_capture())
                {
                    /* Last resort: reuse `next` in place. */
                    bufs[next]->available = false;
                    bufs[next]->released = true;
                }
            }
        }
        bufs[capture_idx]->released = false;
        bufs[capture_idx]->available = true;
        capture_idx = next;
        bufs[capture_idx]->released = true;
        bufs[capture_idx]->available = false;
        return *bufs[capture_idx];
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
