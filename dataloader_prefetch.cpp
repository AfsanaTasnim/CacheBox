// dataloader_prefetch.cpp
// Build: g++ -O2 -std=c++20 -pthread dataloader_prefetch.cpp -o dataloader
// Run (generate): ./dataloader --generate data.bin --num-records 5000000 --record-bytes 512
// Run (load):     ./dataloader --file data.bin --record-bytes 512 --batch-records 1024 --prefetch 16 --readers 4

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;

static uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch())
        .count();
}

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    // Producer: blocks when full. Returns false if closed.
    bool push(T item) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_full_.wait(lk, [&] { return closed_ || q_.size() < capacity_; });
        if (closed_) return false;
        q_.push_back(std::move(item));
        cv_not_empty_.notify_one();
        return true;
    }

    // Consumer: blocks when empty. Returns nullopt if closed and empty.
    std::optional<T> pop() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_empty_.wait(lk, [&] { return closed_ || !q_.empty(); });
        if (q_.empty()) return std::nullopt;
        T item = std::move(q_.front());
        q_.erase(q_.begin());
        cv_not_full_.notify_one();
        return item;
    }

    void close() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

private:
    size_t capacity_;
    std::mutex mu_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_not_full_;
    bool closed_ = false;
    std::vector<T> q_;
};

struct Batch {
    std::vector<uint8_t> bytes; // batch payload (batch_records * record_bytes)
    size_t batch_records = 0;
};

struct Args {
    // dataset
    std::string file;
    bool generate = false;
    size_t num_records = 0;
    size_t record_bytes = 512;

    // loader
    size_t batch_records = 1024;
    size_t prefetch_batches = 16;
    size_t readers = 4;

    // run control
    size_t max_batches = 0; // 0 = consume entire file
};

static void usage(const char* prog) {
    std::cerr
        << "Usage:\n"
        << "  Generate: " << prog << " --generate <file> --num-records N --record-bytes B\n"
        << "  Load:     " << prog << " --file <file> --record-bytes B --batch-records R "
        << "--prefetch P --readers K [--max-batches M]\n";
}

static bool parse_args(int argc, char** argv, Args& a) {
    auto require = [&](int& i) -> std::string {
        if (i + 1 >= argc) {
            usage(argv[0]);
            std::exit(2);
        }
        return argv[++i];
    };

    for (int i = 1; i < argc; i++) {
        std::string s = argv[i];
        if (s == "--file") a.file = require(i);
        else if (s == "--generate") { a.generate = true; a.file = require(i); }
        else if (s == "--num-records") a.num_records = std::stoull(require(i));
        else if (s == "--record-bytes") a.record_bytes = std::stoull(require(i));
        else if (s == "--batch-records") a.batch_records = std::stoull(require(i));
        else if (s == "--prefetch") a.prefetch_batches = std::stoull(require(i));
        else if (s == "--readers") a.readers = std::stoull(require(i));
        else if (s == "--max-batches") a.max_batches = std::stoull(require(i));
        else if (s == "--help") { usage(argv[0]); return false; }
        else { std::cerr << "Unknown arg: " << s << "\n"; usage(argv[0]); return false; }
    }

    if (a.file.empty()) {
        std::cerr << "Missing --file or --generate\n";
        return false;
    }
    if (a.record_bytes == 0) {
        std::cerr << "--record-bytes must be > 0\n";
        return false;
    }
    if (a.generate) {
        if (a.num_records == 0) {
            std::cerr << "For --generate, provide --num-records\n";
            return false;
        }
    }
    if (!a.generate) {
        if (!std::filesystem::exists(a.file)) {
            std::cerr << "File does not exist: " << a.file << "\n";
            return false;
        }
        if (a.batch_records == 0 || a.prefetch_batches == 0 || a.readers == 0) {
            std::cerr << "batch_records/prefetch/readers must be > 0\n";
            return false;
        }
    }
    return true;
}

static void generate_file(const std::string& path, size_t num_records, size_t record_bytes) {
    int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        perror("open");
        std::exit(1);
    }

    std::vector<uint8_t> rec(record_bytes);
    for (size_t i = 0; i < num_records; i++) {
        // deterministic-ish content
        uint64_t x = 1469598103934665603ULL ^ (i * 1099511628211ULL);
        for (size_t j = 0; j < record_bytes; j++) {
            x ^= (x << 13); x ^= (x >> 7); x ^= (x << 17);
            rec[j] = static_cast<uint8_t>(x & 0xFF);
        }
        ssize_t w = ::write(fd, rec.data(), rec.size());
        if (w != (ssize_t)rec.size()) {
            perror("write");
            ::close(fd);
            std::exit(1);
        }
    }

    ::fsync(fd);
    ::close(fd);
    std::cout << "Generated " << num_records << " records (" << (num_records * record_bytes)
              << " bytes) at " << path << "\n";
}

static uint64_t cheap_checksum(const uint8_t* p, size_t n) {
    // very cheap to simulate a compute step (do not use as crypto hash)
    uint64_t s = 0;
    for (size_t i = 0; i < n; i += 64) s = (s * 1315423911ULL) ^ p[i];
    return s;
}

int main(int argc, char** argv) {
    Args a;
    if (!parse_args(argc, argv, a)) return 2;

    if (a.generate) {
        uint64_t t0 = now_ns();
        generate_file(a.file, a.num_records, a.record_bytes);
        uint64_t t1 = now_ns();
        std::cout << "Generate time: " << (double)(t1 - t0) / 1e9 << " s\n";
        return 0;
    }

    // Determine record count from file size
    auto fsz = std::filesystem::file_size(a.file);
    if (fsz % a.record_bytes != 0) {
        std::cerr << "File size is not a multiple of record_bytes.\n"
                  << "file_size=" << fsz << ", record_bytes=" << a.record_bytes << "\n";
        return 1;
    }
    const size_t total_records = (size_t)(fsz / a.record_bytes);

    int fd = ::open(a.file.c_str(), O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }

    BoundedQueue<Batch> q(a.prefetch_batches);
    std::atomic<size_t> next_record{0};
    std::atomic<bool> stop{false};

    std::atomic<size_t> produced_batches{0};
    std::atomic<size_t> produced_records{0};

    auto reader_fn = [&](size_t tid) {
        (void)tid;
        while (!stop.load(std::memory_order_relaxed)) {
            size_t start = next_record.fetch_add(a.batch_records);
            if (start >= total_records) break;

            size_t want = std::min(a.batch_records, total_records - start);
            Batch b;
            b.batch_records = want;
            b.bytes.resize(want * a.record_bytes);

            // Read records as a single contiguous pread (records are fixed-size and contiguous)
            off_t off = (off_t)(start * a.record_bytes);
            size_t to_read = b.bytes.size();
            uint8_t* dst = b.bytes.data();

            size_t got = 0;
            while (got < to_read) {
                ssize_t r = ::pread(fd, dst + got, to_read - got, off + (off_t)got);
                if (r < 0) { perror("pread"); stop.store(true); break; }
                if (r == 0) { stop.store(true); break; }
                got += (size_t)r;
            }
            if (stop.load()) break;

            if (!q.push(std::move(b))) break;
            produced_batches.fetch_add(1);
            produced_records.fetch_add(want);

            if (a.max_batches && produced_batches.load() >= a.max_batches) {
                stop.store(true);
                break;
            }
        }
    };

    auto consumer_fn = [&]() {
        uint64_t checksum_acc = 0;
        size_t consumed_batches = 0;
        size_t consumed_records = 0;

        while (true) {
            auto item = q.pop();
            if (!item.has_value()) break;
            const Batch& b = *item;
            checksum_acc ^= cheap_checksum(b.bytes.data(), b.bytes.size());
            consumed_batches++;
            consumed_records += b.batch_records;

            if (a.max_batches && consumed_batches >= a.max_batches) {
                stop.store(true);
                break;
            }
        }

        std::cout << "Consumer checksum_acc=" << checksum_acc << "\n";
        std::cout << "Consumed batches=" << consumed_batches
                  << ", records=" << consumed_records << "\n";
    };

    uint64_t t0 = now_ns();

    std::vector<std::thread> readers;
    readers.reserve(a.readers);
    for (size_t i = 0; i < a.readers; i++) readers.emplace_back(reader_fn, i);

    std::thread consumer(consumer_fn);

    for (auto& th : readers) th.join();
    q.close();
    consumer.join();

    uint64_t t1 = now_ns();
    ::close(fd);

    double secs = (double)(t1 - t0) / 1e9;
    size_t recs = produced_records.load();
    double mb = (double)(recs * a.record_bytes) / (1024.0 * 1024.0);

    std::cout << "Produced records=" << recs << " of total_records=" << total_records << "\n";
    std::cout << "Time: " << secs << " s\n";
    std::cout << "Throughput: " << (recs / secs) << " records/s, " << (mb / secs) << " MiB/s\n";

    return 0;
}
