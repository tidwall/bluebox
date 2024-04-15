# Bluebox

Bluebox is a tiny Redis clone for the purpose of displaying the capabilities of
[Neco](https://github.com/tidwall/neco), a network concurrency library for C.

Supports SET, GET, DEL, and PING.

## Using

```
make
./bluebox
```

Now you can connect to port 9999 using a Redis client, such as `redis-cli`.

```
redis-cli -p 9999
> SET hello jello
OK
> GET hello
"jello"
```

## Performance

The following benchmarks show the performance of Bluebox and Redis using the 
the [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) tool.

These were produced on Linux with a AMD Ryzen 9 5950X 16-Core processor.

### Redis (memtier_benchmark)

```
redis-server --appendonly no --save ""
```

```
memtier_benchmark --hide-histogram -p 6379
```

```
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        18903.87          ---          ---         0.96435         0.93500         1.87900         3.11900      1455.93
Gets       188831.01         0.00    188831.01         0.96258         0.93500         1.87100         3.16700      7355.78
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     207734.88         0.00    188831.01         0.96274         0.93500         1.87100         3.16700      8811.71
```

### Bluebox (memtier_benchmark)

```
./bluebox
```

```
./memtier_benchmark --hide-histogram -p 9999
```

```
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets        44894.50          ---          ---         0.40609         0.40700         0.46300         0.75100      3457.66
Gets       448451.62         0.00    448451.62         0.40516         0.40700         0.46300         0.74300     17469.13
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals     493346.12         0.00    448451.62         0.40524         0.40700         0.46300         0.74300     20926.79
```

