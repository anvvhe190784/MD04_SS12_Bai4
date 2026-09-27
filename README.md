# MD04_SS12_Bai4 - Real-Time Promotion Price Updates with Redis Pub/Sub

This project implements **Bài tập 4: Cập nhật giá khuyến mãi thời gian thực với Redis Pub/Sub** while inheriting all features and architectures from **Bài tập 1** (Redis Cache-Aside), **Bài tập 2** (Asynchronous Order Processing with Kafka), and **Bài tập 3** (Anti-Overselling with Redisson Distributed Lock).

## Architecture Overview

```
                                      +--------------------------+
                                      |    Promotion-Service     |
                                      |       (Publisher)        |
                                      +-------------+------------+
                                                    |
                                                    | Redis PUBLISH "promotion-updates"
                                                    v
                                      +--------------------------+
                                      |   Redis Message Broker   |
                                      | Channel:promotion-updates|
                                      +-------------+------------+
                                                    |
                                                    | Redis SUBSCRIBE
                                                    v
                                      +--------------------------+
+--------------------------+          |     Product-Service      |
|       Client / Web       | <------> |       (Subscriber)       |
+--------------------------+          +-------------+------------+
                                                    |
                                            Cache Evict & Reload
                                                    v
                                      +--------------------------+
                                      |  Redis Cache / MySQL DB  |
                                      +--------------------------+
```

### Inherited & Core Features:
1. **Bài tập 1: Redis Cache-Aside & TTL**
   - `@Cacheable(value = "products", key = "#id")` with 30-minute TTL.
   - GenericJackson2JsonRedisSerializer for JSON caching.
   - Cache eviction via `@CacheEvict(value = "products", key = "#id")`.
2. **Bài tập 2: Asynchronous Kafka Order Processing**
   - Non-blocking order creation via `KafkaTemplate.send("order-events", orderId, event)`.
   - Dual consumer groups: `email-service-group` (mock email confirmation) and `inventory-service-group` (inventory deduction).
3. **Bài tập 3: Anti-Overselling Flash Sale with Redisson Distributed Lock**
   - Distributed lock key: `lock:product:{id}` via `redissonClient.getLock(...)`.
   - Concurrency protection: `tryLock(3000ms, 5000ms, TimeUnit.MILLISECONDS)` with `finally { lock.unlock(); }`.
   - Guaranteed atomic stock validation and deduction under high concurrency.
4. **Bài tập 4: Real-Time Promotion Updates with Redis Pub/Sub**
   - **Promotion-Service (Publisher):** `PromotionPublisher` publishes `PromotionUpdateMessage` to Redis channel `promotion-updates`.
   - **Product-Service (Subscriber):** `PromotionUpdateSubscriber` implements `MessageListener`, receives channel messages, and immediately evicts `productId` from the Redis cache (`cacheManager.getCache("products").evict(productId)`).
   - **Result:** Price update is broadcasted and invalidated across all nodes in under **100ms** (well below the 1-second requirement). Next product retrieval fetches fresh price from the database.

---

## Tech Stack & Dependencies

- **Java**: 21 LTS
- **Framework**: Spring Boot 3.4.3
- **Cache & Pub/Sub**: Redis (Lettuce & Redisson 3.44.0)
- **Message Broker**: Apache Kafka (Dual Consumer Groups)
- **Database**: H2 / MySQL Compatible (Spring Data JPA)
- **Build Tool**: Gradle 9.7.1

---

## REST API Endpoints

### 1. Promotions (Bài tập 4)
- **POST** `/api/promotions/apply`
  - Request:
    ```json
    {
      "productId": 1,
      "promotionName": "Super Summer Sale 50% Off",
      "newPrice": 12500000.00
    }
    ```
  - Response (200 OK):
    ```json
    {
      "productId": 1,
      "promotionName": "Super Summer Sale 50% Off",
      "newPrice": 12500000.00,
      "broadcastedAt": "2026-09-27T15:40:00Z"
    }
    ```

### 2. Flash Sale (Bài tập 3)
- **POST** `/api/flash-sale/purchase`
  - Request:
    ```json
    {
      "productId": 1,
      "customerName": "Alice",
      "quantity": 1
    }
    ```

### 3. Orders (Bài tập 2)
- **POST** `/api/orders`
  - Request:
    ```json
    {
      "productId": 1,
      "customerEmail": "customer@example.com",
      "quantity": 2
    }
    ```

### 4. Products (Bài tập 1)
- **GET** `/api/products/{id}`: Cache-aside read (<10ms on hit, ~200ms on miss).
- **PUT** `/api/products/{id}`: Update product & trigger `@CacheEvict`.

---

## Test Verification

Run automated test suite:
```bash
./gradlew test
```

Verification includes:
- `RedisPubSubPromotionRealtimeTest`: Validates end-to-end Redis Pub/Sub broadcast and cache invalidation within < 100ms.
- `RedissonFlashSaleConcurrencyTest`: 100 parallel threads competing for 1 remaining stock; exactly 1 succeeds, 99 rejected.
- `OrderAsyncKafkaIntegrationTest`: Non-blocking Kafka producer and dual consumer validation.
- `ProductCacheTest`: Cache-aside hit/miss and TTL assertions.
