package systems.comodal.collision.cache;

import static systems.comodal.collision.cache.AtomicLogCounters.MAX_COUNT;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

/**
 * @param <K> the type of keys used to map to values
 * @param <L> the type of loaded values before being mapped to type V
 * @param <V> the type of mapped values
 * @author James P. Edwards
 */
final class SparseCollisionCache<K, L, V> extends BaseCollisionCache<K, L, V> {

  private final int capacity;
  private final boolean strict;
  private final AtomicInteger size;

  SparseCollisionCache(
      final int capacity, final boolean strictCapacity,
      final Class<V> valueType,
      final int maxCollisionsShift,
      final V[][] hashTable,
      final IntFunction<V[]> getBucket,
      final AtomicLogCounters counters,
      final ToIntFunction<K> hashCoder,
      final BiPredicate<K, V> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> finalizer) {
    super(valueType, maxCollisionsShift, hashTable, getBucket, counters, hashCoder, isValForKey,
        loader, finalizer);
    this.capacity = capacity;
    this.strict = strictCapacity;
    this.size = new AtomicInteger();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public <I> V getAggressive(final K key, final Function<K, I> loader,
      final BiFunction<K, I, V> mapper) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        final I loaded = loader.apply(key);
        if (loaded == null) {
          return null;
        }
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) {  // Nothing to swap with and over capacity.
            return mapper.apply(key, loaded);
          }
        } else if (size.get() > capacity) {
          return checkDecayAndProbSwap(counterOffset, collisions, key, loaded, mapper);
        }
        final V val = mapper.apply(key, loaded);
        do {
          collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
          if (collision == null) {
            counters.initializeOpaque(counterOffset + index);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            counters.increment(counterOffset + index);
            return collision;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return checkDecayAndProbSwap(counterOffset, collisions, key, val);
      }
      if (isValForKey.test(key, collision)) {
        counters.increment(counterOffset + index);
        return collision;
      }
    } while (++index < collisions.length);
    final I loaded = loader.apply(key);
    return loaded == null ? null
        : checkDecayAndProbSwap(counterOffset, collisions, key, loaded, mapper);
  }

  /**
   * This method assumes a full bucket or (XOR) over capacity and checks for both (AND).
   */
  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) { // Assume over capacity.
          final V val = mapper.apply(key, loaded);
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              counters.increment(counterIndex);
              return collision;
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          counters.increment(counterIndex);
          return collision;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = mapper.apply(key, loaded);
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final V val) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) { // Assume over capacity.
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              counters.increment(counterIndex);
              return collision;
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          counters.increment(counterIndex);
          return collision;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  V checkDecayAndSwap(final int counterOffset, final V[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    if (size.get() > capacity) {
      return checkDecayAndProbSwap(counterOffset, collisions, key, loadAndMap);
    }
    int index = 0;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (index == 0) {
            // If not strict, allow first entry into first collision index.
            if (strict && size.get() > capacity) {
              // Nothing to swap with and over capacity.
              return val;
            }
          } else if (size.get() > capacity) {
            decaySwapAndDrop(counterOffset, counterOffset + index, collisions, val);
            return val;
          }
          do {
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterOffset + index);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              counters.increment(counterOffset + index);
              return collision;
            }
          } while (++index == collisions.length);
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, val);
          return val;
        }
        if (isValForKey.test(key, collision)) {
          counters.increment(counterOffset + index);
          return collision;
        }
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (size.get() > capacity) {
            decaySwapAndDrop(counterOffset, counterOffset + collisions.length, collisions, val);
            return val;
          }
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, val);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (index == 0) {
            // If not strict, allow first entry into first collision index.
            if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
              return val;
            }
          } else if (size.get() > capacity) {
            COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
            counters.initializeOpaque(minCounterIndex);
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          do {
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterOffset + index);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              counters.increment(counterOffset + index);
              return collision;
            }
          } while (++index == collisions.length);
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          counters.decay(counterOffset, counterOffset + collisions.length, minCounterIndex);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          counters.increment(counterIndex);
          return collision;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  private void decaySwapAndDrop(final int counterOffset, final int maxCounterIndex,
      final V[] collisions, final V val) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    do {
      int count = counters.getOpaque(counterIndex);
      if (count == 0) {
        COLLISIONS.setOpaque(collisions, counterIndex - counterOffset, val);
        counters.initializeOpaque(counterIndex);
        while (++counterIndex < maxCounterIndex) {
          count = counters.getOpaque(counterIndex);
          if (count > 0) {
            counters.setOpaque(counterIndex, count >> 1);
            continue;
          }
          if (size.getAndDecrement() <= capacity) {
            size.getAndIncrement();
            continue;
          }
          for (int collisionIndex = counterIndex - counterOffset,
              nextCollisionIndex = collisionIndex + 1; ; ++collisionIndex, ++nextCollisionIndex) {
            if (nextCollisionIndex == collisions.length) {
              COLLISIONS.setOpaque(collisions, collisionIndex, null);
              return;
            }
            Object next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
            if (next == null) {
              COLLISIONS.setOpaque(collisions, collisionIndex, null);
              next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
              if (next == null
                  || COLLISIONS.compareAndExchange(collisions, collisionIndex, null, next)
                  != null) {
                return;
              }
            } else {
              COLLISIONS.setOpaque(collisions, collisionIndex, next);
            }
            // Counter misses may occur during this transition.
            count = counters.getOpaque(++counterIndex);
            counters.setOpaque(counterIndex - 1, count >> 1);
          }
        }
        return;
      }
      // Counter misses may occur between these two calls.
      counters.setOpaque(counterIndex, count >> 1);
      if (count < minCount) {
        minCount = count;
        minCounterIndex = counterIndex;
      }
    } while (++counterIndex < maxCounterIndex);
    COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
    counters.initializeOpaque(minCounterIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
            return val;
          }
        } else if (size.get() > capacity) {
          break;
        }
        do {
          collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision; // If another thread raced to PUT, let it win.
          }
        } while (++index < collisions.length && size.get() <= capacity);
        break;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final V witness = (V) COLLISIONS.compareAndExchange(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        if (isValForKey.test(key, witness)) {
          return witness; // If another thread raced to PUT, let it win.
        }
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (index == 0) {  // Strict capacity checked above.
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              return collision; // If another thread raced to PUT, let it win.
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (collision == val) {
          return val;
        }

        if (isValForKey.test(key, collision)) {
          final V witness = (V) COLLISIONS.compareAndExchange(collisions, index, collision, val);
          if (witness == collision) {
            return val;
          }
          if (isValForKey.test(key, witness)) {
            return witness; // If another thread raced to PUT, let it win.
          }
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
            return val;
          }
        } else if (size.get() > capacity) {
          break;
        }
        do {
          collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        break;
      }
      if (isValForKey.test(key, collision)) {
        return collision;
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (index == 0) {  // Strict capacity checked above.
            collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (isValForKey.test(key, collision)) {
              return collision;
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          return collision;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  private void decayAndDrop(final int counterOffset, final int maxCounterIndex, final int skipIndex,
      final V[] collisions) {
    int counterIndex = counterOffset;
    do {
      if (counterIndex == skipIndex) {
        continue;
      }
      int count = counters.getOpaque(counterIndex);
      if (count == 0) {
        if (counterIndex < skipIndex) {
          continue;
        }
        if (size.getAndDecrement() <= capacity) {
          size.getAndIncrement();
          continue;
        }
        for (int collisionIndex = counterIndex - counterOffset,
            nextCollisionIndex = collisionIndex + 1; ; ++collisionIndex, ++nextCollisionIndex) {
          if (nextCollisionIndex == collisions.length) {
            COLLISIONS.setOpaque(collisions, collisionIndex, null);
            return;
          }
          Object next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
          if (next == null) {
            COLLISIONS.setOpaque(collisions, collisionIndex, null);
            next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
            if (next == null
                || COLLISIONS.compareAndExchange(collisions, collisionIndex, null, next)
                != null) {
              return;
            }
          } else {
            COLLISIONS.setOpaque(collisions, collisionIndex, next);
          }
          // Counter misses may occur during this transition.
          count = counters.getOpaque(++counterIndex);
          counters.setOpaque(counterIndex - 1, count >> 1);
        }
      }
      // Counter misses may occur between these two calls.
      counters.setOpaque(counterIndex, count >> 1);
    } while (++counterIndex < maxCounterIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfSpaceAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        for (; size.get() <= capacity; ) {
          collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision;
          }
          if (++index == collisions.length) {
            return null;
          }
        }
        return null;
      }
      if (isValForKey.test(key, collision)) {
        return collision;
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfSpaceReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        for (; size.get() <= capacity; ) {
          collision = (V) COLLISIONS.compareAndExchange(collisions, index, null, val);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision; // If another thread raced to PUT, let it win.
          }
          if (++index == collisions.length) {
            return null;
          }
        }
        return null;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final V witness = (V) COLLISIONS.compareAndExchange(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        if (isValForKey.test(key, witness)) {
          return witness; // If another thread raced to PUT, let it win.
        }
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    synchronized (collisions) {
      int index = 0;
      do {
        V collision = (V) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {
          return false;
        }
        if (isValForKey.test(key, collision)) {
          size.getAndDecrement();
          final int counterOffset = hash << maxCollisionsShift;
          int counterIndex = counterOffset + index;
          for (int nextIndex = index + 1; ; ++index, ++nextIndex) {
            if (nextIndex == collisions.length) {
              COLLISIONS.setOpaque(collisions, index, null);
              return true;
            }
            Object next = COLLISIONS.getOpaque(collisions, nextIndex);
            if (next == null) {
              COLLISIONS.setOpaque(collisions, index, null);
              next = COLLISIONS.getOpaque(collisions, nextIndex);
              if (next == null
                  || COLLISIONS.compareAndExchange(collisions, index, null, next) != null) {
                return true;
              }
            } else {
              COLLISIONS.setOpaque(collisions, index, next);
            }
            // Counter misses may occur during this transition.
            final int count = counters.getOpaque(++counterIndex);
            counters.setOpaque(counterIndex - 1, count >> 1);
          }
        }
      } while (++index < collisions.length);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public void clear() {
    synchronized (hashTable) {
      IntStream.range(0, hashTable.length).parallel().forEach(i -> {
        final V[] collisions = (V[]) COLLISIONS.getOpaque(hashTable, i);
        if (collisions == null) {
          return;
        }
        int index = 0;
        do {
          final Object collision = COLLISIONS.getAndSet(collisions, index, null);
          if (collision != null) {
            size.getAndDecrement();
          }
        } while (++index < collisions.length);
      });
    }
  }

  @Override
  public String toString() {
    return "SparseCollisionCache{capacity=" + capacity
        + ", strictCapacity=" + strict
        + ", size=" + size.get()
        + ", " + super.toString() + '}';
  }
}
