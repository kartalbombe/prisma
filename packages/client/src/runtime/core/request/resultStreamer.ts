import type { InternalRequestParams } from '../../getPrismaClient'
import type { PrismaPromiseCallback } from './createPrismaPromise'

/**
 * Creates an AsyncIterator that streams results from a findMany query
 * This implementation requests all data and then yields it item by item
 * to provide streaming-like behavior without modifying the underlying query mechanism
 */
export function createResultStreamer<T extends any[]>(
  promiseCallback: PrismaPromiseCallback,
): () => AsyncIterator<T[number]> {
  return function stream(): AsyncIterator<T[number]> {
    let results: T[number][] | null = null
    let index = 0
    let promise: Promise<T> | null = null

    const iterator: AsyncIterator<T[number]> = {
      async next(): Promise<IteratorResult<T[number]>> {
        // If we haven't fetched results yet, do it now
        if (results === null) {
          if (promise === null) {
            promise = promiseCallback() as Promise<T>
          }
          const data = await promise
          results = Array.isArray(data) ? data : []
        }

        // Check if we've reached the end
        if (index >= results.length) {
          return { done: true, value: undefined }
        }

        // Return the next item
        const value = results[index]
        index++
        return { done: false, value }
      },
    }

    // Add the async iterator symbol
    ;(iterator as any)[Symbol.asyncIterator] = function () {
      return iterator
    }

    return iterator
  }
}

/**
 * Enhanced version that could support true streaming in the future
 * For now, it's the same as the basic version but structured to allow
 * future enhancements for chunk-based streaming
 */
export function createEnhancedResultStreamer<T extends any[]>(
  promiseCallback: PrismaPromiseCallback,
  requestParams?: InternalRequestParams,
): () => AsyncIterator<T[number]> {
  // TODO: In the future, this could be enhanced to:
  // 1. Make chunked requests to the database
  // 2. Stream results as they come from the network
  // 3. Support backpressure and flow control
  
  // For now, we use the same implementation as the basic streamer
  return createResultStreamer<T>(promiseCallback)
}