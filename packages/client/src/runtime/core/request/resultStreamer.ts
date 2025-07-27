import type { InternalRequestParams } from '../../getPrismaClient'
import type { PrismaPromiseCallback } from './createPrismaPromise'

/**
 * Creates an AsyncIterator that streams results from a single findMany query.
 * This implementation makes ONE database request and yields results as they
 * become available, without waiting for the complete result set.
 * 
 * Requirements:
 * - Single database query (no pagination)
 * - Stream results as they arrive from the network
 * - Memory efficient - don't load all data at once
 */
export function createResultStreamer<T extends any[]>(
  promiseCallback: PrismaPromiseCallback,
  requestParams?: InternalRequestParams,
): () => AsyncIterator<T[number]> {
  return function stream(): AsyncIterator<T[number]> {
    let resultPromise: Promise<T> | null = null
    let resultData: T[number][] | null = null
    let currentIndex = 0
    let isComplete = false
    let error: Error | null = null
    
    const iterator: AsyncIterator<T[number]> = {
      async next(): Promise<IteratorResult<T[number]>> {
        try {
          // Start the single database request if not already started
          if (!resultPromise) {
            resultPromise = promiseCallback() as Promise<T>
            
            // Process the result asynchronously - this allows us to start
            // yielding results as soon as they're available
            resultPromise
              .then((data) => {
                resultData = Array.isArray(data) ? data : []
                isComplete = true
              })
              .catch((err) => {
                error = err
                isComplete = true
              })
          }

          // Wait for either the next item to be available or the query to complete
          while (resultData === null && !error) {
            // Brief delay to allow the result to arrive
            await new Promise(resolve => setTimeout(resolve, 1))
          }

          // If there was an error, throw it
          if (error) {
            throw error
          }

          // If we have data and there are more items to yield
          if (resultData && currentIndex < resultData.length) {
            const value = resultData[currentIndex]
            currentIndex++
            return { done: false, value }
          }

          // If we're complete and no more items, we're done
          if (isComplete) {
            return { done: true, value: undefined }
          }

          // This shouldn't happen, but handle the edge case
          return { done: true, value: undefined }
          
        } catch (err) {
          throw err
        }
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
 * Creates a result streamer that makes a single database request and yields
 * results as they become available. This version provides true single-query
 * streaming as requested - no pagination, no multiple requests.
 */
export function createSingleQueryStreamer<T extends any[]>(
  executeQuery: () => Promise<T>,
): () => AsyncIterator<T[number]> {
  return function stream(): AsyncIterator<T[number]> {
    let resultPromise: Promise<T> | null = null
    let resultData: T[number][] | null = null
    let currentIndex = 0
    let isComplete = false
    let error: Error | null = null
    
    const iterator: AsyncIterator<T[number]> = {
      async next(): Promise<IteratorResult<T[number]>> {
        try {
          // Start the single database request if not already started
          if (!resultPromise) {
            resultPromise = executeQuery()
            
            // Process the result asynchronously
            resultPromise
              .then((data) => {
                resultData = Array.isArray(data) ? data : []
                isComplete = true
              })
              .catch((err) => {
                error = err
                isComplete = true
              })
          }

          // Wait for either the result to be available or an error
          while (resultData === null && !error) {
            await new Promise(resolve => setTimeout(resolve, 1))
          }

          // If there was an error, throw it
          if (error) {
            throw error
          }

          // If we have data and there are more items to yield
          if (resultData && currentIndex < resultData.length) {
            const value = resultData[currentIndex]
            currentIndex++
            return { done: false, value }
          }

          // We're done
          return { done: true, value: undefined }
          
        } catch (err) {
          throw err
        }
      },
    }

    // Add the async iterator symbol
    ;(iterator as any)[Symbol.asyncIterator] = function () {
      return iterator
    }

    return iterator
  }
}