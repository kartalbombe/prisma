import type { PrismaPromise } from './PrismaPromise'

/**
 * A PrismaPromise that supports streaming results via an AsyncIterator
 */
export interface StreamablePrismaPromise<TResult extends any[]> extends PrismaPromise<TResult> {
  /**
   * Returns an AsyncIterator that yields results as they come from the database
   * @returns AsyncIterator that yields individual items from the result array
   */
  stream(): AsyncIterator<TResult[number]>
}

/**
 * Type guard to check if a promise is streamable
 */
export function isStreamablePrismaPromise<T extends any[]>(
  promise: PrismaPromise<any>,
): promise is StreamablePrismaPromise<T> {
  return 'stream' in promise && typeof (promise as any).stream === 'function'
}