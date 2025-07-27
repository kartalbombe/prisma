import { Operation } from '@prisma/client-common'

import { GetResult } from './Result'
import { Exact } from './Utils'

/*
 * /!\ These types are exposed to the user. Proceed with caution.
 *
 * TODO: Move more hardcoded types from generation into here
 */

// prettier-ignore
export type Args<T, F extends Operation> =
  T extends { [K: symbol]: { types: { operations: { [K in F]: { args: any } } } } }
  ? T[symbol]['types']['operations'][F]['args']
  : any

// prettier-ignore
export type Result<T, A, F extends Operation> =
  T extends { [K: symbol]: { types: { payload: any } } }
  ? GetResult<T[symbol]['types']['payload'], A, F>
  : GetResult<{ composites: {}, objects: {}, scalars: {}, name: '' }, {}, F>

// prettier-ignore
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export type Payload<T, F extends Operation = never> = T extends { [K: symbol]: { types: { payload: any } } }
  ? T[symbol]['types']['payload']
  : any

// we don't expose our internal types to keep the API secret
export interface PrismaPromise<T> extends Promise<T> {
  [Symbol.toStringTag]: 'PrismaPromise'
}

// StreamablePrismaPromise for findMany operations that support streaming
export interface StreamablePrismaPromise<T extends any[]> extends PrismaPromise<T> {
  /**
   * Returns an AsyncIterator that yields results as they come from the database
   * @returns AsyncIterator that yields individual items from the result array
   */
  stream(): AsyncIterator<T[number]>
}

export { type Operation }

export { type Exact }
