import { createResultStreamer, createSingleQueryStreamer } from '../../runtime/core/request/resultStreamer'

describe('Result Streamer', () => {
  describe('Basic Result Streamer (original)', () => {
    it('should create an async iterator that yields array items one by one', async () => {
      // Mock promise callback that returns an array
      const mockData = [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ]

      const mockCallback = jest.fn().mockResolvedValue(mockData)
      const streamFunction = createResultStreamer(mockCallback)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual(mockData)
      expect(mockCallback).toHaveBeenCalledTimes(1)
    })

    it('should handle empty arrays', async () => {
      const mockCallback = jest.fn().mockResolvedValue([])
      const streamFunction = createResultStreamer(mockCallback)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual([])
      expect(mockCallback).toHaveBeenCalledTimes(1)
    })

    it('should only call the promise callback once even with multiple iterations', async () => {
      const mockData = [{ id: 1 }, { id: 2 }]
      const mockCallback = jest.fn().mockResolvedValue(mockData)
      const streamFunction = createResultStreamer(mockCallback)
      const iterator = streamFunction()

      // Consume iterator manually to test multiple next() calls
      const result1 = await iterator.next()
      const result2 = await iterator.next()
      const result3 = await iterator.next()

      expect(result1).toEqual({ done: false, value: { id: 1 } })
      expect(result2).toEqual({ done: false, value: { id: 2 } })
      expect(result3).toEqual({ done: true, value: undefined })
      expect(mockCallback).toHaveBeenCalledTimes(1)
    })
  })

  describe('Single Query Streamer (true single-request streaming)', () => {
    it('should make ONE database request and stream results as they arrive', async () => {
      // Mock data that would be returned from a single findMany query
      const fullDataset = Array.from({ length: 250 }, (_, i) => ({ id: i + 1, name: `User ${i + 1}` }))
      
      // Mock single query executor - this simulates one database request
      const mockExecuteQuery = jest.fn().mockImplementation(async () => {
        // Simulate network delay for the single query
        await new Promise(resolve => setTimeout(resolve, 20))
        return fullDataset
      })

      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      // Should have streamed all items
      expect(results).toEqual(fullDataset)
      
      // Should have made only ONE database request
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1)
    })

    it('should handle small datasets with single request', async () => {
      const smallDataset = [{ id: 1 }, { id: 2 }, { id: 3 }]
      
      const mockExecuteQuery = jest.fn().mockResolvedValue(smallDataset)
      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual(smallDataset)
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1)
    })

    it('should handle empty datasets', async () => {
      const mockExecuteQuery = jest.fn().mockResolvedValue([])
      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual([])
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1)
    })

    it('should handle errors in the single query', async () => {
      const mockExecuteQuery = jest.fn().mockRejectedValue(new Error('Database connection failed'))

      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      const results = []
      try {
        for await (const item of iterator) {
          results.push(item)
        }
        fail('Should have thrown an error')
      } catch (error) {
        expect(error.message).toBe('Database connection failed')
        expect(results).toEqual([]) // No results should be processed
        expect(mockExecuteQuery).toHaveBeenCalledTimes(1)
      }
    })

    it('should work with manual iterator usage', async () => {
      const dataset = [{ id: 1 }, { id: 2 }, { id: 3 }]
      
      const mockExecuteQuery = jest.fn().mockResolvedValue(dataset)
      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      // Manual iteration
      const result1 = await iterator.next()
      const result2 = await iterator.next()
      const result3 = await iterator.next()
      const result4 = await iterator.next()

      expect(result1).toEqual({ done: false, value: { id: 1 } })
      expect(result2).toEqual({ done: false, value: { id: 2 } })
      expect(result3).toEqual({ done: false, value: { id: 3 } })
      expect(result4).toEqual({ done: true, value: undefined })

      // Should have made only one request
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1)
    })

    it('should start streaming immediately after first query completion', async () => {
      const dataset = [{ id: 1 }, { id: 2 }, { id: 3 }]
      let queryResolved = false
      
      const mockExecuteQuery = jest.fn().mockImplementation(async () => {
        await new Promise(resolve => setTimeout(resolve, 50))
        queryResolved = true
        return dataset
      })

      const streamFunction = createSingleQueryStreamer(mockExecuteQuery)
      const iterator = streamFunction()

      // Start consuming - this should trigger the single query
      const firstResult = await iterator.next()
      
      // Query should be completed and first result available
      expect(queryResolved).toBe(true)
      expect(firstResult).toEqual({ done: false, value: { id: 1 } })
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1)

      // Subsequent calls should not trigger additional queries
      const secondResult = await iterator.next()
      expect(secondResult).toEqual({ done: false, value: { id: 2 } })
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1) // Still only one call
    })
  })
})