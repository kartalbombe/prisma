import { createResultStreamer, createEnhancedResultStreamer } from '../../runtime/core/request/resultStreamer'

describe('Result Streamer', () => {
  describe('Basic Result Streamer (legacy)', () => {
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

  describe('Enhanced Result Streamer (true streaming)', () => {
    it('should make chunked requests and stream results as they arrive', async () => {
      // Mock data that will be returned in chunks
      const fullDataset = Array.from({ length: 250 }, (_, i) => ({ id: i + 1, name: `User ${i + 1}` }))
      
      // Mock chunked request function that simulates database calls
      const mockChunkedRequest = jest.fn().mockImplementation(async (skip: number, take: number) => {
        // Simulate network delay
        await new Promise(resolve => setTimeout(resolve, 10))
        return fullDataset.slice(skip, skip + take)
      })

      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 100)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      // Should have streamed all items
      expect(results).toEqual(fullDataset)
      
      // Should have made 3 chunked requests (100 + 100 + 50)
      expect(mockChunkedRequest).toHaveBeenCalledTimes(3)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(1, 0, 100)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(2, 100, 100)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(3, 200, 100)
    })

    it('should handle small datasets that fit in one chunk', async () => {
      const smallDataset = [{ id: 1 }, { id: 2 }, { id: 3 }]
      
      const mockChunkedRequest = jest.fn().mockResolvedValue(smallDataset)
      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 100)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual(smallDataset)
      expect(mockChunkedRequest).toHaveBeenCalledTimes(1)
      expect(mockChunkedRequest).toHaveBeenCalledWith(0, 100)
    })

    it('should handle empty datasets', async () => {
      const mockChunkedRequest = jest.fn().mockResolvedValue([])
      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 100)
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual([])
      expect(mockChunkedRequest).toHaveBeenCalledTimes(1)
      expect(mockChunkedRequest).toHaveBeenCalledWith(0, 100)
    })

    it('should support custom chunk sizes', async () => {
      const dataset = Array.from({ length: 25 }, (_, i) => ({ id: i + 1 }))
      
      const mockChunkedRequest = jest.fn().mockImplementation(async (skip: number, take: number) => {
        return dataset.slice(skip, skip + take)
      })

      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 10) // Custom chunk size
      const iterator = streamFunction()

      const results = []
      for await (const item of iterator) {
        results.push(item)
      }

      expect(results).toEqual(dataset)
      // Should make 3 requests: 10 + 10 + 5
      expect(mockChunkedRequest).toHaveBeenCalledTimes(3)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(1, 0, 10)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(2, 10, 10)
      expect(mockChunkedRequest).toHaveBeenNthCalledWith(3, 20, 10)
    })

    it('should handle errors in chunked requests', async () => {
      const mockChunkedRequest = jest.fn()
        .mockResolvedValueOnce([{ id: 1 }, { id: 2 }]) // First chunk succeeds
        .mockRejectedValueOnce(new Error('Database error')) // Second chunk fails

      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 2)
      const iterator = streamFunction()

      const results = []
      try {
        for await (const item of iterator) {
          results.push(item)
        }
        fail('Should have thrown an error')
      } catch (error) {
        expect(error.message).toBe('Database error')
        expect(results).toEqual([{ id: 1 }, { id: 2 }]) // First chunk was processed
        expect(mockChunkedRequest).toHaveBeenCalledTimes(2)
      }
    })

    it('should work with manual iterator usage', async () => {
      const dataset = [{ id: 1 }, { id: 2 }, { id: 3 }]
      
      const mockChunkedRequest = jest.fn().mockImplementation(async (skip: number, take: number) => {
        return dataset.slice(skip, skip + take)
      })

      const streamFunction = createEnhancedResultStreamer(mockChunkedRequest, 2)
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

      // Should have made 2 chunked requests
      expect(mockChunkedRequest).toHaveBeenCalledTimes(2)
    })
  })
})