import { createResultStreamer } from '../../src/runtime/core/request/resultStreamer'

describe('Result Streamer', () => {
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