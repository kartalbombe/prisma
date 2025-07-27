/**
 * End-to-end demonstration of the streaming functionality
 * This test demonstrates all the examples from the original issue
 * with TRUE streaming that fetches data as it arrives from the network
 */

import { createStreamablePrismaPromise } from '../../../runtime/core/request/createPrismaPromise'
import { createEnhancedResultStreamer } from '../../../runtime/core/request/resultStreamer'

describe('End-to-End Streaming Demo', () => {
  // Sample data that represents what would come from a database
  const mockUsers = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
    active: i % 2 === 0, // Every other user is active (starting with id 1)
  }))

  // Simulate the actual createStreamablePrismaPromise function with TRUE streaming
  const createFindManyPromise = (data: typeof mockUsers, chunkSize: number = 100) => {
    // Mock the original promise callback (for backward compatibility)
    const promiseCallback = async () => Promise.resolve(data)
    
    // Create the chunked request function for true streaming
    const createChunkedRequest = async (skip: number, take: number) => {
      // Simulate network delay to demonstrate true streaming
      await new Promise(resolve => setTimeout(resolve, 10))
      return data.slice(skip, skip + take)
    }
    
    const streamCallback = createEnhancedResultStreamer(createChunkedRequest, chunkSize)
    
    return createStreamablePrismaPromise(
      promiseCallback,
      streamCallback,
      { action: 'findMany', args: {}, model: 'User' }
    )
  }

  it('should demonstrate the original issue example - collecting users', async () => {
    // From the issue: const iterator: AsyncIterator<User> = prisma.user.findMany(...).stream()
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 10))
    const iterator = findManyResult.stream()

    // From the issue: collecting users
    const users = []
    for await (const user of iterator) {
      users.push(user)
    }

    expect(users).toHaveLength(10)
    expect(users[0]).toMatchObject({ id: 1, name: 'User 1' })
  })

  it('should demonstrate the CSV export example from the issue', async () => {
    // From the issue: streaming to CSV
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 5))
    
    // Simulate writing to file
    const csvContent: string[] = []
    csvContent.push('id\tname\n') // CSV header

    for await (const user of findManyResult.stream()) {
      csvContent.push(`${user.id}\t${user.name}\n`)
    }

    expect(csvContent).toHaveLength(6) // Header + 5 users
    expect(csvContent[0]).toBe('id\tname\n')
    expect(csvContent[1]).toBe('1\tUser 1\n')
    expect(csvContent[5]).toBe('5\tUser 5\n')
  })

  it('should demonstrate TRUE streaming - data arrives from network in chunks', async () => {
    // From the issue: "Let's say I have 1.000.000 rows in my database"
    // We'll simulate with 250 rows to test chunked behavior with chunk size of 100
    const largeDataset = Array.from({ length: 250 }, (_, i) => ({
      id: i + 1,
      name: `User ${i + 1}`,
      email: `user${i + 1}@example.com`,
    }))

    // Track when each chunk arrives to prove streaming
    const chunkArrivalTimes: number[] = []
    let processedCount = 0
    
    // Create a custom chunked request function to track timing
    const createChunkedRequest = async (skip: number, take: number) => {
      chunkArrivalTimes.push(Date.now())
      // Simulate network delay
      await new Promise(resolve => setTimeout(resolve, 50))
      return largeDataset.slice(skip, skip + take)
    }
    
    const streamCallback = createEnhancedResultStreamer(createChunkedRequest, 100)
    const streamablePrisma = {} as any
    streamablePrisma.stream = streamCallback
    
    let firstUser, lastUser
    const startTime = Date.now()

    for await (const user of streamablePrisma.stream()) {
      if (processedCount === 0) firstUser = user
      lastUser = user
      processedCount++
      
      // Simulate processing without loading all into memory
      expect(user.id).toBe(processedCount)
    }

    expect(processedCount).toBe(250)
    expect(firstUser).toMatchObject({ id: 1, name: 'User 1' })
    expect(lastUser).toMatchObject({ id: 250, name: 'User 250' })
    
    // Verify that chunks arrived at different times (proving streaming)
    expect(chunkArrivalTimes.length).toBe(3) // Should have made 3 chunked requests
    expect(chunkArrivalTimes[1] - chunkArrivalTimes[0]).toBeGreaterThan(40) // At least network delay
    expect(chunkArrivalTimes[2] - chunkArrivalTimes[1]).toBeGreaterThan(40) // At least network delay
  })

  it('should maintain backward compatibility with regular promise usage', async () => {
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 3))

    // Regular promise usage should still work
    const users = await findManyResult
    expect(users).toHaveLength(3)
    expect(users[0]).toMatchObject({ id: 1, name: 'User 1' })

    // Streaming should also work on the same promise
    const streamedUsers = []
    for await (const user of findManyResult.stream()) {
      streamedUsers.push(user)
    }

    expect(streamedUsers).toEqual(users)
  })

  it('should demonstrate real-world use case: data export with processing', async () => {
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 20), 5) // Small chunks for demo
    
    // Simulate exporting only active users to JSON
    const exportedUsers = []
    let totalProcessed = 0
    let chunksProcessed = 0

    for await (const user of findManyResult.stream()) {
      totalProcessed++
      
      // Track that we're processing in chunks
      if (totalProcessed % 5 === 1) {
        chunksProcessed++
      }
      
      if (user.active) {
        // Transform and export only active users
        exportedUsers.push({
          id: user.id,
          displayName: user.name.toUpperCase(),
          contact: user.email,
        })
      }
    }

    expect(totalProcessed).toBe(20) // Processed all users
    expect(chunksProcessed).toBe(4) // Should have processed 4 chunks of 5 users each
    expect(exportedUsers.length).toBe(10) // Only active users (every other one)
    expect(exportedUsers[0]).toMatchObject({
      id: 1, // First active user has id 1 (since i % 2 === 0 when i === 0)
      displayName: 'USER 1',
      contact: 'user1@example.com',
    })
  })

  it('should handle edge cases properly', async () => {
    // Empty result set
    const emptyResult = createFindManyPromise([])
    const emptyUsers = []
    for await (const user of emptyResult.stream()) {
      emptyUsers.push(user)
    }
    expect(emptyUsers).toHaveLength(0)

    // Single item
    const singleResult = createFindManyPromise([mockUsers[0]])
    const singleUsers = []
    for await (const user of singleResult.stream()) {
      singleUsers.push(user)
    }
    expect(singleUsers).toHaveLength(1)
    expect(singleUsers[0]).toMatchObject({ id: 1, name: 'User 1' })
  })

  it('should demonstrate the issue requirement: streams data as it arrives from network', async () => {
    // This test proves we're streaming data from the network, not fetching all at once
    
    const dataset = mockUsers.slice(0, 30) // 30 items to be fetched in 3 chunks of 10
    const requestLog: Array<{ skip: number; take: number; timestamp: number }> = []
    
    // Track all network requests
    const createChunkedRequest = async (skip: number, take: number) => {
      requestLog.push({ skip, take, timestamp: Date.now() })
      // Simulate actual network delay
      await new Promise(resolve => setTimeout(resolve, 30))
      return dataset.slice(skip, skip + take)
    }
    
    const streamCallback = createEnhancedResultStreamer(createChunkedRequest, 10)
    const streamablePrisma = {} as any
    streamablePrisma.stream = streamCallback
    
    const streamResult = []
    const processingLog: Array<{ itemId: number; timestamp: number }> = []
    
    for await (const user of streamablePrisma.stream()) {
      processingLog.push({ itemId: user.id, timestamp: Date.now() })
      streamResult.push(user)
    }

    // Verify we streamed all data correctly
    expect(streamResult).toHaveLength(30)
    expect(streamResult.map(u => u.id)).toEqual(Array.from({ length: 30 }, (_, i) => i + 1))
    
    // Verify we made 3 chunked requests as expected
    expect(requestLog).toHaveLength(3)
    expect(requestLog[0]).toMatchObject({ skip: 0, take: 10 })
    expect(requestLog[1]).toMatchObject({ skip: 10, take: 10 })
    expect(requestLog[2]).toMatchObject({ skip: 20, take: 10 })
    
    // Verify requests were made at different times (proving streaming)
    expect(requestLog[1].timestamp - requestLog[0].timestamp).toBeGreaterThan(20)
    expect(requestLog[2].timestamp - requestLog[1].timestamp).toBeGreaterThan(20)
    
    // Verify processing started before all data was fetched
    const firstItemProcessed = processingLog[0].timestamp
    const lastRequestMade = requestLog[2].timestamp
    expect(firstItemProcessed).toBeLessThan(lastRequestMade) // Started processing before final request
  })
})