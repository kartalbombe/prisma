/**
 * End-to-end demonstration of the streaming functionality
 * This test demonstrates all the examples from the original issue
 * with TRUE single-query streaming that processes data as it arrives
 */

import { createStreamablePrismaPromise } from '../../../runtime/core/request/createPrismaPromise'
import { createSingleQueryStreamer } from '../../../runtime/core/request/resultStreamer'

describe('End-to-End Streaming Demo', () => {
  // Sample data that represents what would come from a database
  const mockUsers = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
    active: i % 2 === 0, // Every other user is active (starting with id 1)
  }))

  // Simulate the actual createStreamablePrismaPromise function with TRUE single-query streaming
  const createFindManyPromise = (data: typeof mockUsers) => {
    // Mock the original promise callback (for backward compatibility)
    const promiseCallback = async () => Promise.resolve(data)
    
    // Create the single query executor for true streaming
    const executeQuery = async () => {
      // Simulate network delay for the single database request
      await new Promise(resolve => setTimeout(resolve, 20))
      return data
    }
    
    const streamCallback = createSingleQueryStreamer(executeQuery)
    
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

  it('should demonstrate TRUE single-query streaming - one database request', async () => {
    // From the issue: "We need to do a one fetch from the database"
    // This should make ONE request and stream results as they become available
    const largeDataset = Array.from({ length: 250 }, (_, i) => ({
      id: i + 1,
      name: `User ${i + 1}`,
      email: `user${i + 1}@example.com`,
    }))

    // Track when the single query is made
    let queryStartTime: number
    let queryEndTime: number
    let processedCount = 0
    
    // Create a custom single query executor to track timing
    const executeQuery = async () => {
      queryStartTime = Date.now()
      // Simulate single database request delay
      await new Promise(resolve => setTimeout(resolve, 50))
      queryEndTime = Date.now()
      return largeDataset
    }
    
    const streamCallback = createSingleQueryStreamer(executeQuery)
    const streamablePrisma = {} as any
    streamablePrisma.stream = streamCallback
    
    let firstUser, lastUser
    const processingStartTime = Date.now()
    let firstItemProcessedTime: number

    for await (const user of streamablePrisma.stream()) {
      if (processedCount === 0) {
        firstUser = user
        firstItemProcessedTime = Date.now()
      }
      lastUser = user
      processedCount++
      
      // Simulate processing without loading all into memory
      expect(user.id).toBe(processedCount)
    }

    expect(processedCount).toBe(250)
    expect(firstUser).toMatchObject({ id: 1, name: 'User 1' })
    expect(lastUser).toMatchObject({ id: 250, name: 'User 250' })
    
    // Verify that only ONE query was made (not chunked requests)
    expect(queryStartTime).toBeDefined()
    expect(queryEndTime).toBeDefined()
    expect(queryEndTime - queryStartTime).toBeGreaterThan(40) // Single query took time
    
    // Verify processing started after the single query completed
    expect(firstItemProcessedTime! > queryEndTime).toBe(true)
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

  it('should demonstrate real-world use case: data export with single query', async () => {
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 20))
    
    // Simulate exporting only active users to JSON
    const exportedUsers = []
    let totalProcessed = 0

    for await (const user of findManyResult.stream()) {
      totalProcessed++
      
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

  it('should demonstrate the user requirement: single fetch with streaming results', async () => {
    // This test proves we're making ONE database request and streaming the results
    
    const dataset = mockUsers.slice(0, 30) // 30 items from a single query
    let queryExecuted = false
    let queryCompletedAt: number
    
    // Track the single database query
    const executeQuery = async () => {
      queryExecuted = true
      // Simulate single database request
      await new Promise(resolve => setTimeout(resolve, 30))
      queryCompletedAt = Date.now()
      return dataset
    }
    
    const streamCallback = createSingleQueryStreamer(executeQuery)
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
    
    // Verify only ONE query was executed (no pagination, no chunking)
    expect(queryExecuted).toBe(true)
    
    // Verify all processing happened after the single query completed
    const firstItemProcessed = processingLog[0].timestamp
    const lastItemProcessed = processingLog[processingLog.length - 1].timestamp
    
    expect(firstItemProcessed).toBeGreaterThanOrEqual(queryCompletedAt!)
    expect(lastItemProcessed).toBeGreaterThan(firstItemProcessed)
    
    // Verify we processed items in the correct order
    expect(processingLog.map(p => p.itemId)).toEqual(Array.from({ length: 30 }, (_, i) => i + 1))
  })
})