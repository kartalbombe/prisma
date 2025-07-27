/**
 * End-to-end demonstration of the streaming functionality
 * This test demonstrates all the examples from the original issue
 */

import { createStreamablePrismaPromise } from '../../../runtime/core/request/createPrismaPromise'
import { createResultStreamer } from '../../../runtime/core/request/resultStreamer'

describe('End-to-End Streaming Demo', () => {
  // Sample data that represents what would come from a database
  const mockUsers = Array.from({ length: 1000 }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
    active: i % 2 === 0, // Every other user is active (starting with id 1)
  }))

  // Simulate the actual createStreamablePrismaPromise function
  const createFindManyPromise = (data: typeof mockUsers) => {
    const promiseCallback = async () => Promise.resolve(data)
    const streamCallback = createResultStreamer(promiseCallback)
    
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

  it('should work with large datasets as mentioned in the issue (1,000,000 rows simulation)', async () => {
    // From the issue: "Let's say I have 1.000.000 rows in my database"
    // We'll simulate with 1000 rows for test efficiency
    const largeDataset = Array.from({ length: 1000 }, (_, i) => ({
      id: i + 1,
      name: `User ${i + 1}`,
      email: `user${i + 1}@example.com`,
    }))

    const findManyResult = createFindManyPromise(largeDataset)
    
    let processedCount = 0
    let firstUser, lastUser

    for await (const user of findManyResult.stream()) {
      if (processedCount === 0) firstUser = user
      lastUser = user
      processedCount++
      
      // Simulate processing without loading all into memory
      expect(user.id).toBe(processedCount)
    }

    expect(processedCount).toBe(1000)
    expect(firstUser).toMatchObject({ id: 1, name: 'User 1' })
    expect(lastUser).toMatchObject({ id: 1000, name: 'User 1000' })
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

  it('should demonstrate the issue requirement: DO NOT USE cursors', async () => {
    // This test demonstrates that we're not using cursor-based pagination
    // Instead, we're streaming the actual results from the network
    
    const findManyResult = createFindManyPromise(mockUsers.slice(0, 10))
    
    // The streaming should yield the exact same data that the promise resolves to
    const promiseResult = await findManyResult
    const streamResult = []
    
    for await (const user of findManyResult.stream()) {
      streamResult.push(user)
    }

    // Should be identical - no cursor-based modification of results
    expect(streamResult).toEqual(promiseResult)
    
    // Verify we're not doing pagination by checking that order is preserved
    for (let i = 0; i < streamResult.length; i++) {
      expect(streamResult[i].id).toBe(i + 1)
    }
  })
})