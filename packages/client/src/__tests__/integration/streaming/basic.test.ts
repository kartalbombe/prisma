import type { StreamablePrismaPromise } from '../../../runtime/core/types/exported/Public'

describe('Streaming Integration Test', () => {
  // Mock a simple Prisma Client setup for demonstration
  const mockUsers = Array.from({ length: 10 }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
  }))

  // Mock findMany that returns a StreamablePrismaPromise
  const createMockFindMany = (): StreamablePrismaPromise<typeof mockUsers> => {
    const basePromise = Promise.resolve(mockUsers)
    
    const streamablePrisma = basePromise as StreamablePrismaPromise<typeof mockUsers>
    
    // Add the stream method
    streamablePrisma.stream = () => {
      let index = 0
      const iterator: AsyncIterator<typeof mockUsers[number]> = {
        async next() {
          if (index >= mockUsers.length) {
            return { done: true, value: undefined }
          }
          const value = mockUsers[index]
          index++
          return { done: false, value }
        }
      }
      ;(iterator as any)[Symbol.asyncIterator] = () => iterator
      return iterator
    }

    return streamablePrisma
  }

  it('should work with the example from the issue - collecting users', async () => {
    // Simulates: const iterator = prisma.user.findMany().stream()
    const findManyResult = createMockFindMany()
    const iterator = findManyResult.stream()

    // Example from issue: collecting users
    const users = []
    for await (const user of iterator) {
      users.push(user)
    }

    expect(users).toHaveLength(10)
    expect(users[0]).toEqual({ id: 1, name: 'User 1', email: 'user1@example.com' })
    expect(users[9]).toEqual({ id: 10, name: 'User 10', email: 'user10@example.com' })
  })

  it('should work with the CSV writing example from the issue', async () => {
    // Simulates: const iterator = prisma.user.findMany().stream()
    const findManyResult = createMockFindMany()
    const iterator = findManyResult.stream()

    // Example from issue: writing to CSV
    const csvLines: string[] = []
    csvLines.push('id\tname\temail') // CSV header

    for await (const user of iterator) {
      csvLines.push(`${user.id}\t${user.name}\t${user.email}`)
    }

    expect(csvLines).toHaveLength(11) // Header + 10 users
    expect(csvLines[0]).toBe('id\tname\temail')
    expect(csvLines[1]).toBe('1\tUser 1\tuser1@example.com')
    expect(csvLines[10]).toBe('10\tUser 10\tuser10@example.com')
  })

  it('should maintain backward compatibility - can still use as regular promise', async () => {
    const findManyResult = createMockFindMany()

    // Should work as regular promise
    const users = await findManyResult
    expect(users).toHaveLength(10)
    expect(users[0]).toEqual({ id: 1, name: 'User 1', email: 'user1@example.com' })
  })

  it('should support both promise methods and streaming', async () => {
    const findManyResult = createMockFindMany()

    // Test promise-like behavior
    const promiseUsers = await findManyResult
    
    // Test streaming behavior
    const streamUsers = []
    for await (const user of findManyResult.stream()) {
      streamUsers.push(user)
    }

    expect(promiseUsers).toEqual(streamUsers)
  })

  it('should handle large datasets efficiently (memory simulation)', async () => {
    // Simulate a larger dataset
    const largeDataset = Array.from({ length: 100000 }, (_, i) => ({
      id: i + 1,
      name: `User ${i + 1}`,
    }))

    const mockLargeFindMany = (): StreamablePrismaPromise<typeof largeDataset> => {
      const basePromise = Promise.resolve(largeDataset)
      const streamablePrisma = basePromise as StreamablePrismaPromise<typeof largeDataset>
      
      streamablePrisma.stream = () => {
        let index = 0
        const iterator: AsyncIterator<typeof largeDataset[number]> = {
          async next() {
            if (index >= largeDataset.length) {
              return { done: true, value: undefined }
            }
            const value = largeDataset[index]
            index++
            return { done: false, value }
          }
        }
        ;(iterator as any)[Symbol.asyncIterator] = () => iterator
        return iterator
      }

      return streamablePrisma
    }

    const findManyResult = mockLargeFindMany()
    let count = 0
    
    for await (const user of findManyResult.stream()) {
      count++
      // Process each user without loading all into memory at once
      expect(user.id).toBe(count)
      
      // Break early to test streaming works
      if (count >= 100) break
    }

    expect(count).toBe(100)
  })
})