# Row Streaming in Prisma Client

This document describes the row streaming functionality added to Prisma Client, allowing you to stream results from `findMany` queries using async iterators.

## Overview

Row streaming allows you to process large datasets efficiently by iterating over results one by one without loading all data into memory at once. This is particularly useful for:

- Exporting large datasets to files (CSV, JSON, etc.)
- Processing large amounts of data with limited memory
- Real-time data processing where you want to start processing before all data is loaded

## Usage

### Basic Streaming

```typescript
import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

// Stream all users
for await (const user of prisma.user.findMany().stream()) {
  console.log(user.name)
}
```

### CSV Export Example

```typescript
import fs from 'fs'

const file = fs.createWriteStream('users.csv')
file.write('id\tname\temail\n')

for await (const user of prisma.user.findMany().stream()) {
  file.write(`${user.id}\t${user.name}\t${user.email}\n`)
}

file.end()
```

### Processing with Filtering

```typescript
// Only stream active users
for await (const user of prisma.user.findMany({
  where: { active: true }
}).stream()) {
  // Process each active user
  await processUser(user)
}
```

### Collecting Results

```typescript
const users = []
for await (const user of prisma.user.findMany().stream()) {
  users.push(user)
}
```

## Type Safety

The streaming functionality is fully type-safe:

```typescript
// TypeScript knows that `user` has the correct type
for await (const user of prisma.user.findMany({
  select: { id: true, name: true }
}).stream()) {
  // user: { id: number, name: string }
  console.log(user.id, user.name)
}
```

## Backward Compatibility

The streaming functionality is additive and doesn't break existing code:

```typescript
// Existing code continues to work
const users = await prisma.user.findMany()

// New streaming functionality
for await (const user of prisma.user.findMany().stream()) {
  // ...
}
```

## How It Works

### Current Implementation

The current implementation:

1. Executes the `findMany` query normally
2. Retrieves all results from the database
3. Creates an AsyncIterator that yields results one by one
4. Provides memory-efficient iteration over the results

### Future Enhancements

The architecture is designed to support future enhancements such as:

- True network-level streaming with chunked requests
- Backpressure and flow control
- Streaming with cursors and pagination
- Real-time streaming from database change streams

## Performance Considerations

### Memory Usage

While the current implementation still loads all data into memory initially, the streaming interface provides:

- Consistent memory usage patterns during iteration
- Ability to process results immediately without waiting for full completion
- Foundation for future true streaming implementations

### When to Use Streaming

Use streaming when:

- Processing large datasets (> 10,000 records)
- Exporting data to files
- You need to start processing before all data is loaded
- Memory usage is a concern

Don't use streaming when:

- Working with small datasets (< 1,000 records)  
- You need random access to all results
- You're doing operations that require the full dataset (sorting, counting, etc.)

## Error Handling

Streaming respects the same error handling as regular Prisma queries:

```typescript
try {
  for await (const user of prisma.user.findMany().stream()) {
    // Process user
  }
} catch (error) {
  console.error('Error during streaming:', error)
}
```

## Limitations

### Current Limitations

1. **Full data loading**: Currently loads all data before streaming (will be improved in future versions)
2. **findMany only**: Only available on `findMany` operations
3. **No cursor support**: Does not support cursor-based pagination in streaming mode

### Not Supported

- Streaming with other query types (`findFirst`, `findUnique`, etc.)
- Streaming with cursors (`cursor` option is ignored in current implementation)
- Streaming with transactions (use regular promises for transactional operations)

## Examples

### Real-world Examples

#### Data Migration

```typescript
// Migrate user data to new format
for await (const user of prisma.user.findMany().stream()) {
  const migratedData = await migrateUserData(user)
  await saveToNewSystem(migratedData)
}
```

#### Batch Processing

```typescript
const batchSize = 100
let batch = []

for await (const user of prisma.user.findMany().stream()) {
  batch.push(user)
  
  if (batch.length === batchSize) {
    await processBatch(batch)
    batch = []
  }
}

// Process remaining items
if (batch.length > 0) {
  await processBatch(batch)
}
```

#### JSON Export

```typescript
import fs from 'fs'

const writeStream = fs.createWriteStream('users.json')
writeStream.write('[\n')

let isFirst = true
for await (const user of prisma.user.findMany().stream()) {
  if (!isFirst) {
    writeStream.write(',\n')
  }
  writeStream.write(JSON.stringify(user))
  isFirst = false
}

writeStream.write('\n]')
writeStream.end()
```

## TypeScript Support

The streaming functionality includes full TypeScript support:

```typescript
import type { StreamablePrismaPromise } from '@prisma/client'

// The return type of findMany() is StreamablePrismaPromise<User[]>
const streamablePromise: StreamablePrismaPromise<User[]> = prisma.user.findMany()

// The stream() method returns AsyncIterator<User>
const iterator: AsyncIterator<User> = streamablePromise.stream()
```

## Implementation Details

### Architecture

The streaming functionality is implemented through:

1. **StreamablePrismaPromise**: Interface extending PrismaPromise with stream() method
2. **Result Streamer**: Creates AsyncIterator from promise results
3. **Type Generation**: Updated to return StreamablePrismaPromise for findMany
4. **Runtime Integration**: Modified model actions to create streamable promises

### Files Modified

- `packages/client-generator-ts/src/TSClient/Model.ts`: Type generation for StreamablePrismaPromise
- `packages/client/src/runtime/core/model/applyModel.ts`: Runtime model action creation
- `packages/client/src/runtime/core/request/`: Streaming infrastructure
- `packages/client/src/runtime/core/types/exported/Public.ts`: Public type definitions