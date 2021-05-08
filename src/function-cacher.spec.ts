import { randomInt } from "crypto"
import { FunctionCacher } from "./function-cacher"

describe("createCacheFunction", () => {
  const cacher = new FunctionCacher()

  const mockInnerWorkings = jest.fn().mockImplementation(() => randomInt(100000))
  const functionA = (a: string, b: number, c: Record<string, string>) => {
    return mockInnerWorkings()
  }
  const functionB = async (a: string, b: number, c: Record<string, string>): Promise<number> => {
    return new Promise((resolve) => setTimeout(() => resolve(mockInnerWorkings())))
  }

  const cachedFunctionA = cacher.createCachedFunction(functionA, [true, true, false])
  const cachedAsyncFunction = cacher.createCachedFunction(functionB, [true, true, false])

  afterEach(() => {
    cacher.clear()
    jest.clearAllMocks()
  })

  it("caches", () => {
    for (let i = 0; i < 10; i++) {
      cachedFunctionA("foo", 2, {})
    }
    expect(mockInnerWorkings).toHaveBeenCalledTimes(1)
  })

  it("can distringuish between arguments", () => {
    for (let i = 0; i < 10; i++) {
      cachedFunctionA("foo", 2, {})
      cachedFunctionA("bar", 7, {})
    }
    expect(mockInnerWorkings).toHaveBeenCalledTimes(2)
  })

  it("also works for async functions / promises", async () => {
    for (let i = 0; i < 10; i++) {
      await cachedAsyncFunction("foo", 2, {})
    }
    expect(mockInnerWorkings).toHaveBeenCalledTimes(1)
  })

  it("also works for async functions / promises", async () => {
    // how many times should each variant be called
    const numberOfCalls = 10
    const sequence = [...Array(numberOfCalls).keys()].map((n) => n + 1)

    const callVariant1Promises = sequence.map(() => cachedAsyncFunction("foo", 2, {}))
    const callVariant2Promises = sequence.map(() => cachedAsyncFunction("bar", 7, {}))

    const variant1Results = await Promise.all(callVariant1Promises)
    const variant2Results = await Promise.all(callVariant2Promises)

    // assert that all results are equal to the return value of the inner function
    expect(mockInnerWorkings).toHaveBeenCalledTimes(2)
    expect(variant1Results).toEqual([...Array(numberOfCalls).keys()].map(() => mockInnerWorkings.mock.results[0].value))
    expect(variant2Results).toEqual([...Array(numberOfCalls).keys()].map(() => mockInnerWorkings.mock.results[1].value))
  })

  it("should handle rejected promises properly", async () => {
    mockInnerWorkings.mockRejectedValueOnce("nope")
    await expect(cachedAsyncFunction("foo", 2, {})).rejects.toEqual("nope")
    const result = await cachedAsyncFunction("foo", 2, {})
    await expect(result).toEqual(mockInnerWorkings.mock.results[1].value)

    // this should change nothing
    await cachedAsyncFunction("foo", 2, {})

    expect(mockInnerWorkings).toHaveBeenCalledTimes(2)
  })

  it("promise catch calls stack and don't replace", async () => {
    mockInnerWorkings.mockRejectedValueOnce("nope")
    await cachedAsyncFunction("foo", 2, {}).catch((reason) => {
      expect(reason).toEqual("nope")
    })

    const result = await cachedAsyncFunction("foo", 2, {})
    await expect(result).toEqual(mockInnerWorkings.mock.results[1].value)

    // this should change nothing
    await cachedAsyncFunction("foo", 2, {})

    expect(mockInnerWorkings).toHaveBeenCalledTimes(2)
  })

  it("executes in provided context (required for caching class methods)", () => {
    expect(true).toBeFalsy()
  })
})
