export class FunctionCacher {
  private cache: Record<string, any> = {}

  clear() {
    this.cache = {}
  }

  createCachedFunction<FnToCacheResponse extends (...args: Array<any>) => ReturnType<FnToCacheResponse>>(
    fn: FnToCacheResponse,
    keys: { [key in keyof FnToCacheResponse]: boolean },
    context?: any
  ) {
    const wrapper = (...args: Parameters<FnToCacheResponse>): ReturnType<FnToCacheResponse> => {
      let cacheKey = undefined
      // typesafe cast for what we know to be true
      if (Array.isArray(keys)) {
        const cacheKeySegments = (keys as Array<boolean>).map((value, index) =>
          value ? `${args[index]}` /* handle things that are not string/number */ : ""
        )
        cacheKey = `${fn.name}__${cacheKeySegments.join("__")}`
      } else {
        throw new Error("This is impossble!")
      }

      // check cache hit
      if (this.cache[cacheKey]) {
        return this.cache[cacheKey]
      }

      let result = fn.apply(context ?? null, args)

      if ((result as any) instanceof Promise) {
        result = (result as Promise<any>).catch((reason) => {
          // evict from cache
          delete this.cache[cacheKey]

          throw reason
        }) as ReturnType<FnToCacheResponse>
      }

      // put into cache
      if (result !== undefined) {
        this.cache[cacheKey] = result
      }

      return result
    }

    return wrapper
  }
}
