# hyhac-sandbox

This folder holds an implementation of a HyperDexClient emulator and a test
implementation that runs against it. It is a separate "cabal" package and can be
run with cabal sandbox:

    cabal sandbox init
    cabal install dependencies-only
    cabal repl

The arguments to the function `test` are as follows:

    test :: Int        -- Number of HyperDexClient calls to make
         -> Int        -- Logging verbosity
         -> Bool       -- Synchronous logging
         -> Maybe Int  -- Doomsday clock value
         -> IO ()

The test implementation is very basic, but of note is the "Doomsday clock" that
is exposed. Key functions in the HyperDexClient emulator are wrapped using the
"Chaos" module tree. Setting the clock to a "Just 100" ensures that when the 
101st call occurs, the clock will be at zero and an exception will be thrown.

This is useful for ensuring that the HyperDex implementation is resilient to a
number of exceptional conditions. When an exception occurs or is thrown during a 
HyperDexClient call, the Hyhac library should return `Left Status` and not
expose the library consumer to those exceptions.
