-- Busy-wait roughly proportional to the requested milliseconds, used to
-- test client- and server-side timeout handling. The server's UDF Lua
-- sandbox exposes no clock (the os module is removed), so the wait is
-- calibrated by iteration count: ITERS_PER_MS inner-loop rounds
-- approximate one millisecond of Lua execution. The timeout tests only
-- need the wait to comfortably exceed a client timer several times
-- shorter than the requested duration (and the fast-path test to stay
-- well under a generous one), so rough calibration is enough.
local ITERS_PER_MS = 300

function sleep(r, milliseconds)
    local x = 0
    for outer = 1, milliseconds * ITERS_PER_MS do
        for i = 1, 1000 do
            x = x + i
        end
    end
    return "slept"
end
