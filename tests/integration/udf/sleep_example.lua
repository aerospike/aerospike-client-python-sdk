-- Sleep function that does a busy wait for approximately the specified milliseconds
-- This is used to test timeout functionality
function sleep(r, milliseconds)
    local start = os.clock()
    local target = start + (milliseconds / 1000.0)

    -- Busy wait loop
    while os.clock() < target do
        -- Do some computation to ensure the loop isn't optimized away
        local x = 0
        for i = 1, 1000 do
            x = x + i
        end
    end

    return "slept"
end
