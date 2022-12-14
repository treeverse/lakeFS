require("io")

f = io.open("/tmp/a", "w")
f:write("hello world")
f:close()
