a = "hello {a} world {b}"
print(a.format(**{'a':1, 'b':2}))