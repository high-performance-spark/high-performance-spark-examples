Pkg.clone("https://github.com/dfdx/Spark.jl")
Pkg.build("Spark")
# we also need latest master of JavaCall.jl
Pkg.checkout("JavaCall")