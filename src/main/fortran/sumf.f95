       INTEGER FUNCTION SUMF(N,A) BIND(C, NAME='sumf')
       INTEGER A(N)
       SUMF=SUM(A)
       END
